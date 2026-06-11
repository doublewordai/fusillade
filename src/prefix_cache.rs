//! Prompt-prefix caching (tracking-only).
//!
//! When a user issues many requests that share a leading prefix (a common
//! system prompt, few-shot preamble, or repeated context), real inference
//! backends serve that shared prefix from a KV cache and bill the shared tokens
//! at a discount (mirroring OpenAI's `cached_tokens`). This module measures, for
//! each incoming request, how many leading "tokens" it shares with previously
//! seen requests in the same `(created_by, model)` scope — matched across paths,
//! so a prefix first seen on one route can be served to a request on another —
//! records the count as `requests.cached_tokens`, and feeds instrumentation.
//! **No discount is applied yet** — `discount_rate` is stored but unused.
//!
//! ## Mechanism (block-prefix cache)
//!
//! The (sanitized) request body is split into fixed-size blocks of
//! `block_tokens` approximate tokens. For each block we compute a *chained*
//! hash that depends on every preceding block, so a hit at block `i` implies the
//! entire leading run of `i + 1` blocks matched some prior request, and matches
//! form a contiguous prefix from block 0. The number of live (non-expired)
//! leading blocks already present in [`prompt_cache_blocks`] gives the cached
//! prefix length. Rows are content-addressed and deduped by the table primary
//! key, so highly repetitive batches collapse to a handful of rows.
//!
//! Token counting is a deliberately cheap approximation (`chars / chars_per_token`)
//! with no tokenizer dependency; swap [`approx_token_count`] for a real BPE
//! tokenizer later without touching callers.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::error::{FusilladeError, Result};

/// Stable namespace for v5 prefix hashes. Arbitrary but fixed — changing it
/// invalidates all existing cache rows (they simply stop matching and age out).
const PREFIX_CACHE_NAMESPACE: Uuid = Uuid::from_u128(0x6675_7369_6c6c_6164_655f_7072_6566_6978);

/// Configuration for prompt-prefix cache tracking.
///
/// Lives inside [`crate::daemon::DaemonConfig`] so both ingestion (on the
/// manager) and the daemon process loop read the same settings.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PrefixCacheConfig {
    /// Master switch. When `false`, all cache work is skipped and
    /// `cached_tokens` is left at `0`.
    pub enabled: bool,

    /// Approximate characters per token for the cheap token estimate.
    pub chars_per_token: usize,

    /// Number of approximate tokens per cache block. Smaller values make hits
    /// more granular (and more likely on short prompts); tune to `128` for
    /// OpenAI-style parity.
    pub block_tokens: usize,

    /// Minimum cached-token count to report a hit. Counts below this are
    /// reported as `0` (OpenAI uses a 1024-token floor).
    pub min_cached_tokens: usize,

    /// Maximum number of leading blocks tracked per request. Bounds row growth
    /// and per-request work for very long prompts.
    pub max_blocks: usize,

    /// Maximum number of leading characters of the body considered. Bounds
    /// per-request hashing cost.
    pub max_body_chars: usize,

    /// Discount fraction (0.0–1.0) that would be applied to cached tokens.
    /// **Stored but unused** in this tracking-only phase.
    pub discount_rate: f64,

    /// Fallback lifetime for cached prefix blocks on routes not present in
    /// [`Self::route_ttls`].
    pub default_ttl: Duration,

    /// Per-route (request `path`) lifetime overrides for cached prefix blocks.
    pub route_ttls: HashMap<String, Duration>,
}

impl Default for PrefixCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            chars_per_token: 4,
            block_tokens: 16,
            min_cached_tokens: 0,
            max_blocks: 1024,
            max_body_chars: 256 * 1024,
            discount_rate: 0.5,
            default_ttl: Duration::from_secs(3600),
            route_ttls: HashMap::new(),
        }
    }
}

/// Resolve the lifetime for a given route (request `path`), falling back to the
/// global default when the route has no explicit override.
pub fn ttl_for(cfg: &PrefixCacheConfig, path: &str) -> Duration {
    cfg.route_ttls.get(path).copied().unwrap_or(cfg.default_ttl)
}

/// Cheap token-count approximation: `ceil(chars / chars_per_token)`.
///
/// Counts Unicode scalar values, not bytes, so multi-byte text is handled
/// consistently. Swap this for a real tokenizer later without changing callers.
pub fn approx_token_count(s: &str, chars_per_token: usize) -> usize {
    let cpt = chars_per_token.max(1);
    s.chars().count().div_ceil(cpt)
}

/// Compute the chained cumulative-prefix block hashes for a body.
///
/// Block `i`'s hash chains in every preceding block, so two bodies with an
/// identical leading run of blocks produce an identical run of hashes — the
/// property the cache relies on. Operates on `char` boundaries (never byte
/// slicing) and is bounded by `max_body_chars` / `max_blocks`.
pub fn prefix_block_hashes(body: &str, cfg: &PrefixCacheConfig) -> Vec<Uuid> {
    let block_chars = cfg.block_tokens.max(1) * cfg.chars_per_token.max(1);
    let chars: Vec<char> = body.chars().take(cfg.max_body_chars).collect();
    if chars.is_empty() {
        return Vec::new();
    }
    let total = chars.len();
    let n_blocks = total.div_ceil(block_chars).min(cfg.max_blocks.max(1));

    let mut hashes = Vec::with_capacity(n_blocks);
    let mut acc = PREFIX_CACHE_NAMESPACE;
    for i in 0..n_blocks {
        let start = i * block_chars;
        let end = ((i + 1) * block_chars).min(total);
        // The final block may be shorter than block_chars; that's fine — its
        // content still chains deterministically.
        let block: String = chars[start..end].iter().collect();
        acc = Uuid::new_v5(&acc, block.as_bytes());
        hashes.push(acc);
    }
    hashes
}

/// Convert a `std::time::Duration` lookback into a `cutoff` timestamp, saturating
/// rather than overflowing on absurdly large TTLs.
fn cutoff_for(now: DateTime<Utc>, ttl: Duration) -> DateTime<Utc> {
    let millis = ttl.as_millis().min(i64::MAX as u128) as i64;
    now - chrono::Duration::milliseconds(millis)
}

/// Compute `cached_tokens` for a request and record its prefix blocks.
///
/// Runs one filtered `SELECT` (count of live leading blocks already in scope)
/// followed by one bulk upsert that inserts new blocks and refreshes
/// `last_seen_at` on matched ones (inactivity-based eviction). The count is
/// taken *before* the upsert so a request never matches itself.
///
/// Returns the approximate number of cached prefix tokens (`0` when disabled,
/// below `min_cached_tokens`, or on an empty body).
pub async fn record_and_count(
    pool: &sqlx::PgPool,
    cfg: &PrefixCacheConfig,
    created_by: &str,
    model: &str,
    path: &str,
    body: &str,
) -> Result<i64> {
    if !cfg.enabled {
        return Ok(0);
    }

    let hashes = prefix_block_hashes(body, cfg);
    if hashes.is_empty() {
        return Ok(0);
    }

    let block_indices: Vec<i32> = (0..hashes.len() as i32).collect();
    let cutoff = cutoff_for(Utc::now(), ttl_for(cfg, path));

    // Count how many of this request's leading blocks are already present and
    // still live. Matching ignores `path` (cross-route hits) but the lookback
    // `cutoff` is still derived from the current request's route TTL. Because
    // hashes chain and each block dedupes to a single row per (created_by,
    // model), the match count equals the cached prefix length in blocks.
    let live_blocks: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM prompt_cache_blocks
        WHERE created_by = $1 AND model = $2
          AND last_seen_at > $3
          AND (block_index, prefix_hash) IN (
              SELECT * FROM UNNEST($4::int[], $5::uuid[])
          )
        "#,
    )
    .bind(created_by)
    .bind(model)
    .bind(cutoff)
    .bind(&block_indices)
    .bind(&hashes)
    .fetch_one(pool)
    .await
    .map_err(|e| FusilladeError::Other(anyhow::anyhow!("prefix cache lookup failed: {}", e)))?;

    // Record (insert new, refresh matched) all of this request's blocks. `path`
    // is stored for TTL/purge accounting only and pinned to the first route to
    // create the block; it is not part of the conflict target so identical
    // prefixes on different routes collapse to one row.
    sqlx::query(
        r#"
        INSERT INTO prompt_cache_blocks (created_by, model, path, block_index, prefix_hash, last_seen_at)
        SELECT $1, $2, $3, idx, h, NOW()
        FROM UNNEST($4::int[], $5::uuid[]) AS t(idx, h)
        ON CONFLICT (created_by, model, block_index, prefix_hash)
        DO UPDATE SET last_seen_at = NOW()
        "#,
    )
    .bind(created_by)
    .bind(model)
    .bind(path)
    .bind(&block_indices)
    .bind(&hashes)
    .execute(pool)
    .await
    .map_err(|e| FusilladeError::Other(anyhow::anyhow!("prefix cache upsert failed: {}", e)))?;

    let cached_tokens = (live_blocks as usize * cfg.block_tokens)
        .min(approx_token_count(body, cfg.chars_per_token));
    if cached_tokens < cfg.min_cached_tokens {
        return Ok(0);
    }
    Ok(cached_tokens as i64)
}

/// Delete cached prefix blocks that have outlived their route TTL.
///
/// Runs one `DELETE` per explicitly-configured route plus one `DELETE` covering
/// every other route under the default TTL. Returns the number of rows removed.
/// Called periodically by the daemon's purge task.
pub async fn purge_expired(pool: &sqlx::PgPool, cfg: &PrefixCacheConfig) -> Result<u64> {
    if !cfg.enabled {
        return Ok(0);
    }
    let now = Utc::now();
    let mut total = 0u64;

    for (path, ttl) in &cfg.route_ttls {
        let cutoff = cutoff_for(now, *ttl);
        let res =
            sqlx::query("DELETE FROM prompt_cache_blocks WHERE path = $1 AND last_seen_at < $2")
                .bind(path)
                .bind(cutoff)
                .execute(pool)
                .await
                .map_err(|e| {
                    FusilladeError::Other(anyhow::anyhow!("prefix cache purge failed: {}", e))
                })?;
        total += res.rows_affected();
    }

    // Everything not covered by an explicit route override uses the default TTL.
    // `path <> ALL(ARRAY[]::text[])` is TRUE for all rows, so an empty override
    // map correctly purges every route under the default.
    let mapped: Vec<String> = cfg.route_ttls.keys().cloned().collect();
    let cutoff = cutoff_for(now, cfg.default_ttl);
    let res = sqlx::query(
        "DELETE FROM prompt_cache_blocks WHERE path <> ALL($1::text[]) AND last_seen_at < $2",
    )
    .bind(&mapped)
    .bind(cutoff)
    .execute(pool)
    .await
    .map_err(|e| FusilladeError::Other(anyhow::anyhow!("prefix cache purge failed: {}", e)))?;
    total += res.rows_affected();

    Ok(total)
}

/// Per-user prefix-cache counters, accumulated in memory and emitted/logged
/// periodically (mirrors the daemon's per-user throughput accounting).
#[derive(Default)]
pub struct PrefixCacheUserStats {
    pub lookups: AtomicU64,
    pub hits: AtomicU64,
    pub cached_tokens: AtomicU64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> PrefixCacheConfig {
        PrefixCacheConfig {
            block_tokens: 2,
            chars_per_token: 4,
            ..Default::default()
        }
    }

    #[test]
    fn approx_token_count_rounds_up_and_counts_chars() {
        assert_eq!(approx_token_count("", 4), 0);
        assert_eq!(approx_token_count("abcd", 4), 1);
        assert_eq!(approx_token_count("abcde", 4), 2);
        // Multi-byte chars count as one char each, not by byte length.
        assert_eq!(approx_token_count("日本語", 1), 3);
    }

    #[test]
    fn identical_bodies_produce_identical_hash_sequences() {
        let c = cfg();
        let body = "the quick brown fox jumps over the lazy dog";
        assert_eq!(prefix_block_hashes(body, &c), prefix_block_hashes(body, &c));
    }

    #[test]
    fn shared_prefix_shares_leading_hashes_then_diverges() {
        let c = cfg();
        let a = prefix_block_hashes("AAAAAAAA-common-prefix-XXXX", &c);
        let b = prefix_block_hashes("AAAAAAAA-common-prefix-YYYY", &c);
        // Leading blocks (the shared run) match; the tail diverges.
        let shared = a.iter().zip(&b).take_while(|(x, y)| x == y).count();
        assert!(shared > 0, "expected a shared leading run");
        assert!(
            shared < a.len().max(b.len()),
            "expected eventual divergence"
        );
    }

    #[test]
    fn block_and_body_caps_bound_output() {
        let c = PrefixCacheConfig {
            block_tokens: 1,
            chars_per_token: 1,
            max_blocks: 3,
            max_body_chars: 100,
            ..Default::default()
        };
        // 10 chars, 1 char/block, but capped at 3 blocks.
        assert_eq!(prefix_block_hashes("0123456789", &c).len(), 3);
    }

    #[test]
    fn multibyte_body_does_not_panic_on_block_boundaries() {
        let c = PrefixCacheConfig {
            block_tokens: 1,
            chars_per_token: 1,
            ..Default::default()
        };
        // Block boundaries fall between multi-byte chars; must slice on chars.
        let _ = prefix_block_hashes("a日b本c語d", &c);
    }

    #[test]
    fn ttl_for_uses_override_then_default() {
        let mut c = cfg();
        c.default_ttl = Duration::from_secs(60);
        c.route_ttls
            .insert("/v1/chat/completions".to_string(), Duration::from_secs(10));
        assert_eq!(ttl_for(&c, "/v1/chat/completions"), Duration::from_secs(10));
        assert_eq!(ttl_for(&c, "/v1/embeddings"), Duration::from_secs(60));
    }
}
