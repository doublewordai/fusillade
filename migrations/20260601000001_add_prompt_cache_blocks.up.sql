-- Prompt-prefix cache index for tracking shared request prefixes.
--
-- Each row records that some request in a given (created_by, model) scope
-- has been seen with a cumulative-prefix block whose content hashes to
-- prefix_hash at position block_index. Because the hash covers the whole prefix
-- up to and including that block, a match at block_index implies the entire
-- leading run of blocks matched a prior request, and matches are monotonic from
-- block 0. cached_tokens for a new request is derived from how many of its
-- leading blocks are already present (and still live per the route TTL). Hits
-- are matched across paths: a prefix first seen on one route can be served to a
-- request on another route, as long as created_by and model match.
--
-- Rows are content-addressed and deduped by the primary key, so highly
-- repetitive batches collapse to a small number of rows (later requests only
-- refresh last_seen_at via ON CONFLICT). path is recorded for TTL/purge purposes
-- only (it pins to the first route to create the block) and is intentionally
-- absent from the primary key so identical prefixes on different routes share a
-- single row. Stale rows are evicted lazily at lookup time (last_seen_at older
-- than the route TTL is ignored) and deleted by the daemon's periodic purge task.

CREATE TABLE prompt_cache_blocks (
    created_by   TEXT        NOT NULL,
    model        TEXT        NOT NULL,
    path         TEXT        NOT NULL,
    block_index  INTEGER     NOT NULL,
    prefix_hash  UUID        NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (created_by, model, block_index, prefix_hash)
);

COMMENT ON TABLE prompt_cache_blocks IS
    'Content-addressed cumulative-prefix blocks per (created_by, model) scope, matched across paths, used to measure prompt-prefix cache hits (cached_tokens). Tracking-only.';

-- Supports the periodic purge sweep, which deletes expired rows per route.
CREATE INDEX idx_prompt_cache_blocks_purge
ON prompt_cache_blocks (path, last_seen_at);
