//! Pool provider abstraction for read/write routing.
//!
//! This module provides the [`PoolProvider`] trait that enables routing database operations
//! to different connection pools based on whether they're read or write operations.
//!
//! # Architecture
//!
//! The [`PoolProvider`] trait defines two methods:
//! - `read()` - Returns a pool for read-only operations (may use replica)
//! - `write()` - Returns a pool for write operations (always uses primary)
//!
//! # Usage
//!
//! The control layer implements this trait for its `DbPools` type to enable
//! read/write separation. For testing, use [`TestDbPools`] which creates a
//! read-only replica to verify correct read/write routing.

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// Trait for providing database pools with read/write routing.
///
/// Implementations can provide separate read and write pools for load distribution,
/// or use a single pool for both operations.
///
/// # Thread Safety
///
/// Implementations must be `Clone`, `Send`, and `Sync` to work with async Rust
/// and be shared across tasks.
///
/// # Example
///
/// ```ignore
/// use fusillade::PoolProvider;
/// use sqlx::PgPool;
///
/// struct MyPools {
///     primary: PgPool,
///     replica: Option<PgPool>,
/// }
///
/// impl PoolProvider for MyPools {
///     fn read(&self) -> &PgPool {
///         self.replica.as_ref().unwrap_or(&self.primary)
///     }
///
///     fn write(&self) -> &PgPool {
///         &self.primary
///     }
/// }
/// ```
pub trait PoolProvider: Clone + Send + Sync + 'static {
    /// Get a pool for read operations.
    ///
    /// This may return a read replica for load distribution, or fall back to
    /// the primary pool if no replica is configured.
    ///
    /// # Use Cases
    ///
    /// Use this for queries that:
    /// - Don't modify data (SELECT without FOR UPDATE)
    /// - Can tolerate slight staleness (eventual consistency)
    /// - Are read-heavy and benefit from load distribution
    ///
    /// Examples: listing files, getting batch status, analytics queries.
    fn read(&self) -> &PgPool;

    /// Get a pool for write operations or reads requiring strong consistency.
    ///
    /// This should always return the primary pool to ensure ACID guarantees
    /// and read-after-write consistency.
    ///
    /// # Use Cases
    ///
    /// Use this for:
    /// - Any INSERT, UPDATE, DELETE operations
    /// - Transactions (BEGIN/COMMIT/ROLLBACK)
    /// - SELECT FOR UPDATE (locking reads)
    /// - Operations that need read-after-write consistency
    /// - Advisory locks
    ///
    /// Examples: creating files, persisting request state, claiming requests.
    fn write(&self) -> &PgPool;
}

/// Test pool provider with read-only replica enforcement.
///
/// This creates two separate connection pools from the same database:
/// - Primary pool for writes (normal permissions)
/// - Replica pool for reads (enforces `default_transaction_read_only = on`)
///
/// This ensures tests catch bugs where write operations are incorrectly
/// routed through `.read()`. PostgreSQL will reject writes with:
/// "cannot execute INSERT/UPDATE/DELETE in a read-only transaction"
///
/// # Example
///
/// ```ignore
/// #[sqlx::test]
/// async fn test_something(pool: PgPool) {
///     let pools = TestDbPools::new(pool).await.unwrap();
///     let manager = PostgresRequestManager::new(pools);
///
///     // Write operations must use .write()
///     // Read operations can use .read()
///     // Mixing them up will fail at test time
/// }
/// ```
#[derive(Clone, Debug)]
pub struct TestDbPools {
    primary: PgPool,
    replica: PgPool,
}

impl TestDbPools {
    /// Create test pools from a single database pool.
    ///
    /// This creates:
    /// - A primary pool (clone of input) for writes
    /// - A replica pool (new connection) configured as read-only
    ///
    /// The replica pool enforces `default_transaction_read_only = on`,
    /// so any write operations will fail with a PostgreSQL error.
    pub async fn new(pool: PgPool) -> Result<Self, sqlx::Error> {
        let primary = pool.clone();

        // Create a separate pool with read-only enforcement
        let replica = PgPoolOptions::new()
            .max_connections(pool.options().get_max_connections())
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    // Set all transactions to read-only by default
                    sqlx::query("SET default_transaction_read_only = on")
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(pool.connect_options().as_ref().clone())
            .await?;

        Ok(Self { primary, replica })
    }
}

impl PoolProvider for TestDbPools {
    fn read(&self) -> &PgPool {
        &self.replica
    }

    fn write(&self) -> &PgPool {
        &self.primary
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Basic compile-time test that TestDbPools implements PoolProvider
    #[allow(dead_code)]
    fn assert_pool_provider<P: PoolProvider>() {}

    #[allow(dead_code)]
    fn test_db_pools_is_pool_provider() {
        assert_pool_provider::<TestDbPools>();
    }

    #[sqlx::test]
    async fn test_read_pool_rejects_writes(pool: sqlx::PgPool) {
        let pools = TestDbPools::new(pool).await.unwrap();

        // Write operations should work on the write pool
        sqlx::query("CREATE TEMP TABLE test_write (id INT)")
            .execute(pools.write())
            .await
            .expect("Write pool should allow CREATE TABLE");

        // Write operations should FAIL on the read pool
        let result = sqlx::query("CREATE TEMP TABLE test_read_reject (id INT)")
            .execute(pools.read())
            .await;

        assert!(result.is_err(), "Read pool should reject CREATE TABLE");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("read-only") || err.contains("cannot execute"),
            "Error should mention read-only restriction, got: {}",
            err
        );
    }

    #[sqlx::test]
    async fn test_read_pool_allows_selects(pool: sqlx::PgPool) {
        let pools = TestDbPools::new(pool).await.unwrap();

        // Read operations should work on the read pool
        // Query a system table that exists in all databases
        let result: (i32,) = sqlx::query_as("SELECT 1 + 1 as sum")
            .fetch_one(pools.read())
            .await
            .expect("Read pool should allow SELECT");

        assert_eq!(result.0, 2, "Should compute 1 + 1 = 2");
    }
}
