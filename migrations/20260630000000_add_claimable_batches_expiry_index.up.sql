CREATE INDEX IF NOT EXISTS idx_batches_claimable_expiry
ON batches (expires_at)
INCLUDE (id)
WHERE cancelling_at IS NULL
  AND deleted_at IS NULL
  AND completed_at IS NULL
  AND failed_at IS NULL
  AND cancelled_at IS NULL;

COMMENT ON INDEX idx_batches_claimable_expiry IS
'Optimized for monitoring claimable batch queue depth by completion window';
