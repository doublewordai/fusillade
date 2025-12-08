-- Remove composite batches indexes for SLA-based claims and monitoring
DROP INDEX IF EXISTS idx_batches_active_by_expiration;
DROP INDEX IF EXISTS idx_batches_sla_monitoring;
