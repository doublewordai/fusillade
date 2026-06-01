-- Remove the prompt-prefix cache index
DROP INDEX IF EXISTS idx_prompt_cache_blocks_purge;
DROP TABLE IF EXISTS prompt_cache_blocks;
