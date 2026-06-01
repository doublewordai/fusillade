-- Remove prompt-prefix cache hit tracking from requests
ALTER TABLE requests
DROP COLUMN IF EXISTS cached_tokens;
