-- Add triggers to update output/error file sizes as requests complete/fail

-- Function to update output file size when requests complete
CREATE OR REPLACE FUNCTION update_output_file_size() 
RETURNS TRIGGER AS $$
DECLARE
    jsonl_line_size INT;
    rows_updated INT;
BEGIN
    -- Only process completed requests
    IF NEW.state <> 'completed' THEN
        RETURN NEW;
    END IF;

    -- Calculate approximate JSONL line size
    -- Format: {"id":"batch_req_UUID","custom_id":"...","response":{"status_code":200,"request_id":"...","body":{...}}}\n
    -- Note: request_id comes from the response body, not a separate column
    jsonl_line_size := 
        8 +  -- '{"id":"'
        10 + -- 'batch_req_'
        36 + -- UUID length
        15 + -- '","custom_id":"'
        COALESCE(LENGTH(NEW.custom_id), 4) + -- custom_id or 'null'
        14 + -- '","response":{'
        16 + -- '"status_code":'
        LENGTH(NEW.response_status::TEXT) +
        9 +  -- ',"body":'
        COALESCE(LENGTH(NEW.response_body), 0) +
        -- Account for JSON escaping in response_body (quotes, backslashes, newlines, etc.)
        CEIL(COALESCE(LENGTH(NEW.response_body), 0) * 0.1)::INT + -- add 10% for escaping
        4 +  -- '}}\n' (closing response, root, and newline)
        50;  -- error margin for formatting + request_id field if present in response

    -- Update the output file size using optimized single query
    -- Defensive: if update fails (file deleted, etc), don't block request state change
    BEGIN
        UPDATE files f
        SET size_bytes = f.size_bytes + jsonl_line_size,
            updated_at = NOW()
        FROM batches b
        WHERE f.id = b.output_file_id 
          AND b.id = NEW.batch_id
          AND b.output_file_id IS NOT NULL;
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        
        IF rows_updated = 0 THEN
            RAISE WARNING 'Failed to update output file size for request % in batch %: file may not exist', 
                NEW.id, NEW.batch_id;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        -- Log error but don't block the request state update
        RAISE WARNING 'Error updating output file size for request % in batch %: %', 
            NEW.id, NEW.batch_id, SQLERRM;
    END;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for output file size updates
CREATE TRIGGER update_output_file_size_on_complete
AFTER UPDATE ON requests
FOR EACH ROW
WHEN (NEW.state = 'completed' AND OLD.state <> 'completed')
EXECUTE FUNCTION update_output_file_size();

-- Function to update error file size when requests fail
CREATE OR REPLACE FUNCTION update_error_file_size()
RETURNS TRIGGER AS $$
DECLARE
    jsonl_line_size INT;
    error_message_escaped TEXT;
    rows_updated INT;
BEGIN
    -- Only process failed requests
    IF NEW.state <> 'failed' THEN
        RETURN NEW;
    END IF;

    -- The error field in the DB contains the serialized error JSON
    -- When written to JSONL, it gets JSON-escaped again (quotes become \", etc)
    -- Rough estimate: escaping adds ~20% to length for typical error messages
    error_message_escaped := COALESCE(NEW.error, '');

    -- Calculate approximate JSONL line size for error
    -- Format: {"id":"batch_req_UUID","custom_id":"...","response":null,"error":{"code":null,"message":"..."}}\n
    jsonl_line_size := 
        8 +  -- '{"id":"'
        10 + -- 'batch_req_'
        36 + -- UUID length
        15 + -- '","custom_id":"'
        COALESCE(LENGTH(NEW.custom_id), 4) + -- custom_id or 'null'
        23 + -- '","response":null,"error":{'
        27 + -- '"code":null,"message":"'
        LENGTH(error_message_escaped) + -- The actual error message (already JSON-serialized in DB)
        CEIL(LENGTH(error_message_escaped) * 0.2)::INT + -- Add 20% for JSON escaping quotes, backslashes
        5 +  -- '"}}\n' (closing quote, error object, root, newline)
        10;  -- error margin

    -- Update the error file size using optimized single query
    -- Defensive: if update fails (file deleted, etc), don't block request state change
    BEGIN
        UPDATE files f
        SET size_bytes = f.size_bytes + jsonl_line_size,
            updated_at = NOW()
        FROM batches b
        WHERE f.id = b.error_file_id 
          AND b.id = NEW.batch_id
          AND b.error_file_id IS NOT NULL;
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        
        IF rows_updated = 0 THEN
            RAISE WARNING 'Failed to update error file size for request % in batch %: file may not exist', 
                NEW.id, NEW.batch_id;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        -- Log error but don't block the request state update
        RAISE WARNING 'Error updating error file size for request % in batch %: %', 
            NEW.id, NEW.batch_id, SQLERRM;
    END;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for error file size updates
CREATE TRIGGER update_error_file_size_on_fail
AFTER UPDATE ON requests
FOR EACH ROW
WHEN (NEW.state = 'failed' AND OLD.state <> 'failed')
EXECUTE FUNCTION update_error_file_size();