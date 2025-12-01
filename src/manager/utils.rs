/// Calculate estimated JSONL line size for a completed request.
/// Format: {"id":"batch_req_UUID","custom_id":"...","response":{"status_code":200,"body":{...}}}\n
pub fn calculate_output_size(
    custom_id: &Option<String>,
    response_status: i16,
    response_body: &str,
) -> i64 {
    // Base structure without custom_id
    let mut size = 8 +  // '{"id":"'
        10 + // 'batch_req_'
        36 + // UUID length
        2; // '",'

    // Add custom_id field (either "custom_id":"value" or "custom_id":null)
    size += match custom_id {
        Some(id) => 15 + id.len(), // '","custom_id":"' + value + '"'
        None => 16,                // '","custom_id":null'
    };

    size += 12 + // '"response":{'
        16 + // '"status_code":'
        response_status.to_string().len() +
        9 +  // ',"body":'
        response_body.len() +
        // Account for JSON escaping of special characters in response_body
        // (quotes, backslashes, newlines, etc. that need escaping when serialized)
        (response_body.len() as f64 * 0.1).ceil() as usize + // add 10% for escaping
        4 +  // '}}\n' (closing response, root, and newline)
        50; // error margin for formatting + request_id field if present

    size as i64
}

/// Calculate estimated JSONL line size for a failed request.
/// Format: {"id":"batch_req_UUID","custom_id":"...","response":null,"error":{"code":null,"message":"..."}}\n
pub fn calculate_error_size(custom_id: &Option<String>, error_json: &str) -> i64 {
    // Base structure without custom_id
    let mut size = 8 +  // '{"id":"'
        10 + // 'batch_req_'
        36 + // UUID length
        2; // '",'

    // Add custom_id field (either "custom_id":"value" or "custom_id":null)
    size += match custom_id {
        Some(id) => 15 + id.len(), // '","custom_id":"' + value + '"'
        None => 16,                // '","custom_id":null'
    };

    size += 10 + // '"response":'
        23 + // 'null,"error":{"code":'
        27 + // 'null,"message":"'
        error_json.len() + // The error message content
        // Account for JSON escaping of special characters in the error message
        // (quotes, backslashes, control characters, etc. that need escaping)
        (error_json.len() as f64 * 0.2).ceil() as usize + // add 20% for escaping
        5 +  // '"}}\n' (closing quote, error object, root, newline)
        10; // error margin

    size as i64
}
