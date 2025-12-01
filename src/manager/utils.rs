/// Calculate estimated JSONL line size for a completed request.
/// Format: {"id":"batch_req_UUID","custom_id":"...","response":{"status_code":200,"body":{...}}}\n
pub fn calculate_output_size(
    custom_id: &Option<String>,
    response_status: i16,
    response_body: &str,
) -> i64 {
    let size = 8 +  // '{"id":"'
        10 + // 'batch_req_'
        36 + // UUID length
        15 + // '","custom_id":"'
        custom_id.as_ref().map(|s| s.len()).unwrap_or(4) + // custom_id or 'null'
        14 + // '","response":{'
        16 + // '"status_code":'
        response_status.to_string().len() +
        9 +  // ',"body":'
        response_body.len() +
        // Account for JSON escaping in response_body (quotes, backslashes, newlines)
        (response_body.len() as f64 * 0.1).ceil() as usize + // add 10% for escaping
        4 +  // '}}\n' (closing response, root, and newline)
        50; // error margin for formatting + request_id field if present

    size as i64
}

/// Calculate estimated JSONL line size for a failed request.
/// Format: {"id":"batch_req_UUID","custom_id":"...","response":null,"error":{"code":null,"message":"..."}}\n
pub fn calculate_error_size(custom_id: &Option<String>, error_json: &str) -> i64 {
    let size = 8 +  // '{"id":"'
        10 + // 'batch_req_'
        36 + // UUID length
        15 + // '","custom_id":"'
        custom_id.as_ref().map(|s| s.len()).unwrap_or(4) + // custom_id or 'null'
        23 + // '","response":null,"error":{'
        27 + // '"code":null,"message":"'
        error_json.len() + // The actual error message (already JSON-serialized)
        (error_json.len() as f64 * 0.2).ceil() as usize + // Add 20% for JSON escaping
        5 +  // '"}}\n' (closing quote, error object, root, newline)
        10; // error margin

    size as i64
}
