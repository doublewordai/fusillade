/// Calculate the raw size of a response body in bytes.
/// This is the actual response content size, not the JSONL-formatted size.
pub fn calculate_response_body_size(response_body: &str) -> i64 {
    response_body.len() as i64
}

/// Calculate the raw size of an error message in bytes.
/// This is the actual error content size, not the JSONL-formatted size.
pub fn calculate_error_message_size(error_json: &str) -> i64 {
    error_json.len() as i64
}

/// Calculate JSONL formatting overhead per line for output file.
/// This is the extra bytes added when wrapping a response in JSONL format.
/// Format: {"id":"batch_req_UUID","custom_id":"...","response":{"status_code":200,"body":{...}}}\n
pub fn calculate_jsonl_output_overhead_per_line(
    custom_id: &Option<String>,
    response_status: i16,
) -> i64 {
    // Base structure without custom_id or body
    let mut overhead = 8 +  // '{"id":"'
        10 + // 'batch_req_'
        36 + // UUID length
        2; // '",'

    // Add custom_id field (either "custom_id":"value" or "custom_id":null)
    overhead += match custom_id {
        Some(id) => 15 + id.len(), // '","custom_id":"' + value + '"'
        None => 16,                // '","custom_id":null'
    };

    overhead += 12 + // '"response":{'
        16 + // '"status_code":'
        response_status.to_string().len() +
        9 +  // ',"body":'
        // Note: actual body size will be added separately by caller
        4 +  // '}}\n' (closing response, root, and newline)
        50; // error margin for formatting + request_id field + JSON escaping (~10% of body)

    overhead as i64
}

/// Calculate JSONL formatting overhead per line for error file.
/// Format: {"id":"batch_req_UUID","custom_id":"...","response":null,"error":{"code":null,"message":"..."}}\n
pub fn calculate_jsonl_error_overhead_per_line(custom_id: &Option<String>) -> i64 {
    // Base structure without custom_id or error message
    let mut overhead = 8 +  // '{"id":"'
        10 + // 'batch_req_'
        36 + // UUID length
        2; // '",'

    // Add custom_id field
    overhead += match custom_id {
        Some(id) => 15 + id.len(),
        None => 16,
    };

    overhead += 10 + // '"response":'
        23 + // 'null,"error":{"code":'
        27 + // 'null,"message":"'
        // Note: actual error message size will be added separately by caller
        5 +  // '"}}\n'
        20; // error margin for formatting + JSON escaping (~20% of message)

    overhead as i64
}

/// Estimate total JSONL file size for output file from raw response body sizes.
/// Takes the sum of raw body sizes and adds estimated overhead per request.
pub fn estimate_output_file_size(
    raw_body_size_sum: i64,
    request_count: i64,
    avg_custom_id_len: usize,
    avg_status_code: i16,
) -> i64 {
    // Average overhead per line
    let avg_overhead = calculate_jsonl_output_overhead_per_line(
        &Some("x".repeat(avg_custom_id_len)),
        avg_status_code,
    );

    raw_body_size_sum + (avg_overhead * request_count)
}

/// Estimate total JSONL file size for error file from raw error message sizes.
pub fn estimate_error_file_size(
    raw_error_size_sum: i64,
    request_count: i64,
    avg_custom_id_len: usize,
) -> i64 {
    // Average overhead per line
    let avg_overhead =
        calculate_jsonl_error_overhead_per_line(&Some("x".repeat(avg_custom_id_len)));

    raw_error_size_sum + (avg_overhead * request_count)
}
