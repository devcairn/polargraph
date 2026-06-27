//! SPARQL 1.1 protocol helpers.
//!
//! Handles content negotiation and request body parsing for the SPARQL HTTP
//! protocol as described in <https://www.w3.org/TR/sparql11-protocol/>.

use crate::response::ResponseFormat;
use http::HeaderMap;

/// Negotiate the response serialization format from the HTTP `Accept` header.
///
/// Returns `ResponseFormat::Csv` when the client requests `text/csv`;
/// otherwise defaults to `ResponseFormat::Json`.
pub fn negotiate_format(headers: &HeaderMap) -> ResponseFormat {
    if let Some(accept) = headers.get("accept").and_then(|v| v.to_str().ok()) {
        if accept.contains("text/csv") {
            return ResponseFormat::Csv;
        }
    }
    ResponseFormat::Json
}

/// Extract the SPARQL query string from a `application/x-www-form-urlencoded`
/// body (the `query=` parameter).
pub fn extract_query_from_form(body: &str) -> Option<String> {
    for pair in body.split('&') {
        let mut kv = pair.splitn(2, '=');
        if let (Some(k), Some(v)) = (kv.next(), kv.next()) {
            if k == "query" {
                return Some(urlencoding_decode(v));
            }
        }
    }
    None
}

fn urlencoding_decode(s: &str) -> String {
    let with_spaces = s.replace('+', " ");
    percent_decode(&with_spaces)
}

fn percent_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let h1 = bytes[i + 1] as char;
            let h2 = bytes[i + 2] as char;
            let hex = format!("{}{}", h1, h2);
            if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                out.push(byte as char);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Placeholder — the GET /sparql handler extracts the query param directly
/// via axum's `Query` extractor.
pub fn extract_query_string() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_form_simple() {
        let body = "query=SELECT+%3Fs+WHERE+%7B+%3Fs+%3Ca%3E+%3Fo+%7D";
        let q = extract_query_from_form(body).unwrap();
        assert!(q.starts_with("SELECT ?s WHERE"));
    }

    #[test]
    fn negotiate_csv() {
        let mut headers = HeaderMap::new();
        headers.insert("accept", "text/csv".parse().unwrap());
        assert_eq!(negotiate_format(&headers), ResponseFormat::Csv);
    }

    #[test]
    fn negotiate_json_default() {
        let headers = HeaderMap::new();
        assert_eq!(negotiate_format(&headers), ResponseFormat::Json);
    }
}
