//! SPARQL 1.1 protocol helpers.
//!
//! Handles content negotiation and request body parsing for the SPARQL HTTP
//! protocol as described in <https://www.w3.org/TR/sparql11-protocol/>.

use crate::response::ResponseFormat;
use crate::translate::SparqlDataset;
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

/// The protocol dataset from `default-graph-uri` / `named-graph-uri`
/// parameters (each repeatable) across urlencoded `sources` (URL query
/// string, form body). `None` when neither parameter is present. When only
/// `default-graph-uri` is given the dataset has no named graphs, as with
/// `FROM` alone.
pub fn dataset_from_params(sources: &[&str]) -> Option<SparqlDataset> {
    let mut default = Vec::new();
    let mut named = Vec::new();
    let mut any = false;
    for source in sources {
        for pair in source.split('&') {
            let mut kv = pair.splitn(2, '=');
            let (Some(k), Some(v)) = (kv.next(), kv.next()) else {
                continue;
            };
            match k {
                "default-graph-uri" => default.push(urlencoding_decode(v)),
                "named-graph-uri" => named.push(urlencoding_decode(v)),
                _ => continue,
            }
            any = true;
        }
    }
    any.then_some(SparqlDataset {
        default,
        named: Some(named),
    })
}

/// Replace the dataset of `query` (its `FROM` / `FROM NAMED`) with the
/// protocol dataset `ds`, which takes precedence (SPARQL 1.1 Protocol §2.1.4).
/// DESCRIBE queries are left unchanged.
pub fn apply_protocol_dataset(
    query: &mut spargebra::Query,
    ds: &SparqlDataset,
) -> Result<(), String> {
    use spargebra::{algebra::QueryDataset, term::NamedNode};
    let iri = |s: &String| NamedNode::new(s.clone()).map_err(|e| format!("bad graph IRI {s}: {e}"));
    let dataset = QueryDataset {
        default: ds.default.iter().map(iri).collect::<Result<_, _>>()?,
        named: ds
            .named
            .as_ref()
            .map(|n| n.iter().map(iri).collect::<Result<_, _>>())
            .transpose()?,
    };
    match query {
        spargebra::Query::Select { dataset: d, .. }
        | spargebra::Query::Ask { dataset: d, .. }
        | spargebra::Query::Construct { dataset: d, .. } => *d = Some(dataset),
        spargebra::Query::Describe { .. } => {}
    }
    Ok(())
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
    fn protocol_dataset_params() {
        assert_eq!(dataset_from_params(&["query=x"]), None);
        let ds = dataset_from_params(&[
            "query=x&default-graph-uri=urn%3Ag%3A1",
            "named-graph-uri=urn:g:2&named-graph-uri=urn:g:3",
        ])
        .unwrap();
        assert_eq!(ds.default, vec!["urn:g:1"]);
        assert_eq!(ds.named, Some(vec!["urn:g:2".into(), "urn:g:3".into()]));

        let mut q =
            spargebra::Query::parse("SELECT * FROM <urn:g:9> WHERE { ?s ?p ?o }", None).unwrap();
        apply_protocol_dataset(&mut q, &ds).unwrap();
        let t = crate::translate_query(&q).unwrap();
        assert_eq!(t.dataset, Some(ds), "protocol dataset replaces FROM");
    }

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
