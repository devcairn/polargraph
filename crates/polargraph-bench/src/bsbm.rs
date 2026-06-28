//! Berlin SPARQL Benchmark (BSBM) — standalone Rust implementation.
//!
//! Generates a scale-factor-N e-commerce dataset (products, vendors, offers,
//! reviews) and runs all 12 standard BSBM query templates in-process against a
//! local `TripleStore`, reporting per-query latency statistics.
//!
//! ## Schema predicates
//!
//! | Predicate                         | Kind     | Description                            |
//! |-----------------------------------|----------|----------------------------------------|
//! | `bsbm:type`                       | Relation | product → ProductType                  |
//! | `bsbm:subClassOf`                 | Relation | ProductType → parent ProductType       |
//! | `bsbm:feature`                    | Relation | product → Feature                      |
//! | `bsbm:producer`                   | Relation | product → Vendor (producer)            |
//! | `bsbm:offerFor`                   | Relation | Offer → product                        |
//! | `bsbm:vendor`                     | Relation | Offer → Vendor                         |
//! | `bsbm:reviewFor`                  | Relation | Review → product                       |
//! | `bsbm:reviewer`                   | Relation | Review → reviewer (node)               |
//! | `bsbm:label`                      | Property | Text label                             |
//! | `bsbm:comment`                    | Property | Text comment                           |
//! | `bsbm:country`                    | Property | Vendor country (Text)                  |
//! | `bsbm:price`                      | Property | Offer price (Float)                    |
//! | `bsbm:validFrom`                  | Property | Offer validity start (Int unix days)   |
//! | `bsbm:validTo`                    | Property | Offer validity end (Int unix days)     |
//! | `bsbm:productPropertyNumeric1..5` | Property | Int numeric product properties         |
//! | `bsbm:productPropertyTextual1..5` | Property | Text product properties                |
//! | `bsbm:rating1..4`                 | Property | Review ratings 1–5 (Int)               |

use hdrhistogram::Histogram;
use polargraph_core::{
    id::{EdgeId, NodeId},
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};
use polargraph_query::{
    datalog::execute_query, datalog::Query, datalog::Term, datalog::VarPattern,
};
use polargraph_storage::TripleStore;
use std::time::{Duration, Instant};

// ── Predicates ────────────────────────────────────────────────────────────────

const BSBM_TYPE: &str = "bsbm:type";
const BSBM_SUBCLASS: &str = "bsbm:subClassOf";
const BSBM_FEATURE: &str = "bsbm:feature";
const BSBM_PRODUCER: &str = "bsbm:producer";
const BSBM_LABEL: &str = "bsbm:label";
const BSBM_COMMENT: &str = "bsbm:comment";
const BSBM_NUM1: &str = "bsbm:productPropertyNumeric1";
const BSBM_NUM2: &str = "bsbm:productPropertyNumeric2";
const BSBM_TEXT1: &str = "bsbm:productPropertyTextual1";
const BSBM_TEXT2: &str = "bsbm:productPropertyTextual2";
const BSBM_OFFER_FOR: &str = "bsbm:offerFor";
const BSBM_PRICE: &str = "bsbm:price";
const BSBM_VALID_FROM: &str = "bsbm:validFrom";
const BSBM_VALID_TO: &str = "bsbm:validTo";
const BSBM_VENDOR: &str = "bsbm:vendor";
const BSBM_COUNTRY: &str = "bsbm:country";
const BSBM_REVIEW_FOR: &str = "bsbm:reviewFor";
const BSBM_RATING1: &str = "bsbm:rating1";
const BSBM_RATING2: &str = "bsbm:rating2";
const BSBM_REVIEWER: &str = "bsbm:reviewer";

// ── Node ID helpers ───────────────────────────────────────────────────────────

/// Deterministic NodeId from an entity IRI via xxHash3-128 (same as owl_rl.rs).
pub fn bsbm_node_id(iri: &str) -> NodeId {
    let hash: u128 = xxhash_rust::xxh3::xxh3_128(iri.as_bytes());
    NodeId(uuid::Uuid::from_u128(hash))
}

fn product_id(i: usize) -> NodeId {
    bsbm_node_id(&format!("urn:bsbm:product:{i}"))
}
fn product_type_id(i: usize) -> NodeId {
    bsbm_node_id(&format!("urn:bsbm:productType:{i}"))
}
fn feature_id(i: usize) -> NodeId {
    bsbm_node_id(&format!("urn:bsbm:feature:{i}"))
}
fn vendor_id(i: usize) -> NodeId {
    bsbm_node_id(&format!("urn:bsbm:vendor:{i}"))
}
fn offer_id(i: usize) -> NodeId {
    bsbm_node_id(&format!("urn:bsbm:offer:{i}"))
}
fn review_id(i: usize) -> NodeId {
    bsbm_node_id(&format!("urn:bsbm:review:{i}"))
}
fn reviewer_id(i: usize) -> NodeId {
    bsbm_node_id(&format!("urn:bsbm:reviewer:{i}"))
}

// ── Triple constructors ───────────────────────────────────────────────────────

fn prop_text(subject: NodeId, predicate: &str, value: &str) -> Triple {
    Triple::Property {
        subject,
        predicate: Predicate::new(predicate),
        value: Value::Text(value.into()),
        temporal: BiTemporalRange::assert_now(Timestamp::now()),
    }
}

fn prop_int(subject: NodeId, predicate: &str, value: i64) -> Triple {
    Triple::Property {
        subject,
        predicate: Predicate::new(predicate),
        value: Value::Int(value),
        temporal: BiTemporalRange::assert_now(Timestamp::now()),
    }
}

fn prop_float(subject: NodeId, predicate: &str, value: f64) -> Triple {
    Triple::Property {
        subject,
        predicate: Predicate::new(predicate),
        value: Value::Float(value),
        temporal: BiTemporalRange::assert_now(Timestamp::now()),
    }
}

fn rel(subject: NodeId, predicate: &str, object: NodeId) -> Triple {
    Triple::Relation {
        subject,
        predicate: Predicate::new(predicate),
        object,
        edge_id: EdgeId::new(),
        temporal: BiTemporalRange::assert_now(Timestamp::now()),
    }
}

// ── Dataset descriptor ────────────────────────────────────────────────────────

/// All generated entity IDs, deterministically derived from the scale factor.
pub struct BsbmDataset {
    pub products: Vec<NodeId>,
    pub product_types: Vec<NodeId>,
    pub features: Vec<NodeId>,
    pub vendors: Vec<NodeId>,
    pub offers: Vec<NodeId>,
    pub reviews: Vec<NodeId>,
    pub reviewers: Vec<NodeId>,
    /// Index of the snapshot timestamp after generation.
    pub snap_ts: polargraph_core::temporal::Timestamp,
}

// ── Dataset generator ─────────────────────────────────────────────────────────

/// Generate a scale-factor-N BSBM dataset and insert it into `store`.
///
/// Counts:
/// - Products: N × 100
/// - ProductTypes: N × 10 (3-level hierarchy: 2 roots, 3 mid, rest leaves)
/// - Features: N × 20
/// - Vendors: N × 5
/// - Offers: N × 50
/// - Reviews: N × 20 (+ N × 20 reviewers)
pub fn generate_dataset(store: &TripleStore, scale: usize) -> BsbmDataset {
    let n_products = scale * 100;
    let n_types = scale * 10;
    let n_features = scale * 20;
    let n_vendors = scale * 5;
    let n_offers = scale * 50;
    let n_reviews = scale * 20;
    let n_reviewers = n_reviews;

    let products: Vec<NodeId> = (0..n_products).map(product_id).collect();
    let product_types: Vec<NodeId> = (0..n_types).map(product_type_id).collect();
    let features: Vec<NodeId> = (0..n_features).map(feature_id).collect();
    let vendors: Vec<NodeId> = (0..n_vendors).map(vendor_id).collect();
    let offers: Vec<NodeId> = (0..n_offers).map(offer_id).collect();
    let reviews: Vec<NodeId> = (0..n_reviews).map(review_id).collect();
    let reviewers: Vec<NodeId> = (0..n_reviewers).map(reviewer_id).collect();

    const BATCH: usize = 500;
    let now = Timestamp::now();
    let _ = now; // used only as sentinel; actual Timestamp::now() is called per triple

    // ── ProductType hierarchy ─────────────────────────────────────────────────
    // Roots: types[0], types[1]
    // Mid:   types[2..5] → subClassOf types[i % 2]
    // Leaves: types[5..] → subClassOf mid type
    {
        let mut triples: Vec<Triple> = Vec::new();
        for (i, &tid) in product_types.iter().enumerate() {
            triples.push(prop_text(tid, BSBM_LABEL, &format!("ProductType {i}")));
            if (2..5).contains(&i) {
                triples.push(rel(tid, BSBM_SUBCLASS, product_types[i % 2]));
            } else if i >= 5 {
                triples.push(rel(tid, BSBM_SUBCLASS, product_types[2 + (i % 3)]));
            }
        }
        batch_insert(store, triples, BATCH);
    }

    // ── Vendors ───────────────────────────────────────────────────────────────
    {
        let countries = ["US", "DE", "FR", "JP", "GB"];
        let mut triples: Vec<Triple> = Vec::new();
        for (i, &vid) in vendors.iter().enumerate() {
            triples.push(prop_text(vid, BSBM_LABEL, &format!("Vendor {i}")));
            triples.push(prop_text(vid, BSBM_COUNTRY, countries[i % countries.len()]));
        }
        batch_insert(store, triples, BATCH);
    }

    // ── Products ──────────────────────────────────────────────────────────────
    {
        let mut triples: Vec<Triple> = Vec::new();
        for (i, &pid) in products.iter().enumerate() {
            // Basic text properties (indexed for trigram search)
            triples.push(prop_text(pid, BSBM_LABEL, &format!("Product {i} label")));
            triples.push(prop_text(
                pid,
                BSBM_COMMENT,
                &format!("Product {i} description text"),
            ));
            triples.push(prop_text(
                pid,
                BSBM_TEXT1,
                &format!("textual property one for product {i}"),
            ));
            triples.push(prop_text(
                pid,
                BSBM_TEXT2,
                &format!("textual property two for product {i}"),
            ));

            // Numeric properties: deterministic values in [0, 500)
            let base = (i * 7 + 13) as i64;
            triples.push(prop_int(pid, BSBM_NUM1, base % 500));
            triples.push(prop_int(pid, BSBM_NUM2, (base * 3 + 17) % 500));

            // Product type (leaf type, round-robin)
            let type_idx = if n_types <= 5 {
                0
            } else {
                5 + (i % (n_types - 5))
            };
            triples.push(rel(pid, BSBM_TYPE, product_types[type_idx]));

            // Features: 2–4 features per product
            let n_feats = 2 + (i % 3);
            for f in 0..n_feats {
                triples.push(rel(pid, BSBM_FEATURE, features[(i + f * 7) % n_features]));
            }

            // Producer (vendor)
            triples.push(rel(pid, BSBM_PRODUCER, vendors[i % n_vendors]));
        }
        batch_insert(store, triples, BATCH);
    }

    // ── Offers ────────────────────────────────────────────────────────────────
    {
        let mut triples: Vec<Triple> = Vec::new();
        for (i, &oid) in offers.iter().enumerate() {
            let product = products[i % n_products];
            let vendor = vendors[i % n_vendors];
            let price = 10.0 + (i as f64 * 3.7) % 990.0;
            let valid_from = 18000i64 + (i as i64 * 7) % 365; // days since epoch
            let valid_to = valid_from + 30 + (i as i64 % 60);

            triples.push(rel(oid, BSBM_OFFER_FOR, product));
            triples.push(rel(oid, BSBM_VENDOR, vendor));
            triples.push(prop_float(oid, BSBM_PRICE, price));
            triples.push(prop_int(oid, BSBM_VALID_FROM, valid_from));
            triples.push(prop_int(oid, BSBM_VALID_TO, valid_to));
        }
        batch_insert(store, triples, BATCH);
    }

    // ── Reviews ───────────────────────────────────────────────────────────────
    let last_ts = {
        let mut triples: Vec<Triple> = Vec::new();
        for (i, (&rid, &rvrid)) in reviews.iter().zip(reviewers.iter()).enumerate() {
            let product = products[i % n_products];
            triples.push(rel(rid, BSBM_REVIEW_FOR, product));
            triples.push(rel(rid, BSBM_REVIEWER, rvrid));
            triples.push(prop_int(rid, BSBM_RATING1, 1 + (i as i64 % 5)));
            triples.push(prop_int(rid, BSBM_RATING2, 1 + ((i + 1) as i64 % 5)));
        }
        commit_batch(store, triples, BATCH)
    };

    BsbmDataset {
        products,
        product_types,
        features,
        vendors,
        offers,
        reviews,
        reviewers,
        snap_ts: last_ts,
    }
}

fn batch_insert(store: &TripleStore, triples: Vec<Triple>, batch_size: usize) {
    for chunk in triples.chunks(batch_size) {
        let mut tx = store.begin();
        for t in chunk {
            tx.insert(t.clone());
        }
        tx.commit().expect("batch_insert commit");
    }
}

fn commit_batch(store: &TripleStore, triples: Vec<Triple>, batch_size: usize) -> Timestamp {
    let mut last = Timestamp(0);
    for chunk in triples.chunks(batch_size) {
        let mut tx = store.begin();
        for t in chunk {
            tx.insert(t.clone());
        }
        last = tx.commit().expect("commit_batch");
    }
    last
}

// ── Query helpers ─────────────────────────────────────────────────────────────

/// Extract the i64 value from a property triple, if it is a BSBM numeric prop.
fn int_val(t: &Triple) -> Option<i64> {
    match t {
        Triple::Property {
            value: Value::Int(n),
            ..
        } => Some(*n),
        _ => None,
    }
}

/// Extract the f64 value from a property triple.
fn float_val(t: &Triple) -> Option<f64> {
    match t {
        Triple::Property {
            value: Value::Float(f),
            ..
        } => Some(*f),
        _ => None,
    }
}

// ── 12 BSBM queries ───────────────────────────────────────────────────────────

/// Q1 — Products of ProductType T with Feature F and numericProp1 > threshold.
///
/// Equivalent SPARQL:
/// ```sparql
/// SELECT ?product WHERE {
///   ?product bsbm:type  <T> .
///   ?product bsbm:feature <F> .
///   ?product bsbm:productPropertyNumeric1 ?n1 .
///   FILTER(?n1 > ?threshold)
/// }
/// ```
pub fn q1(ds: &BsbmDataset, snap: &polargraph_storage::Snapshot) -> Vec<NodeId> {
    // Parameterisation: leaf type (index ≥5), feature[0], threshold=100.
    // Products are assigned to leaf types (indices 5+), never to root/mid types.
    let leaf_start = if ds.product_types.len() > 5 { 5 } else { 0 };
    let type_node = ds.product_types[leaf_start];
    let feature_node = ds.features[0];
    let threshold: i64 = 100;

    // Graph join: products that have the given type AND feature.
    let query = Query::new()
        .pattern(
            VarPattern::new()
                .subject(Term::var("product"))
                .predicate(BSBM_TYPE)
                .object(Term::Bound(type_node)),
        )
        .pattern(
            VarPattern::new()
                .subject(Term::var("product"))
                .predicate(BSBM_FEATURE)
                .object(Term::Bound(feature_node)),
        );
    let bindings = execute_query(&query, snap, None, None).unwrap_or_default();

    // Post-filter: numericProp1 > threshold
    bindings
        .into_iter()
        .filter_map(|b| b.get("product").copied())
        .filter(|pid| {
            snap.scan_by_subject_predicate(pid, BSBM_NUM1)
                .ok()
                .and_then(|ts| ts.into_iter().find_map(|t| int_val(&t)))
                .is_some_and(|n| n > threshold)
        })
        .collect()
}

/// Q2 — Fetch all properties of a given product (detail lookup).
///
/// Equivalent SPARQL:
/// ```sparql
/// DESCRIBE <product>
/// ```
pub fn q2(ds: &BsbmDataset, snap: &polargraph_storage::Snapshot) -> Vec<Triple> {
    let product = ds.products[ds.products.len() / 2];
    snap.scan_by_subject(&product).unwrap_or_default()
}

/// Q3 — Products with two features and numericProp1 in a range.
///
/// Equivalent SPARQL:
/// ```sparql
/// SELECT ?product WHERE {
///   ?product bsbm:feature <F1> .
///   ?product bsbm:feature <F2> .
///   ?product bsbm:productPropertyNumeric1 ?n1 .
///   FILTER(?n1 >= 100 && ?n1 <= 400)
/// }
/// ```
pub fn q3(ds: &BsbmDataset, snap: &polargraph_storage::Snapshot) -> Vec<NodeId> {
    // Features [0] and [7] are co-assigned to product indices 0, 20, 40, …
    let f1 = ds.features[0];
    let f2 = ds.features[7 % ds.features.len()];
    let lo: i64 = 100;
    let hi: i64 = 400;

    let query = Query::new()
        .pattern(
            VarPattern::new()
                .subject(Term::var("product"))
                .predicate(BSBM_FEATURE)
                .object(Term::Bound(f1)),
        )
        .pattern(
            VarPattern::new()
                .subject(Term::var("product"))
                .predicate(BSBM_FEATURE)
                .object(Term::Bound(f2)),
        );
    let bindings = execute_query(&query, snap, None, None).unwrap_or_default();

    bindings
        .into_iter()
        .filter_map(|b| b.get("product").copied())
        .filter(|pid| {
            snap.scan_by_subject_predicate(pid, BSBM_NUM1)
                .ok()
                .and_then(|ts| ts.into_iter().find_map(|t| int_val(&t)))
                .is_some_and(|n| n >= lo && n <= hi)
        })
        .collect()
}

/// Q4 — Products matching feature F1 OR feature F2 (UNION).
///
/// Equivalent SPARQL:
/// ```sparql
/// SELECT DISTINCT ?product WHERE {
///   { ?product bsbm:feature <F1> }
///   UNION
///   { ?product bsbm:feature <F2> }
/// }
/// ```
pub fn q4(ds: &BsbmDataset, snap: &polargraph_storage::Snapshot) -> Vec<NodeId> {
    use std::collections::HashSet;

    let f1 = ds.features[0];
    let f2 = ds.features[2 % ds.features.len()];

    let mut seen = HashSet::new();
    let mut result = Vec::new();

    let branch1 = Query::new().pattern(
        VarPattern::new()
            .subject(Term::var("product"))
            .predicate(BSBM_FEATURE)
            .object(Term::Bound(f1)),
    );
    let branch2 = Query::new().pattern(
        VarPattern::new()
            .subject(Term::var("product"))
            .predicate(BSBM_FEATURE)
            .object(Term::Bound(f2)),
    );

    for b in execute_query(&branch1, snap, None, None)
        .unwrap_or_default()
        .into_iter()
        .chain(execute_query(&branch2, snap, None, None).unwrap_or_default())
    {
        if let Some(&pid) = b.get("product") {
            if seen.insert(pid) {
                result.push(pid);
            }
        }
    }
    result
}

/// Q5 — Products similar to a given product (share at least one feature).
///
/// Equivalent SPARQL:
/// ```sparql
/// SELECT DISTINCT ?similar WHERE {
///   <product> bsbm:feature ?feature .
///   ?similar bsbm:feature ?feature .
///   FILTER(?similar != <product>)
/// }
/// ```
pub fn q5(ds: &BsbmDataset, snap: &polargraph_storage::Snapshot) -> Vec<NodeId> {
    use std::collections::HashSet;

    let product = ds.products[0];
    let query = Query::new()
        .pattern(
            VarPattern::new()
                .subject(Term::Bound(product))
                .predicate(BSBM_FEATURE)
                .object(Term::var("feature")),
        )
        .pattern(
            VarPattern::new()
                .subject(Term::var("similar"))
                .predicate(BSBM_FEATURE)
                .object(Term::var("feature")),
        );
    let bindings = execute_query(&query, snap, None, None).unwrap_or_default();
    let mut seen = HashSet::new();
    bindings
        .into_iter()
        .filter_map(|b| b.get("similar").copied())
        .filter(|&s| s != product && seen.insert(s))
        .collect()
}

/// Q6 — Full-text search on product label.
///
/// Uses the trigram index (same as `TripleStore::text_search`).
///
/// Equivalent SPARQL:
/// ```sparql
/// SELECT ?product WHERE {
///   ?product bsbm:label ?label .
///   FILTER(CONTAINS(?label, "label"))
/// }
/// ```
pub fn q6(_ds: &BsbmDataset, snap: &polargraph_storage::Snapshot) -> Vec<NodeId> {
    snap.text_search(BSBM_LABEL, "label").unwrap_or_default()
}

/// Q7 — Product + cheapest offer + any review (5-way join).
///
/// Equivalent SPARQL:
/// ```sparql
/// SELECT ?offer ?price ?review ?reviewer WHERE {
///   ?offer   bsbm:offerFor  <product> .
///   ?offer   bsbm:vendor    ?vendor .
///   ?offer   bsbm:price     ?price .
///   ?review  bsbm:reviewFor <product> .
///   ?review  bsbm:reviewer  ?reviewer .
/// }
/// ORDER BY ?price LIMIT 1
/// ```
pub fn q7(ds: &BsbmDataset, snap: &polargraph_storage::Snapshot) -> Option<(NodeId, f64, NodeId)> {
    let product = ds.products[ds.products.len() / 3];

    // Offers for this product
    let offer_q = Query::new()
        .pattern(
            VarPattern::new()
                .subject(Term::var("offer"))
                .predicate(BSBM_OFFER_FOR)
                .object(Term::Bound(product)),
        )
        .pattern(
            VarPattern::new()
                .subject(Term::var("offer"))
                .predicate(BSBM_VENDOR)
                .object(Term::var("vendor")),
        );
    let offers = execute_query(&offer_q, snap, None, None).unwrap_or_default();

    // Reviews for this product
    let review_q = Query::new()
        .pattern(
            VarPattern::new()
                .subject(Term::var("review"))
                .predicate(BSBM_REVIEW_FOR)
                .object(Term::Bound(product)),
        )
        .pattern(
            VarPattern::new()
                .subject(Term::var("review"))
                .predicate(BSBM_REVIEWER)
                .object(Term::var("reviewer")),
        );
    let _reviews = execute_query(&review_q, snap, None, None).unwrap_or_default();

    // Find cheapest offer
    let mut cheapest: Option<(NodeId, f64)> = None;
    for b in &offers {
        if let Some(&oid) = b.get("offer") {
            let price = snap
                .scan_by_subject_predicate(&oid, BSBM_PRICE)
                .ok()
                .and_then(|ts| ts.into_iter().find_map(|t| float_val(&t)))
                .unwrap_or(f64::MAX);
            if cheapest.map_or(true, |(_, p)| price < p) {
                cheapest = Some((oid, price));
            }
        }
    }

    // Find first reviewer
    let first_reviewer = _reviews.first().and_then(|b| b.get("reviewer").copied());

    cheapest.map(|(oid, price)| (oid, price, first_reviewer.unwrap_or_default()))
}

/// Q8 — All reviews for a product with reviewer info.
///
/// Equivalent SPARQL:
/// ```sparql
/// SELECT ?review ?reviewer WHERE {
///   ?review bsbm:reviewFor <product> .
///   ?review bsbm:reviewer  ?reviewer .
/// }
/// ```
pub fn q8(ds: &BsbmDataset, snap: &polargraph_storage::Snapshot) -> Vec<(NodeId, NodeId)> {
    let product = ds.products[1 % ds.products.len()];
    let query = Query::new()
        .pattern(
            VarPattern::new()
                .subject(Term::var("review"))
                .predicate(BSBM_REVIEW_FOR)
                .object(Term::Bound(product)),
        )
        .pattern(
            VarPattern::new()
                .subject(Term::var("review"))
                .predicate(BSBM_REVIEWER)
                .object(Term::var("reviewer")),
        );
    execute_query(&query, snap, None, None)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|b| {
            let rev = *b.get("review")?;
            let rvr = *b.get("reviewer")?;
            Some((rev, rvr))
        })
        .collect()
}

/// Q9 — All properties of a single review (detail lookup).
///
/// Equivalent SPARQL:
/// ```sparql
/// DESCRIBE <review>
/// ```
pub fn q9(ds: &BsbmDataset, snap: &polargraph_storage::Snapshot) -> Vec<Triple> {
    let review = ds.reviews[0];
    snap.scan_by_subject(&review).unwrap_or_default()
}

/// Q10 — Products offered by a specific vendor.
///
/// Equivalent SPARQL:
/// ```sparql
/// SELECT DISTINCT ?product WHERE {
///   ?offer bsbm:vendor  <vendor> .
///   ?offer bsbm:offerFor ?product .
/// }
/// ```
pub fn q10(ds: &BsbmDataset, snap: &polargraph_storage::Snapshot) -> Vec<NodeId> {
    let vendor = ds.vendors[0];
    let query = Query::new()
        .pattern(
            VarPattern::new()
                .subject(Term::var("offer"))
                .predicate(BSBM_VENDOR)
                .object(Term::Bound(vendor)),
        )
        .pattern(
            VarPattern::new()
                .subject(Term::var("offer"))
                .predicate(BSBM_OFFER_FOR)
                .object(Term::var("product")),
        );
    execute_query(&query, snap, None, None)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|b| b.get("product").copied())
        .collect()
}

/// Q11 — Review count for a product (COUNT aggregation).
///
/// Equivalent SPARQL:
/// ```sparql
/// SELECT (COUNT(?review) AS ?count) WHERE {
///   ?review bsbm:reviewFor <product> .
/// }
/// ```
pub fn q11(ds: &BsbmDataset, snap: &polargraph_storage::Snapshot) -> usize {
    let product = ds.products[ds.products.len() / 4];
    let query = Query::new().pattern(
        VarPattern::new()
            .subject(Term::var("review"))
            .predicate(BSBM_REVIEW_FOR)
            .object(Term::Bound(product)),
    );
    execute_query(&query, snap, None, None)
        .unwrap_or_default()
        .len()
}

/// Q12 — Products reviewed by a given reviewer.
///
/// Equivalent SPARQL:
/// ```sparql
/// SELECT ?product WHERE {
///   ?review bsbm:reviewer  <reviewer> .
///   ?review bsbm:reviewFor ?product .
/// }
/// ```
pub fn q12(ds: &BsbmDataset, snap: &polargraph_storage::Snapshot) -> Vec<NodeId> {
    let reviewer = ds.reviewers[0];
    let query = Query::new()
        .pattern(
            VarPattern::new()
                .subject(Term::var("review"))
                .predicate(BSBM_REVIEWER)
                .object(Term::Bound(reviewer)),
        )
        .pattern(
            VarPattern::new()
                .subject(Term::var("review"))
                .predicate(BSBM_REVIEW_FOR)
                .object(Term::var("product")),
        );
    execute_query(&query, snap, None, None)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|b| b.get("product").copied())
        .collect()
}

// ── Latency histogram ─────────────────────────────────────────────────────────

struct LatencyUs(Histogram<u64>);

impl LatencyUs {
    fn new() -> Self {
        Self(Histogram::new(2).expect("histogram"))
    }
    fn record(&mut self, d: Duration) {
        let us = d.as_micros().min(u64::MAX as u128) as u64;
        let _ = self.0.record(us);
    }
    fn p50(&self) -> u64 {
        self.0.value_at_quantile(0.50)
    }
    fn p95(&self) -> u64 {
        self.0.value_at_quantile(0.95)
    }
    fn p99(&self) -> u64 {
        self.0.value_at_quantile(0.99)
    }
    fn mean_us(&self) -> f64 {
        self.0.mean()
    }
}

// ── Scenario entry point ──────────────────────────────────────────────────────

/// Run the full BSBM suite and print results.
pub fn run_bsbm(store: &TripleStore, scale: usize, warmup: usize, measure: usize) {
    println!("\n[bsbm] generating scale-factor-{scale} dataset …");
    let t0 = Instant::now();
    let ds = generate_dataset(store, scale);
    let gen_ms = t0.elapsed().as_millis();

    println!(
        "  generated in {gen_ms} ms: {} products, {} types, {} features, {} vendors, {} offers, {} reviews",
        ds.products.len(), ds.product_types.len(), ds.features.len(),
        ds.vendors.len(), ds.offers.len(), ds.reviews.len()
    );
    println!("  warmup={warmup}  measure={measure}");
    println!();

    let snap_ts = ds.snap_ts;

    macro_rules! bench_query {
        ($label:expr, $q:ident $(, $extra:expr)?) => {{
            let mut h = LatencyUs::new();
            let total_runs = warmup + measure;
            for _ in 0..total_runs {
                let snap = store.snapshot(snap_ts);
                let t = Instant::now();
                let _result = $q(&ds, &snap $(, $extra)*);
                h.record(t.elapsed());
            }
            let mean_ms = h.mean_us() / 1000.0;
            let p50_ms = h.p50() as f64 / 1000.0;
            let p95_ms = h.p95() as f64 / 1000.0;
            let p99_ms = h.p99() as f64 / 1000.0;
            // Only measure-phase runs count toward QPS
            let qps_val = if h.mean_us() > 0.0 { 1_000_000.0 / h.mean_us() } else { 0.0 };
            println!(
                "BSBM {:>3}:  avg={:>8.3}ms  p50={:>8.3}ms  p95={:>8.3}ms  p99={:>8.3}ms  qps={:.0}",
                $label, mean_ms, p50_ms, p95_ms, p99_ms, qps_val
            );
            h.mean_us()
        }};
    }

    let mut total_avg_us = 0.0f64;
    let mut n_queries = 0u32;

    macro_rules! accumulate {
        ($avg_us:expr) => {
            total_avg_us += $avg_us;
            n_queries += 1;
        };
    }

    accumulate!(bench_query!("Q1", q1));
    accumulate!(bench_query!("Q2", q2));
    accumulate!(bench_query!("Q3", q3));
    accumulate!(bench_query!("Q4", q4));
    accumulate!(bench_query!("Q5", q5));
    accumulate!(bench_query!("Q6", q6));
    accumulate!(bench_query!("Q7", q7));
    accumulate!(bench_query!("Q8", q8));
    accumulate!(bench_query!("Q9", q9));
    accumulate!(bench_query!("Q10", q10));
    accumulate!(bench_query!("Q11", q11));
    accumulate!(bench_query!("Q12", q12));

    let avg_qps = if total_avg_us > 0.0 {
        n_queries as f64 * 1_000_000.0 / total_avg_us
    } else {
        0.0
    };
    println!();
    println!("BSBM TOTAL: avg_qps={avg_qps:.0}");
}
