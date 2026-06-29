#!/usr/bin/env python3
"""E2E test suite for the PolarGraph REST gateway.

Each test_* function receives base_url and raises AssertionError on failure.
Run via run.sh (which sets BASE_URL) or directly:
    BASE_URL=http://localhost:8000 python3 tests.py
"""

import json
import os
import sys
import uuid
import urllib.request
import urllib.error

# ── HTTP helpers ──────────────────────────────────────────────────────────────

def new_id() -> str:
    return str(uuid.uuid4())


def http_get(url: str):
    """Returns (status_code, parsed_json)."""
    with urllib.request.urlopen(url) as resp:
        return resp.status, json.loads(resp.read())


def http_post(url: str, body: dict):
    """Returns (status_code, parsed_json). HTTP errors are caught and returned."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {}


def http_post_raw(url: str, body: dict):
    """Returns (status_code, content_type_header, body_bytes)."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.headers.get("Content-Type", ""), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, "", b""


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_health(base_url: str):
    """GET /health returns {status: ok}."""
    status, data = http_get(base_url + "/health")
    assert status == 200, f"expected HTTP 200, got {status}"
    assert data.get("status") == "ok", f"expected status=ok, got {data}"


def test_insert_relation(base_url: str):
    """Insert a relation triple and query it back by variable pattern."""
    subj = new_id()
    obj  = new_id()
    pred = f"knows_{uuid.uuid4().hex[:8]}"

    status, data = http_post(base_url + "/insert", {
        "subject": subj, "predicate": pred, "object": obj
    })
    assert status == 200, f"insert failed with status {status}: {data}"
    assert data.get("ok") is True, f"insert response missing ok=true: {data}"

    status, data = http_post(base_url + "/query", {
        "patterns": [f"?s :{pred} ?o"]
    })
    assert status == 200, f"query failed with status {status}: {data}"
    results = data.get("results", [])
    assert len(results) >= 1, f"expected at least 1 result, got {results}"
    assert any(r.get("s") == subj and r.get("o") == obj for r in results), \
        f"expected ({subj}, {obj}) in query results: {results}"


def test_insert_property(base_url: str):
    """Insert a text property via Cypher write, then query it back."""
    prop_val = f"propval_{uuid.uuid4().hex[:8]}"

    status, data = http_post(base_url + "/cypher/write", {
        "cypher": f"CREATE (n {{job: '{prop_val}'}})"
    })
    assert status == 200, f"cypher/write failed with status {status}: {data}"
    assert data.get("ok") is True, f"cypher/write missing ok=true: {data}"
    assert len(data.get("created_node_ids", [])) >= 1, \
        f"expected created_node_ids in response: {data}"

    status, data = http_post(base_url + "/cypher", {
        "cypher": f"MATCH (n) WHERE n.job = '{prop_val}' RETURN n"
    })
    assert status == 200, f"cypher property query failed with status {status}: {data}"
    assert len(data.get("results", [])) >= 1, \
        f"expected results for property query (prop_val={prop_val}): {data}"


def test_multi_pattern_join(base_url: str):
    """Two-pattern join: insert A→works_at→B and B→located_in→C, resolve C."""
    node_a = new_id()
    node_b = new_id()
    node_c = new_id()
    pred1  = f"works_at_{uuid.uuid4().hex[:8]}"
    pred2  = f"located_in_{uuid.uuid4().hex[:8]}"

    http_post(base_url + "/insert", {"subject": node_a, "predicate": pred1, "object": node_b})
    http_post(base_url + "/insert", {"subject": node_b, "predicate": pred2, "object": node_c})

    status, data = http_post(base_url + "/query", {
        "patterns": [f"?a :{pred1} ?b", f"?b :{pred2} ?c"]
    })
    assert status == 200, f"join query failed with status {status}: {data}"
    results = data.get("results", [])
    assert any(r.get("c") == node_c for r in results), \
        f"expected node_c={node_c} in join results: {results}"


def test_cypher_simple(base_url: str):
    """MATCH (a)-[:pred]->(b) RETURN a, b after inserting the relation."""
    node_a = new_id()
    node_b = new_id()
    pred   = f"cyknows_{uuid.uuid4().hex[:8]}"

    http_post(base_url + "/insert", {"subject": node_a, "predicate": pred, "object": node_b})

    status, data = http_post(base_url + "/cypher", {
        "cypher": f"MATCH (a)-[:{pred}]->(b) RETURN a, b"
    })
    assert status == 200, f"cypher simple failed with status {status}: {data}"
    results = data.get("results", [])
    assert any(r.get("a") == node_a and r.get("b") == node_b for r in results), \
        f"expected ({node_a}, {node_b}) in cypher results: {results}"


def test_cypher_text_search(base_url: str):
    """Insert a node with a text property; find it via CONTAINS using the trigram index."""
    unique    = uuid.uuid4().hex[:8]
    name_val  = f"foobar_{unique}"

    status, data = http_post(base_url + "/cypher/write", {
        "cypher": f"CREATE (n {{fullname: '{name_val}'}})"
    })
    assert status == 200, f"cypher/write failed with status {status}: {data}"

    status, data = http_post(base_url + "/cypher", {
        "cypher": f"MATCH (n) WHERE n.fullname CONTAINS '{name_val}' RETURN n"
    })
    assert status == 200, f"cypher text search failed with status {status}: {data}"
    assert len(data.get("results", [])) >= 1, \
        f"expected trigram search results for '{name_val}': {data}"


def test_time_travel(base_url: str):
    """Query with as_of_tx_time returns only triples committed before the cutoff."""
    node_a = new_id()
    node_b = new_id()
    node_c = new_id()
    node_d = new_id()
    pred   = f"tt_{uuid.uuid4().hex[:8]}"

    _, resp1 = http_post(base_url + "/insert", {
        "subject": node_a, "predicate": pred, "object": node_b
    })
    tx_time_1 = resp1.get("tx_time")
    assert tx_time_1 is not None, f"no tx_time in first insert response: {resp1}"

    _, resp2 = http_post(base_url + "/insert", {
        "subject": node_c, "predicate": pred, "object": node_d
    })
    tx_time_2 = resp2.get("tx_time")
    assert tx_time_2 is not None, f"no tx_time in second insert response: {resp2}"
    assert tx_time_2 > tx_time_1, \
        f"expected tx_time_2 > tx_time_1, got {tx_time_2} vs {tx_time_1}"

    # Query the snapshot at tx_time_1 — only the first triple should be visible.
    status, data = http_post(base_url + "/query", {
        "patterns": [f"?s :{pred} ?o"],
        "as_of_tx_time": tx_time_1
    })
    assert status == 200, f"time-travel query failed with status {status}: {data}"
    results = data.get("results", [])

    subject_ids = {r.get("s") for r in results}
    assert node_a in subject_ids, \
        f"node_a should appear in snapshot at tx_time_1: {results}"
    assert node_c not in subject_ids, \
        f"node_c committed AFTER tx_time_1 should not appear: {results}"


def test_vector_search(base_url: str):
    """POST /vector/search returns {results: [...]} JSON shape.

    Note: InsertVector is a gRPC-only RPC not proxied by the REST gateway,
    so the result set may be empty. This test confirms the endpoint is wired
    up and returns the correct response shape.
    """
    status, data = http_post(base_url + "/vector/search", {
        "vector": [1.0, 0.0, 0.0],
        "top_k": 5,
        "namespace": "default"
    })
    assert status == 200, f"vector/search failed with status {status}: {data}"
    assert "results" in data, f"expected 'results' key in response: {data}"
    assert isinstance(data["results"], list), \
        f"results should be a list, got {type(data['results'])}: {data}"


def test_explain(base_url: str):
    """POST /explain returns a plan with a non-empty plan_text field."""
    status, data = http_post(base_url + "/explain", {
        "patterns": ["?a :works_at ?b", "?b :located_in ?c"]
    })
    assert status == 200, f"explain failed with status {status}: {data}"
    assert "plan_text" in data, f"expected 'plan_text' key in explain response: {data}"
    assert len(data["plan_text"]) > 0, f"plan_text should be non-empty: {data}"


def test_streaming(base_url: str):
    """POST /query/stream returns Content-Type: application/x-ndjson with valid JSON lines."""
    node_x = new_id()
    node_y = new_id()
    pred   = f"stream_{uuid.uuid4().hex[:8]}"

    http_post(base_url + "/insert", {"subject": node_x, "predicate": pred, "object": node_y})

    status, content_type, body = http_post_raw(base_url + "/query/stream", {
        "patterns": [f"?s :{pred} ?o"]
    })
    assert status == 200, f"query/stream failed with status {status}"
    assert "ndjson" in content_type.lower(), \
        f"expected application/x-ndjson Content-Type, got: {content_type!r}"

    lines = [ln for ln in body.decode().split("\n") if ln.strip()]
    assert len(lines) >= 1, f"expected at least one NDJSON line in response: {body!r}"

    for line in lines:
        obj = json.loads(line)
        assert isinstance(obj, dict), f"each NDJSON line must be a JSON object, got: {line!r}"


def test_stats(base_url: str):
    """GET /stats returns a JSON object containing mvcc_oracle_ts."""
    status, data = http_get(base_url + "/stats")
    assert status == 200, f"stats failed with status {status}: {data}"
    assert "mvcc_oracle_ts" in data, f"expected 'mvcc_oracle_ts' key in stats: {data}"


def test_indexes(base_url: str):
    """GET /indexes returns column_families array containing SPO."""
    status, data = http_get(base_url + "/indexes")
    assert status == 200, f"indexes failed with status {status}: {data}"
    assert "column_families" in data, f"expected 'column_families' key: {data}"
    cf_names = [cf.get("name") for cf in data["column_families"]]
    assert "spo" in cf_names, f"expected 'spo' in column families, got: {cf_names}"


# ── Runner ────────────────────────────────────────────────────────────────────

TESTS = [
    test_health,
    test_insert_relation,
    test_insert_property,
    test_multi_pattern_join,
    test_cypher_simple,
    test_cypher_text_search,
    test_time_travel,
    test_vector_search,
    test_explain,
    test_streaming,
    test_stats,
    test_indexes,
]


def main():
    base_url = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")

    passed = 0
    failed = 0

    for test_fn in TESTS:
        name = test_fn.__name__
        try:
            test_fn(base_url)
            print(f"PASS  {name}")
            passed += 1
        except AssertionError as exc:
            print(f"FAIL  {name}: {exc}")
            failed += 1
        except Exception as exc:
            print(f"FAIL  {name}: unexpected exception: {exc}")
            failed += 1

    total = passed + failed
    print(f"\n{passed}/{total} tests passed", end="")
    if failed:
        print(f"  ({failed} failed)")
    else:
        print()

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
