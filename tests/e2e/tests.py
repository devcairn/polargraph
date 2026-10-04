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
import urllib.parse
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
    """GET /indexes lists the storage-format-v3 column families."""
    status, data = http_get(base_url + "/indexes")
    assert status == 200, f"indexes failed with status {status}: {data}"
    assert "column_families" in data, f"expected 'column_families' key: {data}"
    cf_names = [cf.get("name") for cf in data["column_families"]]
    for expected in ("spog", "gspo", "meta", "iri", "chg"):
        assert expected in cf_names, f"expected {expected!r} in column families, got: {cf_names}"


# ── Runner ────────────────────────────────────────────────────────────────────

def test_vocabulary_runtime_types(base_url: str):
    """A prefix and a class added at runtime work from Cypher and SPARQL at once."""
    unique = uuid.uuid4().hex[:8]
    ns = f"http://e2e.example/{unique}/"
    status, vocab = http_post(base_url + "/vocabulary/prefixes", {"name": "e2e", "namespace": ns})
    assert status == 200, f"put prefix failed with status {status}: {vocab}"
    assert vocab["prefixes"].get("e2e") == ns, f"prefix not in vocabulary: {vocab}"
    assert vocab["legacy"]["conversion_pending"] is False, f"unexpected legacy data: {vocab}"

    name = f"w-{unique}"
    status, data = http_post(base_url + "/cypher/write", {
        "cypher": f"CREATE (w:`e2e:Widget` {{name: '{name}'}})"
    })
    assert status == 200, f"cypher/write failed with status {status}: {data}"

    status, data = http_post(base_url + "/cypher", {
        "cypher": f"MATCH (w:`e2e:Widget`) WHERE w.name = '{name}' RETURN w"
    })
    assert status == 200, f"cypher failed with status {status}: {data}"
    assert len(data.get("results", [])) == 1, f"expected one widget: {data}"

    query = (
        f"SELECT ?w WHERE {{ ?w a <{ns}Widget> . "
        f"?w <urn:pg:vocab:name> \"{name}\" }}"
    )
    status, data = http_get(base_url + "/sparql?" + urllib.parse.urlencode({"query": query}))
    assert status == 200, f"sparql failed with status {status}: {data}"
    bindings = data["results"]["bindings"]
    assert len(bindings) == 1, f"expected one SPARQL binding: {data}"


def test_cypher_write_deprecated(base_url: str):
    """POST /cypher/write still works and is marked deprecated."""
    body = json.dumps({"cypher": f"CREATE (n {{tag: '{uuid.uuid4().hex[:8]}'}})"}).encode()
    req = urllib.request.Request(
        base_url + "/cypher/write", data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200, f"cypher/write failed with status {resp.status}"
        assert resp.headers.get("Deprecation") == "true", f"no Deprecation header: {resp.headers}"
        assert "/changes" in (resp.headers.get("Warning") or ""), f"no Warning header: {resp.headers}"


def test_changes_replace_cypher_writes(base_url: str):
    """POST /changes writes a typed node by class name; Cypher reads it."""
    unique = uuid.uuid4().hex[:8]
    node = new_id()
    label = f"E2eThing{unique}"
    rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    status, data = http_post(base_url + "/changes", {"adds": [{"triples": [
        {"subject": node, "predicate": rdf_type, "object": label},
        {"subject": node, "predicate": "name", "value": f"t-{unique}"},
    ]}]})
    assert status == 200, f"/changes failed with status {status}: {data}"

    status, data = http_post(base_url + "/cypher", {"cypher": f"MATCH (x:{label}) RETURN x, x.name"})
    assert status == 200, f"cypher failed with status {status}: {data}"
    results = data.get("results", [])
    assert len(results) == 1 and results[0].get("x") == node, f"expected the new node: {data}"

    status, data = http_post(base_url + "/changes", {"strict": True, "retractions": [
        {"subject": node, "predicate": "name", "value": f"t-{unique}"},
    ]})
    assert status == 200 and data.get("retracted") == 1, f"retraction failed: {status} {data}"


def test_query_value_bindings(base_url: str):
    """/query binds a variable to a property value and returns it in @values."""
    node = new_id()
    pred = f"title_{uuid.uuid4().hex[:8]}"
    status, data = http_post(base_url + "/changes", {"adds": [{"triples": [
        {"subject": node, "predicate": pred, "value": "Dune"},
    ]}]})
    assert status == 200, f"/changes failed with status {status}: {data}"

    status, data = http_post(base_url + "/query", {"patterns": [f"?b :{pred} ?t"]})
    assert status == 200, f"query failed with status {status}: {data}"
    results = data.get("results", [])
    assert len(results) == 1, f"expected one row: {data}"
    assert results[0].get("b") == node, f"expected the subject node: {data}"
    assert results[0].get("@values", {}).get("t") == "Dune", f"expected the title value: {data}"


def sparql_select(base_url: str, query: str):
    status, data = http_get(base_url + "/sparql?" + urllib.parse.urlencode({"query": query}))
    assert status == 200, f"sparql failed with status {status}: {data}"
    return data["results"]["bindings"]


def sparql_update(base_url: str, update: str, params: dict = None):
    url = base_url + "/sparql/update"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url, data=update.encode(),
        headers={"Content-Type": "application/sparql-update"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {}


def test_sparql_literal_results(base_url: str):
    """SPARQL returns literals: FILTER on values, ORDER BY, GROUP BY + SUM, ?p."""
    ns = f"http://e2e.example/{uuid.uuid4().hex[:8]}/"
    books = [("Dune", 1965, 412), ("Emma", 1815, 474), ("Ubik", 1969, 202)]
    triples = []
    for title, year, pages in books:
        b = new_id()
        triples += [
            {"subject": b, "predicate": ns + "title", "value": title},
            {"subject": b, "predicate": ns + "year", "value": year},
            {"subject": b, "predicate": ns + "pages", "value": pages},
        ]
    status, data = http_post(base_url + "/changes", {"adds": [{"triples": triples}]})
    assert status == 200, f"/changes failed with status {status}: {data}"

    rows = sparql_select(base_url,
        f"SELECT ?t ?y WHERE {{ ?b <{ns}title> ?t ; <{ns}year> ?y FILTER(?y > 1900) }} ORDER BY DESC(?y)")
    assert [r["t"]["value"] for r in rows] == ["Ubik", "Dune"], f"filter / order: {rows}"
    assert rows[0]["y"]["datatype"] == "http://www.w3.org/2001/XMLSchema#integer", rows

    rows = sparql_select(base_url, f"SELECT (SUM(?p) AS ?total) WHERE {{ ?b <{ns}pages> ?p }}")
    assert rows[0]["total"]["value"] == "1088", f"sum: {rows}"

    rows = sparql_select(base_url,
        f"SELECT ?p WHERE {{ ?b <{ns}title> \"Dune\" ; ?p ?o }} ORDER BY ?p")
    assert [r["p"]["value"] for r in rows] == [ns + "pages", ns + "title", ns + "year"], rows


def test_sparql_update_with_value_variables(base_url: str):
    """DELETE WHERE { <n> ?p ?o } closes every quad of n; INSERT copies a value."""
    ns = f"http://e2e.example/{uuid.uuid4().hex[:8]}/"
    a, b = new_id(), new_id()
    status, data = http_post(base_url + "/changes", {"adds": [{"triples": [
        {"subject": a, "predicate": ns + "name", "value": "Ada"},
        {"subject": a, "predicate": ns + "born", "value": 1815},
        {"subject": a, "predicate": ns + "knows", "object": b},
        {"subject": b, "predicate": ns + "name", "value": "Bob"},
    ]}]})
    assert status == 200, f"/changes failed: {status} {data}"

    # Copy Ada's name to a new predicate via a value variable.
    status, data = sparql_update(base_url,
        f"INSERT {{ ?s <{ns}label> ?n }} WHERE {{ ?s <{ns}name> \"Ada\" ; <{ns}name> ?n }}")
    assert status == 200 and data["inserted"] == 1, f"insert: {data}"
    rows = sparql_select(base_url, f"SELECT ?l WHERE {{ ?s <{ns}label> ?l }}")
    assert [r["l"]["value"] for r in rows] == ["Ada"], rows

    status, data = sparql_update(base_url, f"DELETE WHERE {{ <urn:uuid:{a}> ?p ?o }}")
    assert status == 200 and data["failed"] == 0, f"delete: {data}"
    assert data["deleted"] == 4, f"name, born, knows and label closed: {data}"
    rows = sparql_select(base_url, f"SELECT ?p ?o WHERE {{ <urn:uuid:{a}> ?p ?o }}")
    assert rows == [], f"nothing left: {rows}"
    rows = sparql_select(base_url, f"SELECT ?n WHERE {{ <urn:uuid:{b}> <{ns}name> ?n }}")
    assert [r["n"]["value"] for r in rows] == ["Bob"], f"other node untouched: {rows}"


def test_sparql_filter_functions(base_url: str):
    """STRSTARTS / LANG / REGEX / ?a < ?b / STR of a node over REST."""
    ns = f"http://e2e.example/{uuid.uuid4().hex[:8]}/"
    a, b = new_id(), new_id()
    status, data = http_post(base_url + "/changes", {"adds": [{"triples": [
        {"subject": a, "predicate": ns + "title", "value": {"@value": "Bonjour", "@language": "fr"}},
        {"subject": a, "predicate": ns + "min", "value": 3},
        {"subject": a, "predicate": ns + "max", "value": 9},
        {"subject": b, "predicate": ns + "title", "value": "Hello"},
        {"subject": b, "predicate": ns + "min", "value": 5},
        {"subject": b, "predicate": ns + "max", "value": 2},
    ]}]})
    assert status == 200, f"/changes failed: {status} {data}"

    def titles(where_filter):
        rows = sparql_select(base_url,
            f"SELECT ?t WHERE {{ ?s <{ns}title> ?t ; <{ns}min> ?lo ; <{ns}max> ?hi FILTER({where_filter}) }}")
        return sorted(r["t"]["value"] for r in rows)

    assert titles('STRSTARTS(?t, "Bon")') == ["Bonjour"]
    assert titles('LANG(?t) = "fr"') == ["Bonjour"]
    assert titles('REGEX(?t, "^hel", "i")') == ["Hello"]
    assert titles("?lo < ?hi") == ["Bonjour"]
    assert titles(f'STR(?s) = "urn:uuid:{b}"') == ["Hello"]


def test_sparql_update_atomic(base_url: str):
    """One request = one transaction: dry_run, all-or-nothing, net deletes, read_ts, no mixing."""
    ns = f"http://e2e.example/{uuid.uuid4().hex[:8]}/"
    a, b = ns + "a", ns + "b"

    def names(node):
        rows = sparql_select(base_url, f"SELECT ?n WHERE {{ <{node}> <{ns}name> ?n }}")
        return sorted(r["n"]["value"] for r in rows)

    # dry_run returns the changeset and applies nothing.
    update = f'INSERT DATA {{ <{a}> <{ns}name> "Ada" }}'
    status, data = sparql_update(base_url, update, {"dry_run": "true"})
    assert status == 200 and data["dry_run"], f"dry run: {status} {data}"
    triples = data["changeset"]["adds"][0]["triples"]
    assert triples == [{"subject": a, "predicate": ns + "name", "value": "Ada"}], data
    assert names(a) == [], "dry run applied nothing"

    # A rejected quad (writes to inferred graphs are refused) fails the whole request.
    status, data = sparql_update(base_url,
        f'INSERT DATA {{ <{a}> <{ns}name> "Ada" . '
        f'GRAPH <urn:pg:inferred:default> {{ <{b}> <{ns}name> "Bob" }} }}')
    assert status == 403, f"inferred-graph write rejected: {status} {data}"
    assert names(a) == [], "nothing applied"

    # Several operations commit together; a later DELETE undoes an earlier INSERT.
    status, data = sparql_update(base_url,
        f'INSERT DATA {{ <{a}> <{ns}name> "Ada" . <{b}> <{ns}name> "Bob" }} ; '
        f'DELETE DATA {{ <{b}> <{ns}name> "Bob" }}')
    assert status == 200 and data["inserted"] == 1 and data["commit_ts"] > 0, f"atomic: {data}"
    assert names(a) == ["Ada"] and names(b) == [], (names(a), names(b))

    # WHERE reads the request's read point; quads it retracts that changed
    # after an explicit read_ts make the request a conflict.
    read_ts = data["commit_ts"]
    status, data = http_post(base_url + "/changes", {
        "retractions": [{"subject": a, "predicate": ns + "name", "value": "Ada"}],
        "adds": [{"triples": [{"subject": a, "predicate": ns + "name", "value": "Augusta"}]}],
    })
    assert status == 200, f"/changes: {status} {data}"
    status, data = sparql_update(base_url,
        f'DELETE {{ ?s <{ns}name> ?n }} INSERT {{ ?s <{ns}name> "Ada L." }} '
        f'WHERE {{ ?s <{ns}name> ?n }}', {"read_ts": str(read_ts)})
    assert status == 409, f"stale read_ts: {status} {data}"
    status, data = sparql_update(base_url,
        f'DELETE {{ ?s <{ns}name> ?n }} INSERT {{ ?s <{ns}name> "Ada L." }} '
        f'WHERE {{ ?s <{ns}name> ?n }}')
    assert status == 200 and data["deleted"] == 1 and data["inserted"] == 1, f"rename: {data}"
    assert names(a) == ["Ada L."], names(a)

    # Graph operations can't share a request with data operations.
    status, data = sparql_update(base_url,
        f'INSERT DATA {{ <{b}> <{ns}name> "Bob" }} ; CLEAR GRAPH <{ns}g>')
    assert status == 400, f"mixed request: {status} {data}"
    assert names(b) == []


def test_inferred_facts_via_sparql(base_url: str):
    """Materialized subclass instances are visible to SPARQL; ?inferred=false hides them."""
    ns = f"http://e2e.example/{uuid.uuid4().hex[:8]}/"
    rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    sco = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
    tom = new_id()
    status, data = http_post(base_url + "/changes", {"adds": [{"triples": [
        {"subject": ns + "Cat", "predicate": sco, "object": ns + "Animal"},
        {"subject": tom, "predicate": rdf_type, "object": ns + "Cat"},
    ]}]})
    assert status == 200, f"/changes failed: {status} {data}"
    status, data = http_post(base_url + "/materialize", {})
    assert status == 200 and data["asserted"] >= 1, f"materialize: {status} {data}"

    query = f"SELECT ?a WHERE {{ ?a a <{ns}Animal> }}"
    rows = sparql_select(base_url, query)
    assert [r["a"]["value"] for r in rows] == [f"urn:uuid:{tom}"], rows
    status, data = http_get(base_url + "/sparql?" + urllib.parse.urlencode({"query": query, "inferred": "false"}))
    assert status == 200 and data["results"]["bindings"] == [], f"opt-out: {data}"


def test_counters(base_url: str):
    """POST /counters adds atomically; GET /counters reads them back."""
    ns = f"used-{uuid.uuid4().hex[:8]}"
    a, b = new_id(), new_id()
    for delta in (2, 3):
        status, data = http_post(base_url + "/counters", {"namespace": ns, "increments": [{"node": a, "delta": delta}]})
        assert status == 200, f"increment: {status} {data}"
    status, data = http_get(base_url + "/counters?" + urllib.parse.urlencode({"namespace": ns, "nodes": f"{a},{b}"}))
    assert status == 200 and data["counters"] == {a: 5, b: 0}, data


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
    test_vocabulary_runtime_types,
    test_cypher_write_deprecated,
    test_changes_replace_cypher_writes,
    test_query_value_bindings,
    test_sparql_literal_results,
    test_sparql_update_with_value_variables,
    test_sparql_update_atomic,
    test_sparql_filter_functions,
    test_inferred_facts_via_sparql,
    test_counters,
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
