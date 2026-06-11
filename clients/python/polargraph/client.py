"""Synchronous PolarGraph gRPC client."""

from __future__ import annotations

import uuid
from typing import Any, Generator, Iterator, Optional

import grpc

from ._proto import polargraph_pb2 as pb
from ._proto import polargraph_pb2_grpc as pb_grpc


def _node_id_bytes(node_id: str) -> bytes:
    return uuid.UUID(node_id).bytes


def _pb_node_id(node_id: str) -> pb.NodeId:
    return pb.NodeId(bytes=_node_id_bytes(node_id))


def _str_node_id(node_id: pb.NodeId) -> str:
    return str(uuid.UUID(bytes=node_id.bytes))


def _encode_value(v: Any) -> pb.Value:
    if v is None:
        return pb.Value(null_val=True)
    if isinstance(v, bool):
        return pb.Value(bool_val=v)
    if isinstance(v, int):
        return pb.Value(int_val=v)
    if isinstance(v, float):
        return pb.Value(float_val=v)
    if isinstance(v, str):
        return pb.Value(text_val=v)
    if isinstance(v, bytes):
        return pb.Value(blob_val=v)
    if isinstance(v, (list, tuple)):
        return pb.Value(vec_val=pb.FloatArray(values=[float(x) for x in v]))
    raise TypeError(f"Cannot encode value of type {type(v)}")


def _decode_value(v: pb.Value) -> Any:
    kind = v.WhichOneof("kind")
    if kind == "null_val":
        return None
    if kind == "bool_val":
        return v.bool_val
    if kind == "int_val":
        return v.int_val
    if kind == "float_val":
        return v.float_val
    if kind == "text_val":
        return v.text_val
    if kind == "blob_val":
        return v.blob_val
    if kind == "vec_val":
        return list(v.vec_val.values)
    return None


def _binding_to_dict(b: pb.Binding) -> dict:
    return {k: _str_node_id(v) for k, v in b.vars.items()}


def _cypher_binding_to_dict(b: pb.CypherBinding) -> dict:
    row: dict = {}
    for k, v in b.nodes.items():
        row[k] = _str_node_id(v)
    for k, v in b.values.items():
        row[k] = _decode_value(v)
    return row


class PolarGraphClient:
    """Synchronous client for the PolarGraph gRPC API.

    Usage::

        with PolarGraphClient("localhost", 50051, api_key="secret") as client:
            client.insert_node(node_id, "Person", name="Alice")
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 50051,
        api_key: Optional[str] = None,
        tls_ca_cert: Optional[str] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._api_key = api_key
        self._metadata = [("authorization", f"Bearer {api_key}")] if api_key else []

        target = f"{host}:{port}"
        if tls_ca_cert:
            with open(tls_ca_cert, "rb") as f:
                creds = grpc.ssl_channel_credentials(f.read())
            self._channel = grpc.secure_channel(target, creds)
        else:
            self._channel = grpc.insecure_channel(target)

        self._stub = pb_grpc.PolarGraphServiceStub(self._channel)

    # ── Context manager ──────────────────────────────────────────────────────

    def __enter__(self) -> "PolarGraphClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        self._channel.close()

    # ── Insert helpers ───────────────────────────────────────────────────────

    def insert_node(self, node_id: str, type_name: str, **properties: Any) -> None:
        """Insert a node with a type label and optional property triples."""
        triples: list[pb.Triple] = []
        triples.append(pb.Triple(property=pb.PropertyTriple(
            subject=_pb_node_id(node_id),
            predicate="__type",
            value=_encode_value(type_name),
        )))
        for pred, val in properties.items():
            triples.append(pb.Triple(property=pb.PropertyTriple(
                subject=_pb_node_id(node_id),
                predicate=pred,
                value=_encode_value(val),
            )))
        self._stub.Insert(pb.InsertRequest(triples=triples), metadata=self._metadata)

    def insert_edge(
        self,
        subject: str,
        predicate: str,
        object: str,
        **edge_properties: Any,
    ) -> None:
        """Insert a directed relation triple, with optional edge properties."""
        props = [pb.EdgeProperty(name=k, value=_encode_value(v)) for k, v in edge_properties.items()]
        triple = pb.Triple(relation=pb.RelationTriple(
            subject=_pb_node_id(subject),
            predicate=predicate,
            object=_pb_node_id(object),
            properties=props,
        ))
        self._stub.Insert(pb.InsertRequest(triples=[triple]), metadata=self._metadata)

    def insert(self, triples: list[pb.Triple]) -> pb.InsertResponse:
        """Low-level insert: pass raw proto triples."""
        return self._stub.Insert(pb.InsertRequest(triples=triples), metadata=self._metadata)

    # ── Query ────────────────────────────────────────────────────────────────

    def query(
        self,
        patterns: list[dict],
        rules: Optional[list[dict]] = None,
        limit: Optional[int] = None,
        as_of_valid_time: Optional[int] = None,
        as_of_tx_time: Optional[int] = None,
    ) -> list[dict]:
        """Run a conjunctive pattern query.

        Each pattern dict has keys 's', 'p', 'o'.  A string starting with '?'
        is a variable; a UUID string is a bound node; omitted or None is a wildcard.

        Returns a list of binding dicts mapping variable names to node-ID strings.
        """
        var_patterns = [_dict_to_var_pattern(p) for p in patterns]
        datalog_rules = [_dict_to_rule(r) for r in (rules or [])]
        req = pb.QueryRequest(
            patterns=var_patterns,
            rules=datalog_rules,
            as_of_valid_time=as_of_valid_time or 0,
            as_of_tx_time=as_of_tx_time or 0,
        )
        resp: pb.QueryResponse = self._stub.Query(req, metadata=self._metadata)
        bindings = [_binding_to_dict(b) for b in resp.bindings]
        if limit is not None:
            bindings = bindings[:limit]
        return bindings

    def stream_query(
        self,
        patterns: list[dict],
        rules: Optional[list[dict]] = None,
        as_of_valid_time: Optional[int] = None,
        as_of_tx_time: Optional[int] = None,
    ) -> Iterator[dict]:
        """Stream query results one binding at a time."""
        var_patterns = [_dict_to_var_pattern(p) for p in patterns]
        datalog_rules = [_dict_to_rule(r) for r in (rules or [])]
        req = pb.QueryRequest(
            patterns=var_patterns,
            rules=datalog_rules,
            as_of_valid_time=as_of_valid_time or 0,
            as_of_tx_time=as_of_tx_time or 0,
        )
        for chunk in self._stub.QueryStream(req, metadata=self._metadata):
            for result in chunk.results:
                yield {k: _str_node_id(v) for k, v in result.vars.items()}

    # ── Cypher ───────────────────────────────────────────────────────────────

    def cypher(
        self,
        query: str,
        vector: Optional[list[float]] = None,
        ef: Optional[int] = None,
    ) -> list[dict]:
        """Execute a Cypher read query. Returns list of row dicts."""
        req = pb.CypherQueryRequest(
            cypher=query,
            vector=vector or [],
            ef=ef or 0,
        )
        resp: pb.CypherQueryResponse = self._stub.CypherQuery(req, metadata=self._metadata)
        return [_cypher_binding_to_dict(r) for r in resp.rows]

    def cypher_write(self, query: str) -> dict:
        """Execute a Cypher write statement (CREATE/MERGE/SET/DELETE)."""
        req = pb.CypherWriteRequest(cypher=query)
        resp: pb.CypherWriteResponse = self._stub.CypherWrite(req, metadata=self._metadata)
        return {
            "created_node_ids": [str(uuid.UUID(bytes=b)) for b in resp.created_node_ids],
            "triples_written": resp.triples_written,
            "triples_deleted": resp.triples_deleted,
        }

    # ── Vector ───────────────────────────────────────────────────────────────

    def insert_vector(
        self, node_id: str, vector: list[float], space: str = "default"
    ) -> None:
        """Insert or update a node's embedding in a named HNSW space."""
        self._stub.InsertVector(
            pb.InsertVectorRequest(
                node_id=_pb_node_id(node_id),
                vector=vector,
                space=space,
            ),
            metadata=self._metadata,
        )

    def search_vector(
        self,
        space: str,
        vector: list[float],
        k: int,
        ef: Optional[int] = None,
    ) -> list[dict]:
        """Search for k nearest neighbours in a named HNSW space."""
        req = pb.SearchVectorRequest(query=vector, k=k, space=space, ef=ef or 0)
        resp: pb.SearchVectorResponse = self._stub.SearchVector(req, metadata=self._metadata)
        return [
            {"node_id": _str_node_id(r.node_id), "similarity": r.similarity}
            for r in resp.results
        ]

    # ── Transactions ─────────────────────────────────────────────────────────
    # These methods stub the wire-transaction API described in the roadmap §4.
    # When BeginTransaction/CommitTransaction/RollbackTransaction RPCs land on
    # the server, these methods will call them directly. For now they raise
    # NotImplementedError so callers get a clear error rather than a silent no-op.

    def begin_tx(self) -> str:
        raise NotImplementedError(
            "Wire transactions are not yet implemented in polargraphd. "
            "See roadmap §4."
        )

    def commit_tx(self, tx_id: str) -> int:
        raise NotImplementedError("Wire transactions not yet implemented.")

    def rollback_tx(self, tx_id: str) -> None:
        raise NotImplementedError("Wire transactions not yet implemented.")

    # ── Reachability ─────────────────────────────────────────────────────────

    def reachable(
        self, start: str, predicate: str, max_hops: int = 0
    ) -> list[str]:
        """Return all node IDs reachable from start via predicate."""
        req = pb.ReachableRequest(
            start=_pb_node_id(start),
            predicate=predicate,
            max_hops=max_hops,
        )
        resp: pb.ReachableResponse = self._stub.Reachable(req, metadata=self._metadata)
        return [_str_node_id(n) for n in resp.node_ids]

    # ── Schema ───────────────────────────────────────────────────────────────

    def explain(self, patterns: list[dict], rules: Optional[list[dict]] = None) -> dict:
        """Return the static query execution plan without running the query."""
        req = pb.QueryRequest(
            patterns=[_dict_to_var_pattern(p) for p in patterns],
            rules=[_dict_to_rule(r) for r in (rules or [])],
        )
        resp: pb.ExplainResponse = self._stub.ExplainQuery(req, metadata=self._metadata)
        return {"plan_text": resp.plan_text, "nodes": list(resp.nodes)}


# ── Pattern helpers ──────────────────────────────────────────────────────────

def _make_term(v: Optional[str]) -> Optional[pb.Term]:
    if v is None or v == "_":
        return None
    if v.startswith("?"):
        return pb.Term(var=v.lstrip("?"))
    return pb.Term(bound=_pb_node_id(v))


def _dict_to_var_pattern(p: dict) -> pb.VarPattern:
    s_term = _make_term(p.get("s") or p.get("subject"))
    o_term = _make_term(p.get("o") or p.get("object"))
    pred = p.get("p") or p.get("predicate") or ""
    vp = pb.VarPattern(predicate=pred.lstrip(":"))
    if s_term is not None:
        vp.subject.CopyFrom(s_term)
    if o_term is not None:
        vp.object.CopyFrom(o_term)
    return vp


def _dict_to_rule(r: dict) -> pb.DatalogRule:
    return pb.DatalogRule(
        head_predicate=r["head_predicate"],
        head_subject_var=r["head_subject_var"],
        head_object_var=r["head_object_var"],
        body=[_dict_to_var_pattern(p) for p in r.get("body", [])],
    )
