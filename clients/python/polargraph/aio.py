"""Async PolarGraph gRPC client (grpc.aio)."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional

import grpc
import grpc.aio

from ._proto import polargraph_pb2 as pb
from ._proto import polargraph_pb2_grpc as pb_grpc
from .client import (
    _binding_to_dict,
    _cypher_binding_to_dict,
    _dict_to_rule,
    _dict_to_var_pattern,
    _encode_value,
    _pb_node_id,
    _str_node_id,
)
import uuid


class AsyncPolarGraphClient:
    """Async client for the PolarGraph gRPC API using grpc.aio.

    Usage::

        async with AsyncPolarGraphClient("localhost", 50051, api_key="secret") as client:
            await client.insert_node(node_id, "Person", name="Alice")
            rows = await client.cypher("MATCH (a:Person) RETURN a LIMIT 5")
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 50051,
        api_key: Optional[str] = None,
        tls_ca_cert: Optional[str] = None,
    ) -> None:
        self._metadata = [("authorization", f"Bearer {api_key}")] if api_key else []
        target = f"{host}:{port}"
        if tls_ca_cert:
            with open(tls_ca_cert, "rb") as f:
                creds = grpc.ssl_channel_credentials(f.read())
            self._channel = grpc.aio.secure_channel(target, creds)
        else:
            self._channel = grpc.aio.insecure_channel(target)
        self._stub = pb_grpc.PolarGraphServiceStub(self._channel)

    # ── Context manager ──────────────────────────────────────────────────────

    async def __aenter__(self) -> "AsyncPolarGraphClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._channel.close()

    # ── Insert ───────────────────────────────────────────────────────────────

    async def insert_node(self, node_id: str, type_name: str, **properties: Any) -> None:
        triples: list[pb.Triple] = [
            pb.Triple(property=pb.PropertyTriple(
                subject=_pb_node_id(node_id),
                predicate="__type",
                value=_encode_value(type_name),
            ))
        ]
        for pred, val in properties.items():
            triples.append(pb.Triple(property=pb.PropertyTriple(
                subject=_pb_node_id(node_id),
                predicate=pred,
                value=_encode_value(val),
            )))
        await self._stub.Insert(pb.InsertRequest(triples=triples), metadata=self._metadata)

    async def insert_edge(
        self,
        subject: str,
        predicate: str,
        object: str,
        **edge_properties: Any,
    ) -> None:
        props = [pb.EdgeProperty(name=k, value=_encode_value(v)) for k, v in edge_properties.items()]
        triple = pb.Triple(relation=pb.RelationTriple(
            subject=_pb_node_id(subject),
            predicate=predicate,
            object=_pb_node_id(object),
            properties=props,
        ))
        await self._stub.Insert(pb.InsertRequest(triples=[triple]), metadata=self._metadata)

    # ── Query ────────────────────────────────────────────────────────────────

    async def query(
        self,
        patterns: list[dict],
        rules: Optional[list[dict]] = None,
        limit: Optional[int] = None,
        as_of_valid_time: Optional[int] = None,
        as_of_tx_time: Optional[int] = None,
    ) -> list[dict]:
        req = pb.QueryRequest(
            patterns=[_dict_to_var_pattern(p) for p in patterns],
            rules=[_dict_to_rule(r) for r in (rules or [])],
            as_of_valid_time=as_of_valid_time or 0,
            as_of_tx_time=as_of_tx_time or 0,
        )
        resp: pb.QueryResponse = await self._stub.Query(req, metadata=self._metadata)
        bindings = [_binding_to_dict(b) for b in resp.bindings]
        if limit is not None:
            bindings = bindings[:limit]
        return bindings

    async def stream_query(
        self,
        patterns: list[dict],
        rules: Optional[list[dict]] = None,
        as_of_valid_time: Optional[int] = None,
        as_of_tx_time: Optional[int] = None,
    ) -> AsyncIterator[dict]:
        req = pb.QueryRequest(
            patterns=[_dict_to_var_pattern(p) for p in patterns],
            rules=[_dict_to_rule(r) for r in (rules or [])],
            as_of_valid_time=as_of_valid_time or 0,
            as_of_tx_time=as_of_tx_time or 0,
        )
        async for chunk in self._stub.QueryStream(req, metadata=self._metadata):
            for result in chunk.results:
                yield {k: _str_node_id(v) for k, v in result.vars.items()}

    # ── Cypher ───────────────────────────────────────────────────────────────

    async def cypher(
        self,
        query: str,
        vector: Optional[list[float]] = None,
        ef: Optional[int] = None,
        params: Optional[dict] = None,
    ) -> list[dict]:
        """Execute a Cypher read query asynchronously.

        Args:
            query: Cypher query string. May contain ``$param`` placeholders.
            vector: Query embedding vector for VECTOR_NEAR clauses.
            ef: HNSW exploration factor override (0 = server default).
            params: Named parameter values for ``$param`` substitution.
        """
        serialised_params: dict = {}
        if params:
            import json
            serialised_params = {k: json.dumps(v) for k, v in params.items()}
        req = pb.CypherQueryRequest(
            cypher=query,
            vector=vector or [],
            ef=ef or 0,
            params=serialised_params,
        )
        resp: pb.CypherQueryResponse = await self._stub.CypherQuery(req, metadata=self._metadata)
        return [_cypher_binding_to_dict(r) for r in resp.rows]

    async def cypher_write(self, query: str) -> dict:
        req = pb.CypherWriteRequest(cypher=query)
        resp: pb.CypherWriteResponse = await self._stub.CypherWrite(req, metadata=self._metadata)
        return {
            "created_node_ids": [str(uuid.UUID(bytes=b)) for b in resp.created_node_ids],
            "triples_written": resp.triples_written,
            "triples_deleted": resp.triples_deleted,
        }

    # ── Vector ───────────────────────────────────────────────────────────────

    async def insert_vector(
        self, node_id: str, vector: list[float], space: str = "default"
    ) -> None:
        await self._stub.InsertVector(
            pb.InsertVectorRequest(node_id=_pb_node_id(node_id), vector=vector, space=space),
            metadata=self._metadata,
        )

    async def search_vector(
        self,
        space: str,
        vector: list[float],
        k: int,
        ef: Optional[int] = None,
    ) -> list[dict]:
        req = pb.SearchVectorRequest(query=vector, k=k, space=space, ef=ef or 0)
        resp: pb.SearchVectorResponse = await self._stub.SearchVector(req, metadata=self._metadata)
        return [
            {"node_id": _str_node_id(r.node_id), "similarity": r.similarity}
            for r in resp.results
        ]

    # ── Transactions (stubbed — see roadmap §4) ───────────────────────────────

    async def begin_tx(self) -> str:
        raise NotImplementedError("Wire transactions not yet implemented in polargraphd.")

    async def commit_tx(self, tx_id: str) -> int:
        raise NotImplementedError("Wire transactions not yet implemented.")

    async def rollback_tx(self, tx_id: str) -> None:
        raise NotImplementedError("Wire transactions not yet implemented.")

    # ── Reachability ─────────────────────────────────────────────────────────

    async def reachable(self, start: str, predicate: str, max_hops: int = 0) -> list[str]:
        resp: pb.ReachableResponse = await self._stub.Reachable(
            pb.ReachableRequest(start=_pb_node_id(start), predicate=predicate, max_hops=max_hops),
            metadata=self._metadata,
        )
        return [_str_node_id(n) for n in resp.node_ids]
