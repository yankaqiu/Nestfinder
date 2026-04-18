from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pymilvus import DataType, MilvusClient


@dataclass(slots=True)
class VectorImageRecord:
    row_id: str
    listing_id: str
    image_id: str
    image_url: str
    scrape_source: str
    model_name: str
    model_version: str
    embedding: list[float]


@dataclass(slots=True)
class ImageSearchHit:
    listing_id: str
    image_id: str
    image_url: str | None
    score: float


class MilvusLiteStore:
    def __init__(self, *, uri: str, collection_name: str) -> None:
        self._client = MilvusClient(uri=uri)
        self._collection_name = collection_name

    @property
    def collection_name(self) -> str:
        return self._collection_name

    def ensure_collection(self, *, dimension: int) -> None:
        if self._client.has_collection(collection_name=self._collection_name):
            self._safe_load_collection()
            return

        schema = MilvusClient.create_schema(
            auto_id=False,
            enable_dynamic_field=False,
        )
        schema.add_field(field_name="id", datatype=DataType.VARCHAR, is_primary=True, max_length=128)
        schema.add_field(field_name="listing_id", datatype=DataType.VARCHAR, max_length=64)
        schema.add_field(field_name="image_id", datatype=DataType.VARCHAR, max_length=128)
        schema.add_field(field_name="image_url", datatype=DataType.VARCHAR, max_length=2048)
        schema.add_field(field_name="scrape_source", datatype=DataType.VARCHAR, max_length=64)
        schema.add_field(field_name="model_name", datatype=DataType.VARCHAR, max_length=256)
        schema.add_field(field_name="model_version", datatype=DataType.VARCHAR, max_length=128)
        schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=dimension)

        index_params = self._client.prepare_index_params()
        index_params.add_index(field_name="id", index_type="STL_SORT")
        index_params.add_index(
            field_name="embedding",
            index_type="AUTOINDEX",
            metric_type="COSINE",
        )

        self._client.create_collection(
            collection_name=self._collection_name,
            schema=schema,
            index_params=index_params,
        )
        self._safe_load_collection()

    def upsert(self, records: list[VectorImageRecord]) -> None:
        if not records:
            return
        payload = [
            {
                "id": record.row_id,
                "listing_id": record.listing_id,
                "image_id": record.image_id,
                "image_url": record.image_url,
                "scrape_source": record.scrape_source,
                "model_name": record.model_name,
                "model_version": record.model_version,
                "embedding": record.embedding,
            }
            for record in records
        ]
        self._client.upsert(collection_name=self._collection_name, data=payload)

    def list_row_ids_for_listing(self, *, listing_id: str, model_name: str) -> list[str]:
        self._safe_load_collection()
        rows = self._client.query(
            collection_name=self._collection_name,
            filter=_join_conditions(
                [
                    _eq("listing_id", listing_id),
                    _eq("model_name", model_name),
                ]
            ),
            output_fields=["id"],
            limit=16384,
        )
        return [str(row["id"]) for row in rows]

    def delete_ids(self, ids: list[str]) -> None:
        if not ids:
            return
        self._safe_load_collection()
        self._client.delete(collection_name=self._collection_name, ids=ids)

    def search(
        self,
        *,
        query_vector: list[float],
        listing_ids: list[str],
        model_name: str,
        top_k: int,
        chunk_size: int,
        search_k_multiplier: int,
    ) -> list[ImageSearchHit]:
        self._safe_load_collection()
        hits: list[ImageSearchHit] = []
        search_limit = max(top_k, min(500, top_k * max(1, search_k_multiplier)))

        for chunk in _chunked(listing_ids, chunk_size):
            result = self._client.search(
                collection_name=self._collection_name,
                data=[query_vector],
                anns_field="embedding",
                limit=search_limit,
                filter=_join_conditions(
                    [
                        _eq("model_name", model_name),
                        _in("listing_id", chunk),
                    ]
                ),
                output_fields=["listing_id", "image_id", "image_url"],
                search_params={"metric_type": "COSINE"},
            )
            for batch in result:
                for hit in batch:
                    payload = dict(hit)
                    entity = payload.get("entity", {}) or {}
                    hits.append(
                        ImageSearchHit(
                            listing_id=str(entity.get("listing_id", "")),
                            image_id=str(entity.get("image_id") or payload.get("id") or ""),
                            image_url=entity.get("image_url"),
                            score=float(payload.get("distance", payload.get("score", 0.0))),
                        )
                    )
        return hits

    def row_count(self) -> int:
        if not self._client.has_collection(collection_name=self._collection_name):
            return 0
        stats = self._client.get_collection_stats(collection_name=self._collection_name)
        return int(stats.get("row_count", 0))

    def _safe_load_collection(self) -> None:
        try:
            self._client.load_collection(collection_name=self._collection_name)
        except Exception:
            return


def _chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _join_conditions(conditions: list[str]) -> str:
    return " and ".join(condition for condition in conditions if condition)


def _eq(field_name: str, value: str) -> str:
    return f"{field_name} == {_quote(value)}"


def _in(field_name: str, values: list[str]) -> str:
    quoted = ", ".join(_quote(value) for value in values)
    return f"{field_name} in [{quoted}]"


def _quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"
