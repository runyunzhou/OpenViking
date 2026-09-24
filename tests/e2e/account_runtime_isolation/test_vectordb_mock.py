from __future__ import annotations

import httpx

from .vectordb_mock import create_app


async def test_vectordb_mock_records_coordinates_and_scoped_faults(tmp_path):
    app = create_app(str(tmp_path / "vectordb"))

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://vectordb-mock",
    ) as client:
        response = client.get(
            "/ListVikingdbCollection",
            params={"ProjectName": "account_project_a"},
        )

        response = await response
        assert response.status_code == 200
        assert response.json()["code"] == 0
        ledger = (await client.get("/mock/requests")).json()["requests"]
        assert len(ledger) == 1
        assert ledger[0]["operation"] == "list_collections"
        assert ledger[0]["project"] == "account_project_a"
        assert ledger[0]["collection"] == ""
        await client.post("/mock/reset")
        aggregate = await client.post(
            "/api/vikingdb/data/aggregate",
            json={
                "project": "project_a",
                "collection_name": "collection_a",
                "index_name": "default",
                "agg": {"op": "count", "field": None},
                "filter": None,
            },
        )

        assert aggregate.status_code == 200
        assert aggregate.json()["data"]["agg"] == {"_total": 0}
        ledger = (await client.get("/mock/requests")).json()["requests"]
        assert len(ledger) == 1
        assert ledger[0]["operation"] == "aggregate"
        assert ledger[0]["project"] == "project_a"
        assert ledger[0]["collection"] == "collection_a"
        await client.post("/mock/reset")
        await client.post(
            "/mock/faults",
            json={
                "operation": "upsert",
                "collection": "collection_a",
                "status_code": 503,
            },
        )
        failed = await client.post(
            "/api/vikingdb/data/upsert",
            json={
                "project": "default",
                "collection_name": "collection_a",
                "fields": "[]",
            },
        )
        unaffected = await client.post(
            "/api/vikingdb/data/upsert",
            json={
                "project": "default",
                "collection_name": "collection_b",
                "fields": "[]",
            },
        )

        assert failed.status_code == 503
        assert unaffected.status_code == 200
        ledger = (await client.get("/mock/requests")).json()["requests"]
        assert ledger[0]["collection"] == "collection_a"
        assert ledger[0]["status_code"] == 503
        assert ledger[1]["collection"] == "collection_b"
