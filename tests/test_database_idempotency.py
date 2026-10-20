"""Optional integration test: set DATABASE_URL to a disposable PostgreSQL database."""
import asyncio
import os
from uuid import uuid4

import pytest


@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="requires disposable PostgreSQL via DATABASE_URL")
def test_duplicate_event_has_one_business_result():
    async def scenario():
        import asyncpg
        from app.database.store import claim_shard, commit_result, init_db

        pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
        await init_db(pool)
        event_id = uuid4()
        event = {"event_id": str(event_id), "key": "dedupe-test", "kind": "metric", "shard": 0, "payload": {"value": 1}}
        try:
            token = await claim_shard(pool, 0, "test-worker")
            assert await commit_result(pool, event, "test-worker", token) is True
            assert await commit_result(pool, event, "test-worker", token) is False
            assert await pool.fetchval("SELECT count(*) FROM processed_events WHERE event_id=$1", event_id) == 1
        finally:
            await pool.execute("DELETE FROM event_attempts WHERE event_id=$1", event_id)
            await pool.execute("DELETE FROM processed_events WHERE event_id=$1", event_id)
            await pool.execute("DELETE FROM shard_ownership WHERE shard_id=0 AND owner_id='test-worker'")
            await pool.close()

    asyncio.run(scenario())
