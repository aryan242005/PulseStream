import json
from uuid import UUID

from app.common.config import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS processed_events (
 event_id UUID PRIMARY KEY, event_key TEXT NOT NULL, kind TEXT NOT NULL, shard INTEGER NOT NULL,
 result JSONB NOT NULL, worker_id TEXT NOT NULL, processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS event_attempts (
 id BIGSERIAL PRIMARY KEY, event_id UUID NOT NULL, worker_id TEXT NOT NULL,
 outcome TEXT NOT NULL, detail TEXT, occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS event_attempts_event_id_idx ON event_attempts(event_id);
CREATE TABLE IF NOT EXISTS workers (
 worker_id TEXT PRIMARY KEY, last_seen TIMESTAMPTZ NOT NULL, status TEXT NOT NULL,
 shards INTEGER[] NOT NULL DEFAULT '{}', processed_count BIGINT NOT NULL DEFAULT 0,
 retry_count BIGINT NOT NULL DEFAULT 0, failed_count BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS shard_ownership (
 shard_id INTEGER PRIMARY KEY, owner_id TEXT NOT NULL, fencing_token BIGINT NOT NULL,
 acquired_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS system_counters (
 name TEXT PRIMARY KEY, value BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS shipments (
 shipment_id TEXT PRIMARY KEY, description TEXT NOT NULL,
 min_temp_c DOUBLE PRECISION NOT NULL, max_temp_c DOUBLE PRECISION NOT NULL,
 latest_temp_c DOUBLE PRECISION, latest_status TEXT NOT NULL DEFAULT 'awaiting_reading',
 last_reading_at TIMESTAMPTZ, reading_count BIGINT NOT NULL DEFAULT 0,
 excursion_count BIGINT NOT NULL DEFAULT 0,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 CHECK (min_temp_c < max_temp_c)
);
CREATE TABLE IF NOT EXISTS shipment_readings (
 event_id UUID PRIMARY KEY, shipment_id TEXT NOT NULL REFERENCES shipments(shipment_id),
 sensor_id TEXT NOT NULL, temperature_c DOUBLE PRECISION NOT NULL,
 occurred_at TIMESTAMPTZ NOT NULL, in_range BOOLEAN NOT NULL,
 worker_id TEXT NOT NULL, processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS shipment_readings_recent_idx ON shipment_readings(shipment_id, occurred_at DESC);
"""


async def init_db(pool):
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA)


async def open_pool():
    import asyncpg
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL must be set")
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=12, command_timeout=10)
    await init_db(pool)
    return pool


async def record_attempt(pool, event_id, worker_id, outcome, detail=None):
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO event_attempts(event_id,worker_id,outcome,detail) VALUES($1,$2,$3,$4)", UUID(str(event_id)), worker_id, outcome, detail)


async def claim_shard(pool, shard: int, worker_id: str) -> int:
    """Advance the fencing token after the advisory lock is acquired."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("INSERT INTO shard_ownership(shard_id,owner_id,fencing_token) VALUES($1,$2,0) ON CONFLICT(shard_id) DO NOTHING", shard, worker_id)
            return await conn.fetchval("UPDATE shard_ownership SET owner_id=$2,fencing_token=fencing_token+1,acquired_at=now() WHERE shard_id=$1 RETURNING fencing_token", shard, worker_id)


class StaleShardOwner(Exception):
    pass


def owns_fence(owner_id: str, fencing_token: int, worker_id: str, expected_token: int) -> bool:
    return owner_id == worker_id and fencing_token == expected_token


async def commit_result(pool, event: dict, worker_id: str, fencing_token: int):
    """Insert the business result once and update observability in the same transaction."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            owner = await conn.fetchrow("SELECT owner_id,fencing_token FROM shard_ownership WHERE shard_id=$1 FOR UPDATE", event["shard"])
            if owner is None or not owns_fence(owner["owner_id"], owner["fencing_token"], worker_id, fencing_token):
                raise StaleShardOwner(f"worker {worker_id} has stale ownership for shard {event['shard']}")
            shipment_data = None
            if event["kind"] == "temperature_reading":
                shipment = await conn.fetchrow(
                    "SELECT min_temp_c,max_temp_c FROM shipments WHERE shipment_id=$1 FOR UPDATE",
                    event["key"],
                )
                if shipment is None:
                    raise ValueError(f"Unknown shipment: {event['key']}")
                payload = event.get("payload") or {}
                sensor_id = payload.get("sensor_id")
                temperature = payload.get("temperature_c")
                if not isinstance(sensor_id, str) or not sensor_id.strip():
                    raise ValueError("temperature_reading requires a sensor_id")
                if isinstance(temperature, bool) or not isinstance(temperature, (int, float)):
                    raise ValueError("temperature_reading requires a numeric temperature_c")
                if not -100 <= float(temperature) <= 100:
                    raise ValueError("temperature_c is outside the supported input range")
                occurred_at = event.get("occurred_at")
                if isinstance(occurred_at, str):
                    from datetime import datetime
                    occurred_at = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
                if occurred_at.tzinfo is None:
                    from datetime import timezone
                    occurred_at = occurred_at.replace(tzinfo=timezone.utc)
                in_range = shipment["min_temp_c"] <= float(temperature) <= shipment["max_temp_c"]
                shipment_data = {
                    "shipment_id": event["key"], "sensor_id": sensor_id.strip(),
                    "temperature_c": float(temperature), "occurred_at": occurred_at,
                    "in_range": in_range,
                }
            result = process_event(event, shipment_data)
            status = await conn.execute(
                "INSERT INTO processed_events(event_id,event_key,kind,shard,result,worker_id) VALUES($1,$2,$3,$4,$5::jsonb,$6) ON CONFLICT(event_id) DO NOTHING",
                UUID(str(event["event_id"])), event["key"], event["kind"], event["shard"], json.dumps(result), worker_id,
            )
            inserted = status.endswith("1")
            if inserted:
                await conn.execute("INSERT INTO system_counters(name,value) VALUES('processed',1) ON CONFLICT(name) DO UPDATE SET value=system_counters.value+1")
                if shipment_data:
                    d = shipment_data
                    await conn.execute(
                        "INSERT INTO shipment_readings(event_id,shipment_id,sensor_id,temperature_c,occurred_at,in_range,worker_id) VALUES($1,$2,$3,$4,$5,$6,$7)",
                        UUID(str(event["event_id"])), d["shipment_id"], d["sensor_id"], d["temperature_c"], d["occurred_at"], d["in_range"], worker_id,
                    )
                    await conn.execute(
                        "UPDATE shipments SET reading_count=reading_count+1, excursion_count=excursion_count+$2 WHERE shipment_id=$1",
                        d["shipment_id"], int(not d["in_range"]),
                    )
                    await conn.execute(
                        "UPDATE shipments SET latest_temp_c=$2, latest_status=$3, last_reading_at=$4 WHERE shipment_id=$1 AND (last_reading_at IS NULL OR last_reading_at <= $4)",
                        d["shipment_id"], d["temperature_c"], "in_range" if d["in_range"] else "excursion", d["occurred_at"],
                    )
            await conn.execute("INSERT INTO event_attempts(event_id,worker_id,outcome) VALUES($1,$2,$3)", UUID(str(event["event_id"])), worker_id, "processed" if inserted else "duplicate")
            await conn.execute("INSERT INTO workers(worker_id,last_seen,status,processed_count) VALUES($1,now(),'healthy',$2) ON CONFLICT(worker_id) DO UPDATE SET last_seen=now(),status='healthy',processed_count=workers.processed_count+$2", worker_id, int(inserted))
            return inserted


def process_event(event: dict, shipment_data: dict | None = None) -> dict:
    """Classify a cold-chain reading using the registered shipment policy."""
    result = {"accepted": True, "kind": event["kind"], "key": event["key"]}
    if event["kind"] == "temperature_reading" and shipment_data:
        result.update({
            "shipment_id": shipment_data["shipment_id"],
            "sensor_id": shipment_data["sensor_id"],
            "temperature_c": shipment_data["temperature_c"],
            "in_range": shipment_data["in_range"],
            "alert": None if shipment_data["in_range"] else "temperature_excursion",
            "assessment": "within_shipment_limits" if shipment_data["in_range"] else "outside_shipment_limits",
        })
    return result


async def create_shipment(pool, shipment):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "INSERT INTO shipments(shipment_id,description,min_temp_c,max_temp_c) VALUES($1,$2,$3,$4) RETURNING *",
            shipment.shipment_id, shipment.description.strip(), shipment.min_temp_c, shipment.max_temp_c,
        )
