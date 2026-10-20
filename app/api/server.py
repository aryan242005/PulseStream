import logging
import zlib
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from app.common.config import settings
from app.common.schemas import EventIn, ShipmentIn, TemperatureReadingIn
from app.database.store import create_shipment, open_pool
from app.messaging.nats import connect_nats, publish

pool = None
nc = None
js = None
log = logging.getLogger("pulsestream.api")


@asynccontextmanager
async def lifespan(_app):
    global pool, nc, js
    pool = await open_pool()
    nc, js = await connect_nats()
    yield
    await nc.drain()
    await pool.close()


app = FastAPI(title="PulseStream", version="1.0.0", lifespan=lifespan)
app.mount("/assets", StaticFiles(directory=Path(__file__).parents[2] / "dashboard"), name="assets")


@app.get("/health/live")
async def live():
    return {"status": "ok"}


@app.get("/health/ready")
async def ready():
    if pool is None or nc is None or not nc.is_connected:
        raise HTTPException(503, "dependencies unavailable")
    try:
        await pool.fetchval("SELECT 1")
    except Exception as exc:
        raise HTTPException(503, "database unavailable") from exc
    return {"status": "ready"}


@app.post("/events", status_code=202)
async def submit(event: EventIn):
    return await _publish_event(event)


async def _publish_event(event: EventIn):
    data = event.model_dump(mode="json")
    shard = zlib.crc32(event.key.encode()) % settings.shard_count
    envelope = {**data, "shard": shard}
    try:
        ack = await publish(js, f"events.shard.{shard}", envelope)
    except Exception as exc:
        raise HTTPException(503, "event was not accepted by the broker") from exc
    try:
        await pool.execute("INSERT INTO system_counters(name,value) VALUES('published',1) ON CONFLICT(name) DO UPDATE SET value=system_counters.value+1")
    except Exception:
        log.exception("event was accepted but published counter could not be updated")
    return {"event_id": str(event.event_id), "shard": shard, "stream": ack.stream, "sequence": ack.seq, "status": "accepted"}


@app.post("/shipments", status_code=201)
async def register_shipment(shipment: ShipmentIn):
    try:
        row = await create_shipment(pool, shipment)
    except Exception as exc:
        if "duplicate key" in str(exc).lower() or "unique constraint" in str(exc).lower():
            raise HTTPException(409, "shipment_id is already registered") from exc
        raise
    return dict(row)


@app.get("/shipments")
async def list_shipments():
    rows = await pool.fetch("SELECT shipment_id,description,min_temp_c,max_temp_c,latest_temp_c,latest_status,last_reading_at,reading_count,excursion_count,created_at FROM shipments ORDER BY created_at DESC")
    return {"shipments": [dict(row) for row in rows]}


@app.post("/shipments/{shipment_id}/readings", status_code=202)
async def submit_temperature_reading(shipment_id: str, reading: TemperatureReadingIn):
    exists = await pool.fetchval("SELECT EXISTS(SELECT 1 FROM shipments WHERE shipment_id=$1)", shipment_id)
    if not exists:
        raise HTTPException(404, "shipment is not registered")
    return await _publish_event(EventIn(
        key=shipment_id,
        kind="temperature_reading",
        payload={"sensor_id": reading.sensor_id, "temperature_c": reading.temperature_c},
        event_id=reading.event_id,
        occurred_at=reading.occurred_at,
    ))


@app.post("/events/batch", status_code=202)
async def submit_batch(events: list[EventIn]):
    if not events or len(events) > 5000:
        raise HTTPException(400, "batch must contain 1 to 5000 events")
    accepted = []
    for event in events:
        accepted.append(await submit(event))
    return {"accepted": len(accepted), "events": accepted}


@app.get("/stats")
async def stats():
    counters = {r["name"]: r["value"] for r in await pool.fetch("SELECT name,value FROM system_counters")}
    counters["failed_attempts"] = await pool.fetchval("SELECT count(*) FROM event_attempts WHERE outcome IN ('retry','dead_letter')")
    consumers = {}
    backlog_note = "Unavailable: JetStream consumer monitoring is not exposed by this minimal API."
    for shard in range(settings.shard_count):
        try:
            info = await js.consumer_info("PULSE_EVENTS", f"PULSE_SHARD_{shard}")
            consumers[str(shard)] = {"pending": info.num_pending, "ack_pending": info.num_ack_pending, "redelivered": info.num_redelivered}
        except Exception:
            consumers[str(shard)] = {"pending": None, "ack_pending": None, "redelivered": None}
    workers = await pool.fetch("SELECT worker_id,last_seen,CASE WHEN status='healthy' AND last_seen < now() - interval '15 seconds' THEN 'stale' ELSE status END AS status,shards,processed_count,retry_count,failed_count FROM workers ORDER BY worker_id")
    errors = await pool.fetch("SELECT event_id,worker_id,outcome,detail,occurred_at FROM event_attempts WHERE outcome IN ('retry','dead_letter') ORDER BY occurred_at DESC LIMIT 20")
    shipments = await pool.fetch("SELECT shipment_id,description,min_temp_c,max_temp_c,latest_temp_c,latest_status,last_reading_at,reading_count,excursion_count FROM shipments ORDER BY last_reading_at DESC NULLS LAST,created_at DESC")
    excursions = await pool.fetch("SELECT r.event_id,r.shipment_id,s.description,r.sensor_id,r.temperature_c,s.min_temp_c,s.max_temp_c,r.occurred_at,r.worker_id FROM shipment_readings r JOIN shipments s USING(shipment_id) WHERE NOT r.in_range ORDER BY r.occurred_at DESC LIMIT 20")
    readings = await pool.fetch("SELECT event_id,shipment_id,sensor_id,temperature_c,in_range,occurred_at,worker_id FROM shipment_readings WHERE in_range ORDER BY occurred_at DESC LIMIT 20")
    return {"counters": counters, "workers": [dict(r) for r in workers], "consumers": consumers, "backlog_note": backlog_note,
            "recent_errors": [dict(r) for r in errors],
            "shipments": [dict(r) for r in shipments], "recent_excursions": [dict(r) for r in excursions],
            "recent_readings": [dict(r) for r in readings]}


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return (Path(__file__).parents[2] / "dashboard" / "index.html").read_text(encoding="utf-8")
