import asyncio
import json
import logging
import signal
import json as jsonlib
from contextlib import suppress

import asyncpg

from app.common.config import settings
from app.database.store import StaleShardOwner, claim_shard, commit_result, open_pool, record_attempt
from app.messaging.nats import connect_nats, ensure_consumer, publish
from app.worker.policy import retry_delay, should_dead_letter

log = logging.getLogger("pulsestream.worker")


async def _own_shard(pool, shard):
    """Hold a PostgreSQL session advisory lock for the lifetime of shard consumption."""
    conn = await asyncpg.connect(settings.database_url)
    acquired = await conn.fetchval("SELECT pg_try_advisory_lock($1, $2)", 731002, shard)
    if not acquired:
        await conn.close()
        return None
    return conn


async def _handle(msg, js, pool, worker_id, fencing_token, stopping):
    event = json.loads(msg.data)
    meta = msg.metadata
    try:
        payload = event.get("payload", {})
        if payload.get("_fail") == "transient" and meta.num_delivered < int(payload.get("fail_for", 2)):
            raise ConnectionError("injected transient failure")
        if payload.get("_fail") == "permanent":
            raise ValueError("injected permanent failure")
        inserted = await commit_result(pool, event, worker_id, fencing_token)
        await msg.ack()
        log.info("event handled id=%s shard=%s duplicate=%s", event["event_id"], event["shard"], not inserted)
    except StaleShardOwner as exc:
        await msg.nak(delay=1)
        log.error("stale shard owner detected; stopping worker: %s", exc)
        stopping.set()
    except (ConnectionError, TimeoutError) as exc:
        await record_attempt(pool, event["event_id"], worker_id, "retry", str(exc))
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO system_counters(name,value) VALUES('retried',1) ON CONFLICT(name) DO UPDATE SET value=system_counters.value+1")
            await conn.execute("INSERT INTO workers(worker_id,last_seen,status,retry_count) VALUES($1,now(),'healthy',1) ON CONFLICT(worker_id) DO UPDATE SET last_seen=now(),retry_count=workers.retry_count+1", worker_id)
        await msg.nak(delay=retry_delay(meta.num_delivered))
    except Exception as exc:
        if should_dead_letter(meta.num_delivered):
            await publish(js, "events.dlq", {**event, "failure": str(exc), "deliveries": meta.num_delivered})
            await record_attempt(pool, event["event_id"], worker_id, "dead_letter", str(exc))
            async with pool.acquire() as conn:
                await conn.execute("INSERT INTO system_counters(name,value) VALUES('dead_lettered',1) ON CONFLICT(name) DO UPDATE SET value=system_counters.value+1")
                await conn.execute("INSERT INTO workers(worker_id,last_seen,status,failed_count) VALUES($1,now(),'healthy',1) ON CONFLICT(worker_id) DO UPDATE SET last_seen=now(),failed_count=workers.failed_count+1", worker_id)
            await msg.ack()
            log.error("event dead-lettered id=%s", event["event_id"])
        else:
            await record_attempt(pool, event["event_id"], worker_id, "retry", str(exc))
            async with pool.acquire() as conn:
                await conn.execute("INSERT INTO system_counters(name,value) VALUES('retried',1) ON CONFLICT(name) DO UPDATE SET value=system_counters.value+1")
            await msg.nak(delay=retry_delay(meta.num_delivered))


async def run_workers():
    pool = await open_pool()
    nc, js = await connect_nats()
    worker_id = settings.app_name
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stopping.set)
    owned = {}
    tasks = set()
    shard_jobs = {}

    async def health(reader, writer):
        request = await reader.readline()
        path = request.decode(errors="replace").split(" ")[1] if request else "/"
        body = jsonlib.dumps({"status": "ok", "worker": worker_id, "shards": sorted(owned)}).encode()
        status = b"200 OK" if path in ("/health/live", "/health/ready") else b"404 Not Found"
        writer.write(b"HTTP/1.1 " + status + b"\r\nContent-Type: application/json\r\nContent-Length: " + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    health_server = await asyncio.start_server(health, "0.0.0.0", 8001)

    async def consume(shard, lock_conn):
        while not stopping.is_set():
            try:
                fencing_token = await claim_shard(pool, shard, worker_id)
                break
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("could not establish shard fence for shard=%s; retrying", shard)
                await asyncio.sleep(2)
        sem = asyncio.Semaphore(settings.concurrency)
        async def one(msg):
            async with sem:
                await _handle(msg, js, pool, worker_id, fencing_token, stopping)
        while not stopping.is_set():
            sub = None
            try:
                durable = await ensure_consumer(js, shard)
                sub = await js.pull_subscribe(f"events.shard.{shard}", durable=durable)
                try:
                    while not stopping.is_set():
                        try:
                            batch = await sub.fetch(settings.concurrency, timeout=settings.poll_seconds)
                        except Exception as exc:
                            if "timeout" in str(exc).lower():
                                continue
                            raise
                        for msg in batch:
                            task = asyncio.create_task(one(msg))
                            tasks.add(task)
                            task.add_done_callback(tasks.discard)
                except Exception:
                    log.exception("fetch failed for shard=%s; reconnecting", shard)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("consumer setup failed for shard=%s; retrying", shard)
            finally:
                if sub:
                    await sub.unsubscribe()
            if not stopping.is_set():
                await asyncio.sleep(2)

    try:
        while not stopping.is_set():
            await pool.execute("INSERT INTO workers(worker_id,last_seen,status,shards) VALUES($1,now(),'healthy',$2) ON CONFLICT(worker_id) DO UPDATE SET last_seen=now(),shards=$2,status='healthy'", worker_id, list(owned))
            # Let concurrently starting replicas register before dividing shards.
            if not owned:
                await asyncio.sleep(3)
            active = await pool.fetch("SELECT worker_id FROM workers WHERE status='healthy' AND last_seen > now() - interval '10 seconds' ORDER BY worker_id")
            worker_ids = [row["worker_id"] for row in active]
            if not worker_ids:
                worker_ids = [worker_id]
            for shard in range(settings.shard_count):
                if len(worker_ids) <= settings.shard_count:
                    desired_owner = worker_ids[shard % len(worker_ids)]
                else:
                    desired_owner = worker_ids[shard]
                if shard in owned and desired_owner != worker_id:
                    job = shard_jobs.pop(shard, None)
                    if job:
                        job.cancel()
                        await asyncio.gather(job, return_exceptions=True)
                    lock = owned.pop(shard)
                    await lock.close()
                    log.info("released shard=%s for balanced assignment", shard)
                if desired_owner == worker_id and shard not in owned:
                    lock = await _own_shard(pool, shard)
                    if lock:
                        owned[shard] = lock
                        log.info("acquired shard=%s", shard)
                        task = asyncio.create_task(consume(shard, lock))
                        shard_jobs[shard] = task
                        tasks.add(task)
                        task.add_done_callback(tasks.discard)
            await pool.execute("UPDATE workers SET last_seen=now(),shards=$2,status='healthy' WHERE worker_id=$1", worker_id, list(owned))
            await asyncio.sleep(2)
    finally:
        stopping.set()
        for task in list(tasks):
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await pool.execute("UPDATE workers SET status='stopped',shards='{}' WHERE worker_id=$1", worker_id)
        for lock in owned.values():
            await lock.close()
        await nc.drain()
        await pool.close()
        health_server.close()
        await health_server.wait_closed()
