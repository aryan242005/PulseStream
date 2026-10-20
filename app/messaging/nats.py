import json

import nats
from nats.js.api import ConsumerConfig, RetentionPolicy, StorageType, StreamConfig

from app.common.config import settings

STREAM = "PULSE_EVENTS"
DLQ_STREAM = "PULSE_DLQ"


async def connect_nats():
    nc = await nats.connect(settings.nats_url, connect_timeout=5, max_reconnect_attempts=-1, reconnect_time_wait=1)
    js = nc.jetstream()
    for config in (
        StreamConfig(name=STREAM, subjects=["events.shard.*"], storage=StorageType.FILE, retention=RetentionPolicy.LIMITS),
        StreamConfig(name=DLQ_STREAM, subjects=["events.dlq"], storage=StorageType.FILE, retention=RetentionPolicy.LIMITS),
    ):
        try:
            await js.add_stream(config)
        except Exception as exc:
            try:
                await js.stream_info(config.name)
            except Exception:
                raise exc
    return nc, js


async def ensure_consumer(js, shard: int):
    name = f"PULSE_SHARD_{shard}"
    try:
        await js.add_consumer(STREAM, config=ConsumerConfig(durable_name=name, filter_subject=f"events.shard.{shard}", ack_policy="explicit", ack_wait=30, max_deliver=settings.max_deliveries + 2))
    except Exception as exc:
        if "already in use" not in str(exc).lower() and "consumer name already in use" not in str(exc).lower():
            # Consumer may already exist; verify by fetching its info before treating this as fatal.
            try:
                await js.consumer_info(STREAM, name)
            except Exception:
                raise exc
    return name


async def publish(js, subject: str, event: dict):
    return await js.publish(subject, json.dumps(event, default=str).encode(), timeout=5)
