import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "")
    nats_url: str = os.getenv("NATS_URL", "nats://localhost:4222")
    shard_count: int = int(os.getenv("SHARD_COUNT", "3"))
    max_deliveries: int = int(os.getenv("MAX_DELIVERIES", "5"))
    concurrency: int = int(os.getenv("WORKER_CONCURRENCY", "4"))
    poll_seconds: float = float(os.getenv("WORKER_POLL_SECONDS", "2"))
    app_name: str = os.getenv("WORKER_ID", f"worker-{os.getenv('HOSTNAME', 'local')}")


settings = Settings()
