import asyncio
import logging
import os
import sys

from app.api import serve
from app.worker.runner import run_workers


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    role = sys.argv[1] if len(sys.argv) > 1 else os.getenv("ROLE", "api")
    if role in {"api", "monitoring"}:
        serve()
    elif role == "worker":
        asyncio.run(run_workers())
    else:
        raise SystemExit(f"Unknown role: {role}. Use api or worker.")


if __name__ == "__main__":
    main()
