"""Publish diagnostic workload for pipeline throughput and delivery-recovery demos."""
import argparse
import json
import time
import urllib.request
from uuid import uuid4


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--kind", default="diagnostic")
    parser.add_argument("--failure", choices=["transient", "permanent"])
    parser.add_argument("--same-id", help="Reuse this UUID for the whole batch to demonstrate deduplication")
    parser.add_argument("--wait", action="store_true", help="Wait for these events to finish and report observed processing throughput")
    args = parser.parse_args()
    before = None
    if args.wait:
        with urllib.request.urlopen(args.url.rstrip("/") + "/stats", timeout=10) as response:
            before = json.load(response).get("counters", {})
    start = time.perf_counter()
    events = []
    for i in range(args.count):
        payload = {"sequence": i}
        if args.failure:
            payload.update({"_fail": args.failure, "fail_for": 2})
        events.append({"event_id": args.same_id or str(uuid4()), "key": f"customer-{i % 100}", "kind": args.kind, "payload": payload})
    request = urllib.request.Request(args.url.rstrip("/") + "/events/batch", data=json.dumps(events).encode(), headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=60) as response:
        result = json.load(response)
    accepted_at = time.perf_counter()
    submit_elapsed = accepted_at - start
    print(f"Accepted {result['accepted']} events in {submit_elapsed:.2f}s ({result['accepted']/submit_elapsed:.1f} submissions/s)")
    if args.wait:
        deadline = accepted_at + 300
        while time.perf_counter() < deadline:
            with urllib.request.urlopen(args.url.rstrip("/") + "/stats", timeout=10) as response:
                counters = json.load(response).get("counters", {})
            done = (counters.get("processed", 0) - before.get("processed", 0)) + (counters.get("dead_lettered", 0) - before.get("dead_lettered", 0))
            if done >= result["accepted"]:
                elapsed = time.perf_counter() - accepted_at
                print(f"All {result['accepted']} events processed or dead-lettered in {elapsed:.2f}s ({done/elapsed:.1f} events/s)")
                break
            time.sleep(1)
        else:
            raise SystemExit("Timed out waiting for processing; inspect /stats and worker logs.")


if __name__ == "__main__":
    main()
