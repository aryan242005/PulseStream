"""Register demo shipments, publish sensor readings, and wait for assessments."""
import argparse
import json
import time
import urllib.error
import urllib.request
from uuid import uuid4


def request_json(url, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"} if data is not None else {},
        method="POST" if data is not None else "GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8000", help="PulseStream API base URL")
    parser.add_argument("--timeout", type=int, default=60, help="Seconds to wait for worker assessments")
    args = parser.parse_args()
    base = args.url.rstrip("/")
    run_id = uuid4().hex[:8].upper()
    shipment_a = f"DEMO-{run_id}-A"
    shipment_b = f"DEMO-{run_id}-B"
    shipments = [
        {"shipment_id": shipment_a, "description": "Demo chilled cargo", "min_temp_c": 2, "max_temp_c": 8},
        {"shipment_id": shipment_b, "description": "Demo frozen cargo", "min_temp_c": -25, "max_temp_c": -15},
    ]
    readings = [
        (shipment_a, "sensor-a", 5.2),
        (shipment_a, "sensor-a", 7.8),
        (shipment_a, "sensor-a", 9.1),
        (shipment_a, "sensor-a", 5.4),
        (shipment_b, "sensor-b", -20.0),
        (shipment_b, "sensor-b", -27.0),
    ]
    for shipment in shipments:
        request_json(f"{base}/shipments", shipment)
    event_ids = set()
    for shipment_id, sensor_id, temperature in readings:
        result = request_json(f"{base}/shipments/{shipment_id}/readings", {
            "sensor_id": sensor_id,
            "temperature_c": temperature,
        })
        event_ids.add(result["event_id"])
        print(f"Accepted {shipment_id}: {temperature:.1f} °C on shard {result['shard']}")

    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        stats = request_json(f"{base}/stats")
        observed = {row["event_id"] for row in stats.get("recent_readings", [])}
        if event_ids <= observed:
            print(f"Processed {len(event_ids)} readings.")
            for row in stats.get("recent_excursions", []):
                if row["shipment_id"] in (shipment_a, shipment_b):
                    print(f"Excursion: {row['shipment_id']} reported {row['temperature_c']:.1f} °C "
                          f"(configured band {row['min_temp_c']:.1f}–{row['max_temp_c']:.1f} °C)")
            print("Open the dashboard to review shipment status and the reading history.")
            return
        time.sleep(1)
    raise SystemExit("Timed out waiting for processing; inspect /stats and worker logs.")


if __name__ == "__main__":
    main()
