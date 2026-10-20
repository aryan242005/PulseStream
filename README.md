# PulseStream

PulseStream is a cold-chain shipment monitoring demonstration built on a distributed event pipeline. Operators register each shipment with its own allowed temperature band, then submit sensor readings. NATS JetStream buffers readings by shipment, workers classify them against the registered limits, and PostgreSQL keeps the latest shipment state, every processed reading, and excursion history. The dashboard is designed around that operator workflow, with worker and shard health available alongside it.

Temperature limits are supplied by the operator for each shipment. The sample values are illustrative; this project does not prescribe handling limits, determine product viability, or replace a validated monitoring system.

## Architecture and guarantees

```mermaid
flowchart LR
  C[Sensor / simulator] -->|shipment reading| A[FastAPI ingestion + dashboard]
  A -->|CRC32(shipment ID) % 3| S0[NATS shard 0]
  A --> S1[NATS shard 1]
  A --> S2[NATS shard 2]
  S0 --> W[Worker replicas]
  S1 --> W
  S2 --> W
  W -->|classify against shipment band| P[(PostgreSQL)]
  W -->|permanent failures| D[NATS dead-letter stream]
  A -->|actual counters and recent records| P
```

Each reading uses its shipment ID as the partition key so that readings for one shipment share a shard. A worker stores the assessment, reading history, and latest shipment state in one database transaction. An out-of-range reading is recorded as an excursion event for operator review; it is not automatically treated as proof that cargo is unsafe or safe.

The UI supports shipment registration and sensor readings directly. The API also provides `POST /shipments`, `GET /shipments`, and `POST /shipments/{shipment_id}/readings`. The general `POST /events` endpoint remains available for event-pipeline integrations and delivery demonstrations.

The producer returns HTTP 202 only after JetStream acknowledges persistence. `zlib.crc32(key) % 3` routes a key consistently to one subject (`events.shard.N`); equal keys therefore share a shard. Ordering is only the broker's order within that subject and durable consumer, and concurrent processing can finish out of order. There is no total order across shards.

Workers take per-shard PostgreSQL session advisory locks. The lock holder is the only worker that subscribes to that shard; PostgreSQL releases the lock if its session/process dies. After replicas register at startup, active worker IDs are sorted and shards are assigned round-robin; when there are more workers than shards, only the first `SHARD_COUNT` IDs own one shard each. A worker releases shards when membership changes, and another acquires them. On acquisition, the worker advances a persistent fencing token. Each result transaction locks the ownership row and verifies both owner and token before writing, so a worker whose lock session was replaced cannot commit after the new token is recorded. On restart another worker can acquire the shard. This is a simple single-PostgreSQL coordinator, not consensus or a highly available lock service. The coordinator records worker heartbeats, but status can be stale for up to the heartbeat interval. On a network partition, PostgreSQL session failure detection and TCP timeouts determine when reassignment occurs. An expired/dead consumer may leave an unacknowledged message for redelivery; transactions plus the unique `processed_events.event_id` constraint prevent duplicate business effects.

Delivery is **at least once**, not exactly once. A worker can commit its result and die before acknowledging; JetStream will redeliver, and the second transaction becomes a no-op for the business result. Transient errors are NAKed with bounded exponential delay. Failures at the configured delivery limit are copied to `PULSE_DLQ` and acknowledged from the source. Permanent failure attempts and retry counts are recorded in PostgreSQL.

The dashboard's event counters, worker records, and recent-event tables come from PostgreSQL. Shard pending/ack-pending/redelivery values are queried from JetStream. Queue values are shown as unavailable if that broker query fails; processing throughput is not fabricated or presented as a live rate.

## Docker Compose quick start

Prerequisites: Docker Engine or Docker Desktop with Compose v2. Copy the example environment file first; Compose intentionally refuses to start while required settings are missing. In PowerShell:

```powershell
Copy-Item .env.example .env
docker compose up --build -d
docker compose ps
```

Open <http://localhost:8000> for the dashboard or <http://localhost:8000/docs> for API docs. `.env.example` contains a local-only sample password; change `POSTGRES_PASSWORD` and the matching `DATABASE_URL` in `.env` before exposing this beyond your machine. Compose networking uses service names, never container `localhost`.

Useful commands:

```powershell
docker compose logs -f api worker
docker compose exec api python -c "import urllib.request; print(urllib.request.urlopen('http://localhost:8000/health/ready').read().decode())"
docker compose up -d --scale worker=5
docker compose down
docker compose down -v   # also permanently deletes NATS and PostgreSQL data
```

## Minikube / Kubernetes

Prerequisites: Docker, kubectl, and Minikube. Start Minikube with enough resources for PostgreSQL and the worker pool (for example 4 CPUs and 5 GB memory). Change both placeholder password values in the `pulsestream-secret` in `k8s/01-config.yaml` to the same URL-safe password before deployment; this is a template, not a production secret manager.

```powershell
minikube start --cpus=4 --memory=5120
minikube image build -t pulsestream:local .
./scripts/deploy-minikube.ps1
kubectl port-forward svc/pulsestream-api 8000:8000 -n pulsestream
```

In another terminal, visit <http://localhost:8000>. Scale the worker pool with:

```powershell
kubectl scale deployment/pulsestream-worker --replicas=5 -n pulsestream
kubectl get pods -n pulsestream -w
```

The API can scale horizontally after placing it behind a suitable ingress/service because it is stateless apart from shared dependencies. Workers can scale horizontally; only the three PostgreSQL locks' holders consume the three shards, so replicas beyond three remain healthy but idle until a lock becomes free. Scaling the shard count requires a coordinated migration of API routing, JetStream consumers, and worker configuration. Keep the single NATS and PostgreSQL instances for this local demo: their storage is persistent, but they are not an HA cluster.

Teardown: `kubectl delete namespace pulsestream`. PVC data may be retained by the Minikube storage provisioner; inspect/delete the PVCs separately when you intentionally want a clean database and broker.

## Cold-chain demo

With Compose running, run this from the project folder:

```powershell
python scripts/simulate_cold_chain.py
```

The simulator creates two uniquely named shipments, publishes six readings including out-of-band values, waits for the workers, and prints the assessments. Limits in this simulator are sample data only. Open <http://localhost:8000> to inspect the shipment table, excursion events, reading history, and distributed pipeline health. You can also register a shipment in the UI and submit readings by hand.

## Pipeline demonstrations

With Compose running, these commands are available in PowerShell from the project root.

**A — pipeline load:** publish 1,000 diagnostic events, wait for completion, and report observed processing throughput. This exercises broker and worker capacity; use the cold-chain simulator above for shipment behavior.

```powershell
python scripts/publish.py --count 1000 --wait
docker compose logs -f worker
```

**B — worker crash:** publish a large batch, then run `docker compose kill worker` while it is processing. Restart with `docker compose up -d worker`. Messages not acknowledged before termination are redelivered after JetStream's `ack_wait`; verify counters, shipment readings, and delivery attempts at `/stats` and watch worker logs. The exact time depends on whether the message was already committed and the local machine's scheduling.

**C — duplicate delivery:** submit the same event UUID twice. The unique database key yields one business row; attempts show the duplicate delivery.

```powershell
$id = [guid]::NewGuid().ToString()
python scripts/publish.py --count 2 --same-id $id
```

**D — retry and dead letter:** a transient demo succeeds after two deliveries. A permanent failure exhausts the delivery limit and appears in the DLQ counter and recent error records.

```powershell
python scripts/publish.py --count 1 --failure transient
python scripts/publish.py --count 1 --failure permanent
```

**E — Kubernetes scale:** scale from three to five replicas as above. Check five healthy pods; only up to three can own shards because there are three shards.

**F — persistence:** restart NATS and PostgreSQL containers with `docker compose restart nats postgres`, then verify `/stats` and pending JetStream messages. Compose named volumes preserve data. In Kubernetes, deleting/restarting Pods while retaining PVCs preserves broker/database files.

## Tests and development

Python 3.12+ is required. Install and run:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
pytest
```

Tests cover shipment band validation, temperature excursion classification, deterministic shard mapping, retry policy, fencing, and idempotency. Database uniqueness and broker redelivery are integration behaviors; demonstrate them against Compose using scenarios C and B. Configuration is supplied by environment variables in `.env.example` and Kubernetes ConfigMap/Secret. The shipment and reading tables are created idempotently at application startup.

## Troubleshooting

- `docker compose ps` should show NATS and PostgreSQL healthy before the API and workers start. Use `docker compose logs nats postgres api worker` for startup errors.
- If you change `.env` credentials after initializing PostgreSQL's volume, the existing database user password does not change automatically. Either update the database password or intentionally recreate the local volume with `docker compose down -v`.
- Kubernetes Pods stuck pending usually indicate Minikube storage/resources. Inspect `kubectl describe pod -n pulsestream` and `kubectl get pvc -n pulsestream`.
- `kubectl logs deployment/pulsestream-worker -n pulsestream` shows current ownership and processing. A shard can have only one advisory-lock owner at a time.
- Backlog metrics are broker-derived. If unavailable, check NATS readiness and JetStream consumer initialization in API logs.
