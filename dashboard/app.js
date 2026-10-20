const $ = (selector) => document.querySelector(selector);
const escapeHtml = (value) => String(value ?? "—").replace(/[&<>"']/g, (char) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
}[char]));
const number = (value) => new Intl.NumberFormat().format(Number(value ?? 0));
const shortId = (value) => value ? `${value.slice(0, 8)}…` : "—";
const dateTime = (value) => {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString([], { dateStyle: "medium", timeStyle: "short" });
};
let toastTimer;

function setNode(id, state, good) {
  const element = $(`#${id}`);
  element.textContent = state;
  element.className = `node-state ${good ? "good" : "bad"}`;
}

function renderRows(rows, selector, columns, emptyText) {
  const body = $(selector);
  if (!rows?.length) {
    body.innerHTML = `<tr><td class="empty-cell" colspan="${columns.length}">${escapeHtml(emptyText)}</td></tr>`;
    return;
  }
  body.innerHTML = rows.map((row) => `<tr>${columns.map((column) => `<td>${column(row)}</td>`).join("")}</tr>`).join("");
}

function renderShipments(shipments) {
  renderRows(shipments, "#shipment-rows", [
    (r) => `<strong>${escapeHtml(r.shipment_id)}</strong><br><span class="muted-cell">${escapeHtml(r.description)}</span>`,
    (r) => r.latest_temp_c === null ? "—" : `${Number(r.latest_temp_c).toFixed(1)} °C`,
    (r) => `${Number(r.min_temp_c).toFixed(1)}–${Number(r.max_temp_c).toFixed(1)} °C`,
    (r) => `<span class="status-pill ${r.latest_status === "excursion" ? "dead_letter" : r.latest_status === "in_range" ? "healthy" : "unknown"}">${escapeHtml(r.latest_status.replaceAll("_", " "))}</span>`,
    (r) => number(r.reading_count),
    (r) => number(r.excursion_count),
    (r) => escapeHtml(dateTime(r.last_reading_at)),
  ], "No shipments registered.");
  const selector = $("#reading-shipment");
  const previous = selector.value;
  selector.innerHTML = `<option value="">Choose a shipment</option>${shipments.map((s) => `<option value="${escapeHtml(s.shipment_id)}">${escapeHtml(s.shipment_id)} · ${escapeHtml(s.description)}</option>`).join("")}`;
  if (shipments.some((s) => s.shipment_id === previous)) selector.value = previous;
}

function renderStats(data) {
  const counters = data.counters ?? {};
  const shipments = data.shipments ?? [];
  $("#m-shipments").textContent = number(shipments.length);
  $("#m-processed").textContent = number(shipments.reduce((total, shipment) => total + Number(shipment.reading_count ?? 0), 0));
  $("#m-retried").textContent = `${number(counters.retried ?? counters.failed_attempts)} / ${number(counters.dead_lettered)}`;
  $("#m-excursions").textContent = number(shipments.reduce((total, shipment) => total + Number(shipment.excursion_count ?? 0), 0));
  renderShipments(shipments);
  renderRows(data.recent_excursions ?? [], "#excursion-rows", [
    (r) => `<strong>${escapeHtml(r.shipment_id)}</strong><br><span class="muted-cell">${escapeHtml(r.description)}</span>`,
    (r) => escapeHtml(r.sensor_id),
    (r) => `<strong class="temperature-alert">${Number(r.temperature_c).toFixed(1)} °C</strong>`,
    (r) => `${Number(r.min_temp_c).toFixed(1)}–${Number(r.max_temp_c).toFixed(1)} °C`,
    (r) => escapeHtml(dateTime(r.occurred_at)),
    (r) => `<span class="mono" title="${escapeHtml(r.event_id)}">${escapeHtml(shortId(r.event_id))}</span>`,
  ], "No out-of-range readings recorded.");
  renderRows(data.recent_readings ?? [], "#reading-rows", [
    (r) => escapeHtml(r.shipment_id),
    (r) => escapeHtml(r.sensor_id),
    (r) => `${Number(r.temperature_c).toFixed(1)} °C`,
    (r) => `<span class="status-pill ${r.in_range ? "healthy" : "dead_letter"}">${r.in_range ? "in range" : "excursion"}</span>`,
    (r) => escapeHtml(r.worker_id),
    (r) => escapeHtml(dateTime(r.occurred_at)),
  ], "Readings will appear after processing.");

  const workers = data.workers ?? [];
  const active = workers.filter((worker) => worker.status === "healthy").length;
  const stale = workers.filter((worker) => worker.status === "stale").length;
  $("#worker-summary").textContent = `${active} healthy${stale ? ` · ${stale} stale` : ""} · ${workers.length} registered`;
  setNode("workers-state", workers.length ? `${active}/${workers.length} healthy` : "No workers", active > 0);
  const consumers = data.consumers ?? {};
  const shards = Object.entries(consumers);
  const brokerReady = shards.length > 0 && shards.some(([, stats]) => stats.pending !== null);
  setNode("nats-state", brokerReady ? "Connected" : "Metrics unavailable", brokerReady);
  setNode("db-state", "Connected", true);
  $("#shard-count").textContent = `${shards.length || 3} logical shards`;
  const maxPending = Math.max(1, ...shards.map(([, stats]) => Number(stats.pending ?? 0)));
  if (!shards.length) {
    $("#backlog-grid").innerHTML = '<div class="empty-state">Broker shard metrics are not available yet.</div>';
  } else {
    $("#backlog-grid").innerHTML = shards.map(([shard, stats]) => {
      const pending = stats.pending;
      const width = pending === null ? 0 : Math.max(pending > 0 ? 4 : 0, Number(pending) / maxPending * 100);
      return `<article class="shard-card"><div class="shard-top"><span>Shard <b class="shard-id">${escapeHtml(shard)}</b></span><span class="muted-cell">JetStream</span></div><div class="shard-stat"><span>Pending<b>${pending === null ? "—" : number(pending)}</b></span><span>Ack pending<b>${stats.ack_pending === null ? "—" : number(stats.ack_pending)}</b></span><span>Redelivered<b>${stats.redelivered === null ? "—" : number(stats.redelivered)}</b></span></div><div class="bar-track"><div class="bar-fill" style="width:${width}%"></div></div></article>`;
    }).join("");
  }

  renderRows(workers, "#worker-rows", [
    (r) => `<span class="mono">${escapeHtml(r.worker_id)}</span>`,
    (r) => `<span class="status-pill ${escapeHtml(r.status)}">${escapeHtml(r.status)}</span>`,
    (r) => escapeHtml(Array.isArray(r.shards) ? r.shards.join(", ") || "—" : JSON.stringify(r.shards)),
    (r) => number(r.processed_count),
    (r) => number(r.retry_count),
    (r) => escapeHtml(dateTime(r.last_seen)),
  ], "No workers have registered yet.");

  renderRows(data.recent_errors ?? [], "#error-rows", [
    (r) => `<span class="mono" title="${escapeHtml(r.event_id)}">${escapeHtml(shortId(r.event_id))}</span>`,
    (r) => escapeHtml(r.worker_id),
    (r) => `<span class="status-pill ${escapeHtml(r.outcome)}">${escapeHtml(r.outcome)}</span>`,
    (r) => `<span title="${escapeHtml(r.detail)}">${escapeHtml(r.detail)}</span>`,
    (r) => escapeHtml(dateTime(r.occurred_at)),
  ], "No retries or dead-letter records yet.");
}

async function refresh() {
  try {
    const signal = AbortSignal.timeout(5000);
    const [statsResponse, healthResponse] = await Promise.all([fetch("/stats", { cache: "no-store", signal }), fetch("/health/ready", { cache: "no-store", signal })]);
    if (!statsResponse.ok) throw new Error(`Stats endpoint returned HTTP ${statsResponse.status}`);
    const data = await statsResponse.json();
    renderStats(data);
    const ready = healthResponse.ok;
    setNode("api-state", ready ? "Ready" : "Not ready", ready);
    const badge = $("#live-badge");
    badge.classList.toggle("down", !ready);
    $("#live-label").textContent = ready ? "Live · auto-refresh" : "Dependencies not ready";
    $("#side-dot").classList.toggle("down", !ready);
    $("#side-state").textContent = ready ? "Pipeline connected" : "Dependencies unavailable";
    $("#connection-error").hidden = ready;
    $("#connection-error").textContent = ready ? "" : "The API is reachable, but one or more dependencies are not ready. Check the API and service container logs.";
    $("#updated").textContent = `Updated ${new Date().toLocaleTimeString()}`;
  } catch (error) {
    $("#live-badge").classList.add("down");
    $("#live-label").textContent = "Connection error";
    $("#side-dot").classList.add("down");
    $("#side-state").textContent = "API unavailable";
    setNode("api-state", "Unavailable", false);
    $("#connection-error").hidden = false;
    $("#connection-error").textContent = `${error.message}. Make sure the API container is running, then refresh.`;
  }
}

function showToast(message, isError = false) {
  const toast = $("#toast");
  toast.textContent = message;
  toast.classList.toggle("error", isError);
  toast.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => toast.classList.remove("show"), 3800);
}

async function postJson(url, body) {
  const response = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  const result = await response.json();
  if (!response.ok) throw new Error(result.detail || `HTTP ${response.status}`);
  return result;
}

$("#shipment-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = event.currentTarget;
  const button = form.querySelector("button");
  const message = $("#shipment-message");
  const body = Object.fromEntries(new FormData(form));
  body.min_temp_c = Number(body.min_temp_c);
  body.max_temp_c = Number(body.max_temp_c);
  button.disabled = true;
  message.textContent = "Registering shipment…";
  try {
    const result = await postJson("/shipments", body);
    message.textContent = `Shipment ${result.shipment_id} is ready for readings.`;
    message.className = "form-message success";
    showToast(`Registered shipment ${result.shipment_id}.`);
    await refresh();
    $("#reading-shipment").value = result.shipment_id;
  } catch (error) {
    message.textContent = error.message;
    message.className = "form-message error";
  } finally {
    button.disabled = false;
  }
});

$("#reading-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = $("#reading-button");
  const message = $("#reading-message");
  const shipmentId = $("#reading-shipment").value;
  button.disabled = true;
  message.textContent = "Publishing reading to the stream…";
  message.className = "form-message";
  try {
    const result = await postJson(`/shipments/${encodeURIComponent(shipmentId)}/readings`, {
      sensor_id: $("#sensor-id").value,
      temperature_c: Number($("#temperature").value),
    });
    message.textContent = `Accepted · shard ${result.shard} · seq ${result.sequence}`;
    message.className = "form-message success";
    showToast("Reading accepted; worker assessment will appear shortly.");
    refresh();
  } catch (error) {
    message.textContent = error.message;
    message.className = "form-message error";
  } finally {
    button.disabled = false;
  }
});

$("#refresh").addEventListener("click", refresh);
refresh();
setInterval(refresh, 3000);
