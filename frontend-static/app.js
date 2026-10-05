const defaultConfig = { gatewayUrl: "", userId: "organizer-1", workspaceId: "workspace-1", roles: "OWNER,ORGANIZER,PARTICIPANT,CHECKIN_STAFF" };

const state = {
  config: loadConfig(),
  events: [],
  selected: null,
  lastResponse: null,
  busy: new Set()
};

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];
const uid = () => globalThis.crypto?.randomUUID?.() || "local-" + Date.now() + "-" + Math.random().toString(16).slice(2);

function loadConfig() {
  const saved = sessionStorage.getItem("eventflow-console-config");
  return saved ? { ...defaultConfig, ...JSON.parse(saved) } : { ...defaultConfig };
}

async function loadRuntimeConfig() {
  if (sessionStorage.getItem("eventflow-console-config")) return;
  try {
    const response = await fetch("config.json?ts=" + Date.now(), { cache: "no-store" });
    if (!response.ok) return;
    const remote = await response.json();
    const gatewayUrl = remote.gatewayUrl || remote.base || "";
    if (gatewayUrl) state.config.gatewayUrl = String(gatewayUrl).replace(/\/$/, "");
  } catch {
    // Optional runtime config; the form remains usable when config.json is absent.
  }
}

function saveConfig() {
  state.config = {
    gatewayUrl: $("#gateway-url").value.trim().replace(/\/$/, ""),
    userId: $("#user-id").value.trim(),
    workspaceId: $("#workspace-id").value.trim(),
    roles: $("#roles").value.trim()
  };
  sessionStorage.setItem("eventflow-console-config", JSON.stringify(state.config));
  toast("Runtime config saved for this tab.", "good");
}

function initConfig() {
  $("#gateway-url").value = state.config.gatewayUrl;
  $("#user-id").value = state.config.userId;
  $("#workspace-id").value = state.config.workspaceId;
  $("#roles").value = state.config.roles;
  $("#idempotency-key").value = uid();
  const now = new Date(Date.now() + 60 * 60 * 1000);
  const later = new Date(Date.now() + 2 * 60 * 60 * 1000);
  $("#create-event-form [name=startsAt]").value = toLocalInput(now);
  $("#create-event-form [name=endsAt]").value = toLocalInput(later);
}

function toLocalInput(date) {
  const local = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
  return local.toISOString().slice(0, 16);
}

function eventUrl(path) {
  const base = state.config.gatewayUrl;
  return base ? base + path : path;
}

function requestHeaders(extra = {}) {
  return {
    "X-User-Id": state.config.userId,
    "X-Workspace-Id": state.config.workspaceId,
    "X-Roles": state.config.roles,
    "X-Request-Id": uid(),
    "X-Correlation-Id": uid(),
    ...extra
  };
}

function scrub(value) {
  if (typeof value === "string" && (value.includes("X-Amz-") || value.includes("X-Amz-Signature"))) return "[presigned URL redacted]";
  if (Array.isArray(value)) return value.map(scrub);
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, scrub(entry)]));
  return value;
}

async function api(path, options = {}) {
  const started = performance.now();
  const headers = requestHeaders(options.headers || {});
  if (options.body && !(options.body instanceof FormData) && !(options.body instanceof Blob)) headers["Content-Type"] = "application/json";
  const response = await fetch(eventUrl(path), { ...options, headers });
  const raw = await response.text();
  let data = raw;
  try { data = raw ? JSON.parse(raw) : null; } catch { /* keep text */ }
  const responseHeaders = Object.fromEntries(["x-request-id", "x-correlation-id", "x-processed-at", "x-eventflow-db-route", "x-eventflow-replay-lsn", "x-eventflow-commit-lsn", "x-eventflow-entity-version", "x-eventflow-write-pin-until"].map((key) => [key, response.headers.get(key)]).filter(([, value]) => value));
  state.lastResponse = { status: response.status, ms: Math.round(performance.now() - started), headers: responseHeaders, body: scrub(data) };
  renderLastResponse();
  $("#metric-request").textContent = String(response.status);
  $("#metric-request-detail").textContent = (responseHeaders["x-request-id"] || "request id unavailable").slice(0, 18);
  if (!response.ok) {
    const error = new Error(data?.message || data?.code || raw || response.statusText);
    error.status = response.status;
    error.payload = data;
    throw error;
  }
  return { data, response };
}

function renderLastResponse() {
  $("#last-response").textContent = state.lastResponse ? JSON.stringify(state.lastResponse, null, 2) : "No request yet.";
}

function toast(message, kind = "") {
  const element = document.createElement("div");
  element.className = "toast " + (kind ? "is-" + kind : "");
  element.textContent = message;
  $("#toast-stack").append(element);
  setTimeout(() => element.remove(), 4200);
}

function setBusy(id, busy) {
  const element = document.getElementById(id);
  if (!element) return;
  if (busy) {
    state.busy.add(id);
    element.dataset.label = element.textContent;
    element.textContent = "Working...";
    element.disabled = true;
  } else {
    state.busy.delete(id);
    element.textContent = element.dataset.label || element.textContent;
    element.disabled = false;
  }
}

function handleError(error) {
  const text = error.status ? error.status + " - " + error.message : "Network error - " + error.message;
  toast(text, "error");
}

function selectedId() {
  return state.selected?.id || $("#registration-event-id").value.trim() || $("#media-event-id").value.trim();
}

function syncSelectedId(id) {
  $("#registration-event-id").value = id || "";
  $("#media-event-id").value = id || "";
  $("#metric-selected").textContent = id ? id.slice(0, 8) : "-";
  $("#metric-selected-detail").textContent = id ? "Ready for RSVP and media" : "Choose one to continue";
}

function showView(name) {
  $$(".view").forEach((view) => {
    const active = view.dataset.view === name;
    view.hidden = !active;
    view.classList.toggle("is-visible", active);
  });
  $$(".nav-item").forEach((item) => item.classList.toggle("is-active", item.dataset.viewTarget === name));
  const active = $("[data-view='" + name + "']");
  $("#view-title").textContent = active?.querySelector("h2")?.textContent || "EventFlow verification console";
  $("#primary-nav").classList.remove("is-open");
  $("#menu-toggle").setAttribute("aria-expanded", "false");
}

function htmlEscape(value) {
  return String(value ?? "").replace(/[&<>"']/g, (character) => {
    if (character === "&") return "&amp;";
    if (character === "<") return "&lt;";
    if (character === ">") return "&gt;";
    if (character === '"') return "&quot;";
    return "&#039;";
  });
}

function renderEvents() {
  $("#event-count-tag").textContent = state.events.length + " loaded";
  $("#metric-events").textContent = String(state.events.length);
  const body = $("#events-table");
  if (!state.events.length) {
    body.innerHTML = '<tr><td colspan="4" class="empty-cell">No events in this workspace.</td></tr>';
    return;
  }
  body.innerHTML = state.events.map((event) => '<tr><td>' + htmlEscape(event.title) + '</td><td><span class="tag ' + (event.status === "PUBLISHED" ? "tag-green" : "tag-amber") + '">' + htmlEscape(event.status) + '</span></td><td>' + event.confirmedCount + "/" + event.capacity + '</td><td><button class="text-button select-event" type="button" data-event-id="' + event.id + '">Select</button></td></tr>').join("");
  $$(".select-event").forEach((button) => button.addEventListener("click", () => selectEvent(button.dataset.eventId)));
}

function selectEvent(id) {
  state.selected = state.events.find((event) => event.id === id) || null;
  const event = state.selected;
  syncSelectedId(event?.id);
  $("#selected-event-title").textContent = event?.title || "No event selected";
  $("#selected-event-status").textContent = event?.status || "-";
  $("#selected-event-status").className = "tag " + (event?.status === "PUBLISHED" ? "tag-green" : "tag-amber");
  $("#selected-event-meta").textContent = event ? event.id + " | " + event.startsAt + " to " + event.endsAt + " | " + event.confirmedCount + "/" + event.capacity + " seats | version " + event.version : "Select a row from the inventory.";
  $("#publish-event").disabled = !event || event.status !== "DRAFT";
  $("#cancel-event").disabled = !event || event.status === "CANCELLED";
  $("#open-media").disabled = !event;
  const form = $("#update-event-form");
  form.hidden = !event;
  if (event) {
    form.elements.title.value = event.title;
    form.elements.description.value = event.description || "";
    form.elements.startsAt.value = toLocalInput(new Date(event.startsAt));
    form.elements.endsAt.value = toLocalInput(new Date(event.endsAt));
    form.elements.timezone.value = event.timezone;
    form.elements.capacity.value = event.capacity;
    form.elements.version.value = event.version;
  }
}

async function loadEvents() {
  setBusy("load-events", true);
  try {
    const result = await api("/api/v1/events");
    state.events = result.data || [];
    renderEvents();
    if (state.selected) selectEvent(state.selected.id);
    toast("Loaded " + state.events.length + " event(s).", "good");
  } catch (error) { handleError(error); throw error; } finally { setBusy("load-events", false); }
}

async function createEvent(event) {
  setBusy("create-event-form-submit", true);
  const form = event.currentTarget;
  const data = new FormData(form);
  try {
    const result = await api("/api/v1/events", { method: "POST", body: JSON.stringify({ title: data.get("title"), description: data.get("description"), startsAt: new Date(data.get("startsAt")).toISOString(), endsAt: new Date(data.get("endsAt")).toISOString(), timezone: data.get("timezone"), capacity: Number(data.get("capacity")) }) });
    state.events.unshift(result.data);
    renderEvents();
    selectEvent(result.data.id);
    toast("Draft event created.", "good");
    showView("events");
  } catch (error) { handleError(error); } finally { setBusy("create-event-form-submit", false); }
}

async function eventAction(path, buttonId, message) {
  if (!state.selected) return toast("Select an event first.", "error");
  setBusy(buttonId, true);
  try {
    const result = await api("/api/v1/events/" + state.selected.id + path, { method: "POST" });
    state.events = state.events.map((event) => event.id === result.data.id ? result.data : event);
    renderEvents();
    selectEvent(result.data.id);
    toast(message, "good");
  } catch (error) { handleError(error); } finally { setBusy(buttonId, false); }
}

async function updateEvent(event) {
  event.preventDefault();
  if (!state.selected) return;
  const data = new FormData(event.currentTarget);
  setBusy("update-event-form-submit", true);
  try {
    const result = await api("/api/v1/events/" + state.selected.id, { method: "PATCH", body: JSON.stringify({ title: data.get("title"), description: data.get("description"), startsAt: new Date(data.get("startsAt")).toISOString(), endsAt: new Date(data.get("endsAt")).toISOString(), timezone: data.get("timezone"), capacity: Number(data.get("capacity")), version: Number(data.get("version")) }) });
    state.events = state.events.map((item) => item.id === result.data.id ? result.data : item);
    renderEvents();
    selectEvent(result.data.id);
    toast("Versioned update saved.", "good");
  } catch (error) { handleError(error); } finally { setBusy("update-event-form-submit", false); }
}

async function registrationAction(kind) {
  const id = selectedId();
  if (!id) return toast("Select or paste an event ID.", "error");
  const buttonId = kind === "register" ? "register-button" : kind === "mine" ? "mine-button" : "cancel-registration";
  setBusy(buttonId, true);
  try {
    const options = { method: kind === "register" ? "POST" : kind === "cancel" ? "DELETE" : "GET" };
    if (kind === "register") options.headers = { "Idempotency-Key": $("#idempotency-key").value.trim() || uid() };
    const suffix = kind === "register" ? "/registrations" : "/registrations/me";
    const result = await api("/api/v1/events/" + id + suffix, options);
    $("#registration-result").textContent = JSON.stringify(scrub(result.data), null, 2);
    toast("Registration response received.", "good");
  } catch (error) { $("#registration-result").textContent = JSON.stringify(scrub(error.payload || { message: error.message, status: error.status }), null, 2); handleError(error); } finally { setBusy(buttonId, false); }
}

async function checkIn() {
  const id = selectedId();
  if (!id) return toast("Select or paste an event ID.", "error");
  setBusy("checkin-button", true);
  try {
    const result = await api("/api/v1/events/" + id + "/check-ins", { method: "POST", body: JSON.stringify({ participantId: $("#participant-id").value.trim() }) });
    $("#attendance-result").textContent = JSON.stringify(scrub(result.data), null, 2);
    toast("Check-in response received.", "good");
  } catch (error) { $("#attendance-result").textContent = JSON.stringify(scrub(error.payload || { message: error.message }), null, 2); handleError(error); } finally { setBusy("checkin-button", false); }
}

async function attendance() {
  const id = selectedId();
  if (!id) return toast("Select or paste an event ID.", "error");
  setBusy("attendance-button", true);
  try {
    const result = await api("/api/v1/events/" + id + "/attendance");
    $("#attendance-result").textContent = JSON.stringify(scrub(result.data), null, 2);
  } catch (error) { handleError(error); } finally { setBusy("attendance-button", false); }
}

function uploadUrl(signed) {
  return location.origin.startsWith("http") ? "/__s3?url=" + encodeURIComponent(signed) : signed;
}

async function uploadBlob(signed, blob, contentType, progress, step) {
  if (step !== undefined) markMediaStep(step);
  const response = await fetch(uploadUrl(signed), { method: "PUT", headers: { "Content-Type": contentType }, body: blob });
  if (!response.ok) throw new Error("S3 upload returned " + response.status);
  if (progress) progress(100);
  return response.headers.get("etag") || response.headers.get("ETag");
}

function markMediaStep(index) {
  $$("#media-steps li").forEach((item, itemIndex) => {
    item.classList.toggle("is-done", itemIndex < index);
    item.classList.toggle("is-current", itemIndex === index);
  });
}

async function createMediaSession(multipart) {
  const id = selectedId();
  const file = $("#media-file").files[0];
  if (!id || !file) throw new Error("Select an event and a file first.");
  const path = multipart ? "/media/multipart-upload-session" : "/media/upload-session";
  const result = await api("/api/v1/events/" + id + path, { method: "POST", body: JSON.stringify({ fileName: file.name, contentType: file.type, size: file.size }) });
  return { id, file, data: result.data };
}

async function singleUpload() {
  setBusy("single-upload", true);
  $("#media-progress").style.width = "10%";
  try {
    const session = await createMediaSession(false);
    $("#media-progress").style.width = "40%";
    await uploadBlob(session.data.uploadUrl, session.file, session.file.type, (value) => { $("#media-progress").style.width = value + "%"; });
    $("#media-progress").style.width = "75%";
    const result = await api("/api/v1/events/" + session.id + "/media/" + session.data.mediaId + "/finalize", { method: "POST" });
    $("#media-result").textContent = JSON.stringify(scrub(result.data), null, 2);
    toast(result.data.state === "READY" ? "Private object finalized." : "Object rejected during finalize.", result.data.state === "READY" ? "good" : "error");
    $("#media-progress").style.width = "100%";
  } catch (error) { $("#media-progress").style.width = "0"; $("#media-result").textContent = JSON.stringify(scrub(error.payload || { message: error.message }), null, 2); handleError(error); } finally { setBusy("single-upload", false); }
}

async function multipartUpload() {
  setBusy("multipart-upload", true);
  try {
    const session = await createMediaSession(true);
    markMediaStep(1);
    const parts = [];
    for (const part of session.data.parts) {
      const start = (part.partNumber - 1) * session.data.partSize;
      const etag = await uploadBlob(part.uploadUrl, session.file.slice(start, Math.min(start + session.data.partSize, session.file.size)), session.file.type, null, 2);
      if (!etag) throw new Error("S3 did not return an ETag for part " + part.partNumber);
      parts.push({ partNumber: part.partNumber, etag });
    }
    markMediaStep(3);
    const result = await api("/api/v1/events/" + session.id + "/media/" + session.data.mediaId + "/multipart-complete", { method: "POST", body: JSON.stringify({ parts }) });
    $("#media-result").textContent = JSON.stringify(scrub(result.data), null, 2);
    $$("#media-steps li").forEach((item) => item.classList.add("is-done"));
    toast("Multipart object completed.", "good");
  } catch (error) { $("#media-result").textContent = JSON.stringify(scrub(error.payload || { message: error.message }), null, 2); handleError(error); } finally { setBusy("multipart-upload", false); }
}

async function downloadMedia() {
  const id = selectedId();
  const mediaId = prompt("Paste the mediaId returned by the upload response:");
  if (!id || !mediaId) return;
  setBusy("download-media", true);
  try {
    const result = await api("/api/v1/events/" + id + "/media/" + mediaId + "/download-url");
    window.open(result.data.downloadUrl, "_blank", "noopener,noreferrer");
    toast("Signed download URL opened.", "good");
  } catch (error) { handleError(error); } finally { setBusy("download-media", false); }
}

async function weather() {
  setBusy("weather-button", true);
  try {
    const result = await api("/api/v1/network/weather");
    const data = result.data;
    $("#weather-location").textContent = [data.city, data.region, data.country].filter(Boolean).join(", ") || "Location resolved";
    const current = data.current || {};
    const units = data.currentUnits || {};
    $("#weather-summary").textContent = (current.temperature_2m ?? "-") + " " + (units.temperature_2m || "C") + " | humidity " + (current.relative_humidity_2m ?? "-") + "% | wind " + (current.wind_speed_10m ?? "-") + " " + (units.wind_speed_10m || "km/h");
    toast("Weather response received.", "good");
  } catch (error) { $("#weather-summary").textContent = error.message; handleError(error); } finally { setBusy("weather-button", false); }
}

async function healthSweep() {
  setBusy("overview-refresh", true);
  try {
    await loadEvents();
    $("#metric-gateway").textContent = "OK";
    $("#metric-gateway-detail").textContent = "API response received";
    $("#connection-status").className = "status-chip is-good";
    $("#connection-status").innerHTML = "<i></i> Connected";
  } catch {
    $("#metric-gateway").textContent = "DOWN";
    $("#metric-gateway-detail").textContent = "Check Gateway / lab";
    $("#connection-status").className = "status-chip is-bad";
    $("#connection-status").innerHTML = "<i></i> Unavailable";
  } finally { setBusy("overview-refresh", false); }
}

document.addEventListener("DOMContentLoaded", async () => {
  await loadRuntimeConfig();
  initConfig();
  $$(".nav-item").forEach((item) => item.addEventListener("click", () => showView(item.dataset.viewTarget)));
  $$("[data-jump]").forEach((item) => item.addEventListener("click", () => showView(item.dataset.jump)));
  $("#menu-toggle").addEventListener("click", () => {
    const nav = $("#primary-nav");
    const open = nav.classList.toggle("is-open");
    $("#menu-toggle").setAttribute("aria-expanded", String(open));
  });
  $("#save-config").addEventListener("click", saveConfig);
  $("#refresh-all").addEventListener("click", healthSweep);
  $("#overview-refresh").addEventListener("click", healthSweep);
  $("#load-events").addEventListener("click", loadEvents);
  $("#create-event-form").addEventListener("submit", (event) => { event.preventDefault(); createEvent(event); });
  $("#update-event-form").addEventListener("submit", updateEvent);
  $("#publish-event").addEventListener("click", () => eventAction("/publish", "publish-event", "Event published."));
  $("#cancel-event").addEventListener("click", () => eventAction("/cancel", "cancel-event", "Event cancelled."));
  $("#open-media").addEventListener("click", () => showView("media"));
  $("#register-button").addEventListener("click", () => registrationAction("register"));
  $("#mine-button").addEventListener("click", () => registrationAction("mine"));
  $("#cancel-registration").addEventListener("click", () => registrationAction("cancel"));
  $("#refresh-registration").addEventListener("click", () => registrationAction("mine"));
  $("#checkin-button").addEventListener("click", checkIn);
  $("#attendance-button").addEventListener("click", attendance);
  $("#single-upload").addEventListener("click", singleUpload);
  $("#multipart-upload").addEventListener("click", multipartUpload);
  $("#download-media").addEventListener("click", downloadMedia);
  $("#weather-button").addEventListener("click", weather);
  $("#copy-last").addEventListener("click", async () => { await navigator.clipboard?.writeText($("#last-response").textContent); toast("Response JSON copied.", "good"); });
  $("#media-file").addEventListener("change", (event) => { $("#media-result").textContent = event.target.files[0] ? event.target.files[0].name + " selected." : "No media action yet."; });
  renderEvents();
});
