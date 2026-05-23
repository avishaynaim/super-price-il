// Live scanner — WebSocket OCR mode.
//
// Architecture:
//   1. Camera feed → canvas frame capture every ~2s
//   2. Frame sent as JPEG base64 over WebSocket to /ws/live-ocr
//   3. Server runs RapidOCR, extracts printed number codes (729xxxxxxxxxx)
//   4. Server replies with matched products + prices from local DB
//
// This handles printed number codes on receipts/labels, not just stripe barcodes.

import { shekel, escapeHtml, toast } from "./ui.js";
import { openProduct } from "./product.js";
import { addToBasket } from "./receipt.js";

const DEBOUNCE_MS       = 8000;
const CAPTURE_MS_RAPID  = 2000;   // RapidOCR ~1-2s — auto scan
const $ = id => document.getElementById(id);

// ── debug logger ──────────────────────────────────────────────────────────────
const _dbgLines = [];
function dbg(...args) {
  const msg = args.map(a => typeof a === "object" ? JSON.stringify(a) : String(a)).join(" ");
  console.log("[live]", msg);
  _dbgLines.push(msg);
  if (_dbgLines.length > 40) _dbgLines.shift();
  const el = $("live-debug");
  if (el) { el.textContent = _dbgLines.join("\n"); el.scrollTop = el.scrollHeight; }
}

let stream       = null;
let ws           = null;
let captureTimer = null;
const items      = new Map();   // barcode → item record
const recent     = new Map();   // barcode → last-seen timestamp

// ── status ────────────────────────────────────────────────────────────────────
function setStatus(text, kind = "") {
  const el = $("live-status");
  if (el) { el.textContent = text; el.className = "live-status " + kind; }
}

// ── capture one frame and send to server OCR ──────────────────────────────────
function captureFrame() {
  const v = $("live-video");
  if (!v?.videoWidth) { dbg("capture: video not ready"); return; }
  if (!ws || ws.readyState !== WebSocket.OPEN) { dbg("capture: WS not open, state=", ws?.readyState); return; }

  const mode  = $("live-ocr-mode")?.value || "rapid";
  // Claude gets full resolution for best recall; RapidOCR downscaled for speed
  const maxPx = mode === "claude" ? 1920 : 1000;
  const scale = Math.min(1, maxPx / Math.max(v.videoWidth, v.videoHeight));
  const w = Math.round(v.videoWidth  * scale);
  const h = Math.round(v.videoHeight * scale);

  const cvs = Object.assign(document.createElement("canvas"), { width: w, height: h });
  cvs.getContext("2d").drawImage(v, 0, 0, w, h);
  const b64 = cvs.toDataURL("image/jpeg", mode === "claude" ? 0.92 : 0.85);
  const id  = Date.now().toString();

  dbg(`→ frame ${id} ${w}x${h} (${Math.round(b64.length/1024)}KB) mode=${mode}`);
  ws.send(JSON.stringify({ type: "frame", image: b64, id, mode }));
}

// ── handle server OCR result ──────────────────────────────────────────────────
function onWsMessage(ev) {
  let msg;
  try { msg = JSON.parse(ev.data); } catch { return; }

  if (msg.type === "error") {
    dbg("WS error from server:", msg.error);
    _reenableScanBtn();
    return;
  }
  if (msg.type === "reset_ack") { dbg("WS reset ack"); return; }
  if (msg.type === "processing") {
    dbg(`Claude מעבד… ${msg.elapsed_s}s`);
    setStatus(`🧠 Claude מעבד… ${msg.elapsed_s} שניות`, "ok");
    const btn = $("live-scan-now");
    if (btn && !btn.hidden) btn.textContent = `⏳ ${msg.elapsed_s}s…`;
    return;
  }
  if (msg.type !== "result") return;

  dbg(`← result id=${msg.id} latency=${msg.latency_ms}ms lines=${msg.lines?.length} new_items=${msg.items?.length}`);

  const now = Date.now();
  let gotNew = false;

  for (const it of msg.items || []) {
    const bc = it.barcode;
    if (!bc) continue;
    if (recent.has(bc) && now - recent.get(bc) < DEBOUNCE_MS) {
      dbg("SKIP debounce:", bc); continue;
    }
    recent.set(bc, now);
    if (items.has(bc)) { dbg("SKIP already shown:", bc); continue; }

    dbg("NEW item:", bc, it.name, it.min_price);
    items.set(bc, {
      barcode:        bc,
      name:           it.name || bc,
      manufacturer:   it.manufacturer,
      min_price:      it.min_price,
      max_price:      it.max_price,
      cheapest_chain: it.cheapest_chain,
      priciest_chain: it.priciest_chain,
      chains:         it.all_chains || [],
      matched:        it.matched,
    });
    gotNew = true;

    if (it.matched) {
      const saving = (it.max_price && it.min_price) ? it.max_price - it.min_price : 0;
      toast(
        `✓ ${it.name || bc}  ${shekel(it.min_price)} @ ${it.cheapest_chain}` +
        (saving > 0.5 ? `  (חסוך ₪${saving.toFixed(2)})` : ""),
        "ok", 4000
      );
    } else {
      toast(`קוד ${bc} — לא נמצא במאגר`, "warn", 2500);
    }
  }

  if (gotNew) { renderItems(); updateTotals(); }
  _reenableScanBtn();

  const latMs = msg.latency_ms;
  const found = items.size;
  setStatus(found
    ? `✓ נמצאו ${found} מוצרים (${(latMs/1000).toFixed(1)}s)`
    : `סריקה הושלמה — לא נמצאו קודים (${(latMs/1000).toFixed(1)}s)`, found ? "ok" : "");
}

// ── render ─────────────────────────────────────────────────────────────────────
function renderItems() {
  const root = $("live-items");
  if (!items.size) {
    root.innerHTML = '<div class="empty">הצב את קוד המוצר בתוך המסגרת</div>';
    return;
  }
  root.innerHTML = [...items.values()].reverse().map(it => {
    if (!it.matched) {
      return `<div class="live-item live-item-miss">
        <div class="li-main"><div class="li-name">${escapeHtml(it.barcode)}</div>
        <div class="li-sub">לא נמצא במאגר</div></div></div>`;
    }
    const saving = (it.max_price && it.min_price) ? it.max_price - it.min_price : 0;
    const savingHtml = saving > 0.5
      ? `<span class="savings-pos">חסוך ₪${saving.toFixed(2)}</span>` : "";
    const itemData = JSON.stringify({
      barcode: it.barcode, name: it.name, price: it.min_price, chain: it.cheapest_chain
    }).replace(/"/g, "&quot;");

    return `
      <div class="live-item live-item-ok live-item-click" data-barcode="${escapeHtml(it.barcode)}"
           role="button" tabindex="0">
        <div class="li-main">
          <div class="li-name">${escapeHtml(it.name)}</div>
          <div class="li-sub">${escapeHtml(it.manufacturer || "")}
            <span class="li-bc">${escapeHtml(it.barcode)}</span></div>
          <div class="li-prices">
            <span class="li-best">
              <b>${shekel(it.min_price)}</b>
              <span class="chain-tag">${escapeHtml(it.cheapest_chain || "")}</span>
            </span>
            ${it.max_price !== it.min_price ? `
            <span class="li-worst">
              ${shekel(it.max_price)}
              <span class="chain-tag chain-tag-hi">${escapeHtml(it.priciest_chain || "")}</span>
            </span>` : ""}
            ${savingHtml}
          </div>
        </div>
        <div class="li-actions">
          <div class="li-price">${shekel(it.min_price)} <span class="li-arrow">›</span></div>
          <button class="btn-add-item li-add" data-item="${itemData}">+ סל</button>
        </div>
      </div>`;
  }).join("");

  root.querySelectorAll(".live-item-click").forEach(el => {
    el.addEventListener("click", e => {
      if (e.target.closest(".li-add")) return;
      openProduct(el.dataset.barcode);
    });
  });
  root.querySelectorAll(".li-add").forEach(btn => {
    btn.addEventListener("click", e => {
      e.stopPropagation();
      try { addToBasket(JSON.parse(btn.dataset.item)); } catch {}
    });
  });
}

function updateTotals() {
  const matched = [...items.values()].filter(it => it.matched);
  const total    = matched.reduce((s, it) => s + (it.min_price || 0), 0);
  const maxTotal = matched.reduce((s, it) => s + (it.max_price || it.min_price || 0), 0);
  const saving   = maxTotal - total;
  $("live-totals").innerHTML =
    `מוצרים: <b>${matched.length}</b> · סל מינימום: <b>${shekel(total)}</b>` +
    (saving > 1 ? ` · <span class="savings-pos">חיסכון אפשרי: ${shekel(saving)}</span>` : "");
}

// ── start / stop / reset ──────────────────────────────────────────────────────
async function start() {
  if (stream) return;
  setStatus("מפעיל מצלמה…", "");
  dbg("start()");

  if (!navigator.mediaDevices?.getUserMedia) {
    const isSecure = location.protocol === "https:" || location.hostname === "localhost" || location.hostname === "127.0.0.1";
    const msg = isSecure
      ? "המצלמה אינה נתמכת בדפדפן זה"
      : "יש לפתוח את האפליקציה דרך localhost:8000 (לא דרך כתובת IP) כדי לאפשר גישה למצלמה";
    dbg("mediaDevices unavailable. secure=", isSecure, "protocol=", location.protocol, "host=", location.hostname);
    setStatus(msg, "err");
    return;
  }

  try {
    stream = await navigator.mediaDevices.getUserMedia({
      audio: false,
      video: { facingMode: { ideal: "environment" }, width: { ideal: 1920 }, height: { ideal: 1080 } },
    });
    const track = stream.getVideoTracks()[0];
    const s = track.getSettings();
    dbg(`Camera: ${s.width}x${s.height} facing=${s.facingMode} label=${track.label}`);
  } catch (e) {
    dbg("Camera error:", e.name, e.message);
    setStatus("שגיאת מצלמה: " + (e.message || e.name), "err");
    return;
  }

  const v = $("live-video");
  v.srcObject = stream;
  await v.play().catch(e => dbg("play() error:", e.message));
  dbg(`Video playing: ${v.videoWidth}x${v.videoHeight}`);

  // Connect WebSocket to server OCR
  const wsProto = location.protocol === "https:" ? "wss:" : "ws:";
  const wsUrl   = `${wsProto}//${location.host}/api/ws/live-ocr`;
  dbg("Connecting WS:", wsUrl);
  ws = new WebSocket(wsUrl);
  ws.onopen    = () => { dbg("WS open ✓"); setStatus("מחובר — כוון את קוד המוצר למסגרת", "ok"); };
  ws.onerror   = (e) => { dbg("WS error:", JSON.stringify(e)); setStatus("שגיאת חיבור OCR", "err"); };
  ws.onclose   = (e) => { dbg("WS closed code=", e.code, e.reason); };
  ws.onmessage = onWsMessage;

  $("live-start").hidden    = true;
  $("live-stop").hidden     = false;
  $("live-reset").hidden    = false;
  $("live-ocr-mode").hidden = false;
  $("live-summary").hidden  = true;
  $("live-hint")?.classList.add("hidden");
  $("live-target")?.classList.add("active");
  $("live-scanline")?.classList.add("active");

  const applyMode = (mode) => {
    if (captureTimer) { clearInterval(captureTimer); captureTimer = null; }
    if (mode === "claude") {
      // Manual mode — user presses "סרוק עכשיו"
      $("live-scan-now").hidden = false;
      setStatus("כוון את הקבלה למסגרת ולחץ סרוק עכשיו", "ok");
    } else {
      // Auto mode — scan every 2s
      $("live-scan-now").hidden = true;
      captureTimer = setInterval(captureFrame, CAPTURE_MS_RAPID);
      setStatus("סורק אוטומטית…", "ok");
    }
    dbg("mode applied:", mode);
  };

  applyMode($("live-ocr-mode")?.value || "rapid");

  $("live-ocr-mode")?.addEventListener("change", () => {
    const mode = $("live-ocr-mode")?.value;
    dbg("Mode changed to:", mode);
    if (stream) applyMode(mode);
  });
}

function scanNow() {
  const btn = $("live-scan-now");
  if (btn.disabled) return;
  btn.disabled = true;
  btn.textContent = "⏳ מעבד…";
  setStatus("שולח לסריקה…", "ok");
  captureFrame();
}

function _reenableScanBtn() {
  const btn = $("live-scan-now");
  if (!btn || btn.hidden) return;
  btn.disabled = false;
  btn.textContent = "📸 סרוק עכשיו";
}

function stop() {
  if (captureTimer) { clearInterval(captureTimer); captureTimer = null; }
  if (ws)           { ws.close(); ws = null; }
  if (stream)       { stream.getTracks().forEach(t => t.stop()); stream = null; }
  const v = $("live-video");
  if (v) v.srcObject = null;
  $("live-start").hidden    = false;
  $("live-stop").hidden     = true;
  $("live-reset").hidden    = true;
  $("live-scan-now").hidden = true;
  $("live-ocr-mode").hidden = true;
  $("live-hint")?.classList.remove("hidden");
  $("live-target")?.classList.remove("active");
  $("live-scanline")?.classList.remove("active");
  showSummary();
  setStatus("", "");
  dbg("stopped");
}

function showSummary() {
  const el = $("live-summary");
  if (!el) return;
  const matched = [...items.values()].filter(it => it.matched);
  if (!matched.length) { el.hidden = true; return; }
  const total    = matched.reduce((s, it) => s + (it.min_price || 0), 0);
  const maxTotal = matched.reduce((s, it) => s + (it.max_price || it.min_price || 0), 0);
  const saving   = maxTotal - total;
  el.hidden = false;
  el.innerHTML = `
    <div class="summary-box">
      <div class="summary-title">✓ סריקה הושלמה</div>
      <div class="summary-row"><span>מוצרים שזוהו</span><b>${matched.length}</b></div>
      <div class="summary-row"><span>סל מינימום</span><b>${shekel(total)}</b></div>
      ${saving > 1 ? `<div class="summary-row savings-pos"><span>חיסכון אפשרי</span><b>${shekel(saving)}</b></div>` : ""}
    </div>`;
}

function reset() {
  items.clear(); recent.clear(); renderItems(); updateTotals();
  if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: "reset" }));
  dbg("reset");
}

// ── product text search ───────────────────────────────────────────────────────
function initLiveSearch() {
  const form    = $("live-search-form");
  const input   = $("live-search-input");
  const results = $("live-search-results");
  if (!form) return;

  const { api } = { api: (url, params) => fetch(url + (params ? "?" + new URLSearchParams(params) : "")).then(r => r.json()) };

  form.addEventListener("submit", async e => {
    e.preventDefault();
    const q = (input.value || "").trim();
    if (!q) return;
    results.hidden = false;
    results.innerHTML = '<div class="ls-row ls-loading">מחפש…</div>';
    try {
      const rows = await fetch(`/api/search?q=${encodeURIComponent(q)}&limit=8`).then(r => r.json());
      if (!rows.length) { results.innerHTML = '<div class="ls-row ls-empty">לא נמצאו מוצרים</div>'; return; }
      results.innerHTML = rows.map(r => `
        <div class="ls-row" data-barcode="${escapeHtml(r.barcode)}" role="option" tabindex="0">
          <div class="ls-name">${escapeHtml(r.name || r.barcode)}</div>
          <div class="ls-meta">${shekel(r.min_price)} – ${shekel(r.max_price)}
            <span class="ls-chains">${r.chains_with_price} רשתות</span></div>
        </div>`).join("");
      results.querySelectorAll(".ls-row[data-barcode]").forEach(el => {
        el.addEventListener("click", () => { results.hidden = true; input.value = ""; openProduct(el.dataset.barcode); });
      });
    } catch (err) {
      results.innerHTML = `<div class="ls-row ls-empty">שגיאה</div>`;
    }
  });
  document.addEventListener("click", e => {
    if (!form.contains(e.target) && !results.contains(e.target)) results.hidden = true;
  });
}

// ── public init ───────────────────────────────────────────────────────────────
export function initLive() {
  if (!$("live-start")) return;
  $("live-start").addEventListener("click", start);
  $("live-stop").addEventListener("click", stop);
  $("live-reset").addEventListener("click", reset);
  $("live-scan-now").addEventListener("click", scanNow);
  initLiveSearch();
  renderItems();
  document.addEventListener("visibilitychange", () => { if (document.hidden && stream) stop(); });
  window.addEventListener("hashchange", () => {
    const tab = (location.hash || "").replace(/^#\/?/, "").split("/")[0];
    if (tab === "live") { if (!stream) start(); }
    else if (stream) stop();
  });
  if ((location.hash || "").replace(/^#\/?/, "").split("/")[0] === "live") start();
}
