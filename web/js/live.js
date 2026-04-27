// Live OCR: open the rear camera, stream frames over a WebSocket to /api/ws/live-ocr,
// draw bounding boxes over the live <video> with kind-colored highlights, and tick
// a side list of detected items as new barcodes are matched against the catalog.
//
// Throttling: capture at most ~1 frame/sec (server may take 0.5-2s/frame on Termux),
// drop overlapping captures, and reuse the same JPEG-encoded canvas to keep memory flat.

import { shekel, escapeHtml, toast } from "./ui.js";

const FRAME_INTERVAL_MS = 1100;
const JPEG_QUALITY      = 0.7;
const CAPTURE_LONG_SIDE = 960;     // server downscales further to 1600 max anyway
const BOX_FADE_MS       = 2400;
const BOX_KEEP_KINDS    = new Set(["barcode"]);  // boxes that persist instead of fade
const FRAME_WATCHDOG_MS = 25000;   // if a frame doesn't return in this window, reset

const $ = id => document.getElementById(id);

let stream      = null;
let ws          = null;
let frameTimer  = null;
let frameSeq    = 0;
let inFlight    = false;
let inFlightSince = 0;
let framesSent  = 0;
let framesReceived = 0;
let lastVideoSize = { w: 0, h: 0 };
const items    = new Map();        // barcode → item record
const liveBoxes = [];              // { id, box, kind, text, expires, el }

function setStatus(text, kind = "") {
  const el = $("live-status");
  if (!el) return;
  el.textContent = text;
  el.className = "live-status " + kind;
}

function svgRoot() {
  return $("live-overlay");
}

function videoEl() {
  return $("live-video");
}

function ensureSvgViewbox() {
  const v = videoEl();
  const w = v.videoWidth || 1280;
  const h = v.videoHeight || 720;
  const svg = svgRoot();
  if (lastVideoSize.w !== w || lastVideoSize.h !== h) {
    svg.setAttribute("viewBox", `0 0 ${w} ${h}`);
    svg.setAttribute("preserveAspectRatio", "none");
    lastVideoSize = { w, h };
  }
}

function captureFrameAsBlob() {
  const v = videoEl();
  if (!v.videoWidth) return null;
  const long = Math.max(v.videoWidth, v.videoHeight);
  const scale = long > CAPTURE_LONG_SIDE ? CAPTURE_LONG_SIDE / long : 1;
  const w = Math.round(v.videoWidth * scale);
  const h = Math.round(v.videoHeight * scale);
  const cvs = document.createElement("canvas");
  cvs.width = w; cvs.height = h;
  cvs.getContext("2d").drawImage(v, 0, 0, w, h);
  return new Promise(resolve => cvs.toBlob(resolve, "image/jpeg", JPEG_QUALITY));
}

function blobToBase64(blob) {
  return new Promise((res, rej) => {
    const r = new FileReader();
    r.onload  = () => res(r.result.toString().split(",", 2)[1]);
    r.onerror = () => rej(r.error);
    r.readAsDataURL(blob);
  });
}

// ---------- WebSocket ----------

function openWs() {
  return new Promise((resolve, reject) => {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const w = new WebSocket(`${proto}//${location.host}/api/ws/live-ocr`);
    w.onopen = () => resolve(w);
    w.onerror = e => reject(e);
    w.onclose = () => {
      setStatus("WebSocket נסגר", "warn");
    };
    w.onmessage = ev => {
      let m;
      try { m = JSON.parse(ev.data); } catch { return; }
      if (m.type === "result") onResult(m);
      else if (m.type === "error") setStatus(`שגיאה: ${m.error}`, "err");
      else if (m.type === "reset_ack") {
        items.clear(); renderItems(); setStatus("אופס — מתחילים מחדש", "ok");
      }
    };
  });
}

// ---------- frame loop ----------

async function tick() {
  // Watchdog: if a frame hasn't returned in FRAME_WATCHDOG_MS, free the slot.
  // Server-side OCR sometimes stalls (long PIL decode); without this the UI
  // would freeze on "מצולם בזמן אמת" indefinitely.
  if (inFlight && performance.now() - inFlightSince > FRAME_WATCHDOG_MS) {
    setStatus("פריים אחרון לא חזר — שולח מחדש", "warn");
    inFlight = false;
  }
  if (inFlight) return;
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    setStatus("WebSocket לא פתוח", "warn");
    return;
  }
  let blob = null;
  try {
    blob = await captureFrameAsBlob();
  } catch (e) {
    console.error("capture error", e);
    setStatus("שגיאה בלכידת פריים: " + (e.message || e), "err");
    return;
  }
  if (!blob) {
    // Video not ready yet — try again on the next tick. Crucially, do NOT
    // toggle inFlight, otherwise we'd lock ourselves out forever.
    setStatus("ממתין לוידאו…", "");
    return;
  }
  inFlight = true;
  inFlightSince = performance.now();
  try {
    const b64 = await blobToBase64(blob);
    const id  = "f" + (++frameSeq);
    framesSent++;
    ws.send(JSON.stringify({ type: "frame", id, image: b64 }));
    setStatus(`שולח פריים #${framesSent} · ${(blob.size/1024)|0} KB · מעבד…`, "");
  } catch (e) {
    console.error("send error", e);
    inFlight = false;
    setStatus("שגיאה בשליחה: " + (e.message || e), "err");
  }
}

// ---------- overlay ----------

function kindClass(kind) {
  return "lb lb-" + kind;
}

function renderBoxes(lines) {
  const svg = svgRoot();
  ensureSvgViewbox();
  const w = lastVideoSize.w || 1, h = lastVideoSize.h || 1;
  const now = performance.now();
  // 1) age out the existing boxes
  for (let i = liveBoxes.length - 1; i >= 0; i--) {
    const b = liveBoxes[i];
    if (b.expires && now > b.expires) {
      b.el.remove();
      liveBoxes.splice(i, 1);
    }
  }
  // 2) add new boxes
  for (const ln of lines) {
    const [x, y, bw, bh] = ln.box;
    const px = x * w, py = y * h, pw = bw * w, ph = bh * h;
    const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
    g.setAttribute("class", kindClass(ln.kind));
    const r = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    r.setAttribute("x", px.toFixed(1));
    r.setAttribute("y", py.toFixed(1));
    r.setAttribute("width",  Math.max(2, pw).toFixed(1));
    r.setAttribute("height", Math.max(2, ph).toFixed(1));
    r.setAttribute("rx", "3");
    g.appendChild(r);
    if (ln.kind !== "text") {
      const t = document.createElementNS("http://www.w3.org/2000/svg", "text");
      t.setAttribute("x", (px + pw + 4).toFixed(1));
      t.setAttribute("y", (py + ph * 0.85).toFixed(1));
      t.setAttribute("class", "lb-label");
      t.textContent = ln.text;
      g.appendChild(t);
    }
    svg.appendChild(g);
    liveBoxes.push({
      id: ln.text + ":" + px.toFixed(0) + "," + py.toFixed(0),
      box: ln.box,
      kind: ln.kind,
      text: ln.text,
      expires: BOX_KEEP_KINDS.has(ln.kind) ? 0 : now + BOX_FADE_MS,
      el: g,
    });
  }
  // cap total to avoid DOM blowup
  while (liveBoxes.length > 80) {
    liveBoxes.shift().el.remove();
  }
}

// ---------- side list ----------

function onResult(m) {
  inFlight = false;
  framesReceived++;
  const nLines = (m.lines || []).length;
  const byKind = (m.lines || []).reduce((acc, l) => {
    acc[l.kind] = (acc[l.kind] || 0) + 1; return acc;
  }, {});
  const bcCount = byKind.barcode || 0;
  const newBc = (m.items || []).length;
  const tot   = m.totals || {};

  // Clear, actionable status. When 0 lines, hint at common causes.
  let status = "ok";
  let txt = `פריים #${framesReceived} · ${m.latency_ms} ms · ${nLines} שורות`;
  if (bcCount) txt += ` · ברקודים: ${bcCount}`;
  if (newBc)   txt += ` · חדשים: ${newBc}`;
  if (nLines === 0) {
    status = "warn";
    txt += " · אין טקסט בפריים — קרב את המצלמה לקבלה ושמור על תאורה";
  } else if (bcCount === 0) {
    status = "warn";
    txt += " · אין ברקודים — מקד על השורות התחתונות של הקבלה";
  }
  setStatus(txt, status);
  renderBoxes(m.lines || []);
  for (const it of m.items || []) {
    items.set(it.barcode, it);
  }
  renderItems();
  $("live-totals").innerHTML =
    `מוצרים מזוהים: <b>${tot.distinct_items ?? 0}</b> · ` +
    `סל מוערך: <b>${shekel(tot.basket_total)}</b> · ` +
    `<span class="muted">פריימים נשלחו ${framesSent} · נקלטו ${framesReceived}</span>`;
}

function renderItems() {
  const root = $("live-items");
  if (!items.size) {
    root.innerHTML = '<div class="empty">עדיין לא זוהו מוצרים — כוון את המצלמה לקבלה</div>';
    return;
  }
  const rows = [...items.values()].reverse();
  root.innerHTML = rows.map(it => {
    const matched = it.matched;
    const cls = matched ? "live-item live-item-ok" : "live-item live-item-miss";
    const name = matched ? escapeHtml(it.name || "") : "ברקוד לא ידוע";
    const sub  = matched
      ? escapeHtml(it.manufacturer || "")
      : "המוצר לא נמצא בקטלוג";
    return `
      <div class="${cls}">
        <div class="li-main">
          <div class="li-name">${name}</div>
          <div class="li-sub">${sub} <span class="li-bc">${escapeHtml(it.barcode)}</span></div>
        </div>
        <div class="li-price">${matched && it.min_price ? shekel(it.min_price) : ""}</div>
      </div>
    `;
  }).join("");
}

// ---------- start/stop ----------

async function start() {
  if (stream) return;
  setStatus("מבקש גישה למצלמה…", "");
  try {
    stream = await navigator.mediaDevices.getUserMedia({
      audio: false,
      video: {
        facingMode: { ideal: "environment" },
        width:  { ideal: 1280 },
        height: { ideal: 720 },
      },
    });
  } catch (e) {
    setStatus("גישה למצלמה נדחתה: " + (e.message || e.name), "err");
    return;
  }
  const v = videoEl();
  v.srcObject = stream;
  await v.play().catch(() => {});
  setStatus("מתחבר לשרת…", "");
  try {
    ws = await openWs();
  } catch (e) {
    setStatus("WebSocket נכשל", "err");
    return;
  }
  setStatus("מצולם בזמן אמת — סרוק קבלה", "ok");
  $("live-start").hidden = true;
  $("live-stop").hidden = false;
  $("live-reset").hidden = false;
  framesSent = 0;
  framesReceived = 0;
  inFlight = false;
  // Fire first tick almost immediately so the user sees activity right away,
  // then settle into the steady cadence.
  setTimeout(tick, 250);
  frameTimer = setInterval(tick, FRAME_INTERVAL_MS);
}

function stop() {
  if (frameTimer) { clearInterval(frameTimer); frameTimer = null; }
  if (ws && ws.readyState === WebSocket.OPEN) ws.close();
  ws = null;
  if (stream) {
    for (const t of stream.getTracks()) t.stop();
    stream = null;
  }
  videoEl().srcObject = null;
  // clear overlay boxes
  while (liveBoxes.length) liveBoxes.shift().el.remove();
  $("live-start").hidden = false;
  $("live-stop").hidden = true;
  setStatus("הופסק", "");
}

function reset() {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: "reset" }));
  }
  items.clear();
  framesSent = 0;
  framesReceived = 0;
  renderItems();
  while (liveBoxes.length) liveBoxes.shift().el.remove();
  $("live-totals").textContent = "";
}

// ---------- public init ----------

export function initLive() {
  if (!$("live-start")) return;   // tab not present
  $("live-start").addEventListener("click", start);
  $("live-stop").addEventListener("click", stop);
  $("live-reset").addEventListener("click", reset);
  renderItems();
  // Stop the camera + WS when leaving the tab to save battery and CPU.
  document.addEventListener("visibilitychange", () => {
    if (document.hidden && stream) stop();
  });
  window.addEventListener("hashchange", () => {
    const tab = (location.hash || "").replace(/^#\/?/, "").split("/")[0];
    if (tab && tab !== "live" && stream) stop();
  });
}
