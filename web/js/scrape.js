// Scrape-status tab — live retailer ingest health.
//
// Polls /api/stats/retailers-status (cheap) every POLL_MS while the tab is
// visible. Renders a single table; updates cells in place to avoid flicker.
// Click a row to open the per-retailer stores modal (same as before, but now
// reachable from this tab too).
//
// ETA columns:
//   - "running" chains: countdown to expected scrape completion
//     (median past run duration − elapsed since started)
//   - "idle" chains:    countdown to next 04:15 UTC daily window

import { api } from "./api.js";
import { escapeHtml, renderError } from "./ui.js";
import { current } from "./router.js";

const POLL_MS_VISIBLE        = 5000;
const POLL_MS_VISIBLE_ACTIVE = 1500;   // poll faster when something is running
const POLL_MS_HIDDEN         = 60_000; // back off when tab not focused

let timer = null;
let lastRows = [];
let etaTick = null;
let countdownTick = null;

const $ = id => document.getElementById(id);

function fmtNum(n) {
  if (n == null || Number.isNaN(+n)) return "—";
  return Number(n).toLocaleString();
}

function fmtTs(ts) {
  if (!ts) return "—";
  const d = new Date(ts.endsWith("Z") || ts.includes("+") ? ts : ts + "Z");
  if (Number.isNaN(+d)) return ts;
  return d.toLocaleString("he-IL", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

function fmtSecs(s) {
  if (s == null) return "—";
  if (s < 60) return `${s} שנ׳`;
  if (s < 3600) return `${Math.floor(s/60)}:${String(s%60).padStart(2, "0")}`;
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
  return `${h} שע׳ ${m} דק׳`;
}

function statusIcon(s) {
  return ({
    fresh: '<span class="pill ok">🟢 היום</span>',
    stale: '<span class="pill warn">🟡 ישן</span>',
    gone:  '<span class="pill err">🔴 פג</span>',
    never: '<span class="pill err">⚫ אין</span>',
  })[s] || s;
}

function healthIcon(h) {
  return ({
    ok:      '<span class="pill ok">✅ תקין</span>',
    partial: '<span class="pill warn">🟡 חלקי</span>',
    thin:    '<span class="pill warn">⚠️ דליל</span>',
    dead:    '<span class="pill err">💀 מת</span>',
  })[h] || h;
}

function etaCell(r) {
  if (r.running_now) {
    const sec = r.eta_seconds;
    const p   = r.progress || {};
    const pct = p.pct ?? 0;
    const done = p.files_done ?? 0;
    const total = p.files_total ?? 0;
    const rate = p.rate_files_per_sec ?? 0;
    const rows = p.rows_written ?? 0;
    return `
      <div class="eta running" data-eta="${sec ?? ""}">
        <div class="eta-line">
          <span class="dot-pulse"></span>
          <span class="eta-pct">${pct}%</span>
          <span class="eta-rate">${rate.toFixed ? rate.toFixed(2) : rate}/s</span>
          <span class="eta-time">· ${fmtSecs(sec)}</span>
        </div>
        <div class="eta-bar"><span style="width:${Math.min(100, pct)}%"></span></div>
        <div class="eta-sub">${fmtNum(done)}/${fmtNum(total)} קבצים · ${fmtNum(rows)} שורות</div>
      </div>`;
  }
  if (r.next_run_eta_seconds != null) {
    return `<span class="eta queued" data-next="${r.next_run_eta_seconds}">
      בעוד ${fmtSecs(r.next_run_eta_seconds)}
    </span>`;
  }
  return "—";
}

function rowClass(r) {
  // Three-tier color: yellow = currently scraping, green = recently analyzed
  // and looks healthy, red = either never scraped or out of retention.
  if (r.running_now) return "row-active";
  if (r.status === "fresh" || r.status === "stale") return "row-ok";
  return "row-miss";
}

function rowHtml(r) {
  const ageTxt = r.days_stale == null ? "—" :
    (r.days_stale === 0 ? "היום" : `לפני ${r.days_stale} ימים`);
  const stHas = r.stores_with_data || 0;
  const stTot = r.stores_total || 0;
  const stMiss = r.stores_missing || 0;
  const prHas = r.products_priced || 0;
  const prMiss = r.products_missing || 0;
  const prTot = r.products_observed || 0;
  const cls = rowClass(r);
  return `
    <tr class="ret-row ${cls}${r.running_now ? ' running' : ''}" data-code="${escapeHtml(r.code)}" tabindex="0">
      <td>
        <div class="ret-name">${escapeHtml(r.name_he || r.code)}</div>
        <div class="ret-code">${escapeHtml(r.code)}</div>
      </td>
      <td>${statusIcon(r.status)}</td>
      <td>${healthIcon(r.health)}</td>
      <td class="num">
        <b>${fmtNum(stHas)}</b><span class="muted"> / ${fmtNum(stTot)}</span>
        ${stMiss > 0 ? `<div class="miss">חסר: ${fmtNum(stMiss)}</div>` : ""}
      </td>
      <td class="num">
        <b>${fmtNum(prHas)}</b><span class="muted"> / ${fmtNum(prTot)}</span>
        ${prMiss > 0 ? `<div class="miss">חסר: ${fmtNum(prMiss)}</div>` : ""}
      </td>
      <td class="num">${fmtNum(r.current_prices)}</td>
      <td>${ageTxt}<div class="muted">${fmtTs(r.last_scrape_at)}</div></td>
      <td class="eta-cell">${etaCell(r)}</td>
      <td><button class="btn-small" type="button" data-action="view-stores">צפה בחנויות</button></td>
    </tr>`;
}

function summaryHtml(rows) {
  const fresh = rows.filter(r => r.status === "fresh").length;
  const stale = rows.filter(r => r.status === "stale").length;
  const gone  = rows.filter(r => r.status === "gone").length;
  const never = rows.filter(r => r.status === "never").length;
  const running = rows.filter(r => r.running_now).length;
  const totMissingStores   = rows.reduce((a, r) => a + (r.stores_missing   || 0), 0);
  const totMissingProducts = rows.reduce((a, r) => a + (r.products_missing || 0), 0);
  return `
    <span>סה"כ <b>${rows.length}</b></span>
    <span>· מתעדכנים עכשיו <b class="ok">${running}</b></span>
    <span>· היום <b class="ok">${fresh}</b></span>
    <span>· ישן <b class="warn">${stale}</b></span>
    <span>· פג <b class="err">${gone}</b></span>
    <span>· אין <b class="err">${never}</b></span>
    <span>· חנויות חסרות <b>${fmtNum(totMissingStores)}</b></span>
    <span>· מוצרים חסרים <b>${fmtNum(totMissingProducts)}</b></span>`;
}

function render(rows) {
  const wrap = $("scrape-table-wrap");
  if (!wrap) return;
  $("scrape-summary").innerHTML = summaryHtml(rows);
  wrap.innerHTML = `
    <table class="dash-retailers-table scrape-table">
      <thead><tr>
        <th>רשת</th>
        <th>סטטוס</th>
        <th>בריאות</th>
        <th title="חנויות במאגר / מתוכן עם מחירים נוכחיים">חנויות (יש/חסר)</th>
        <th title="מוצרים שיש להם מחיר נוכחי / מוצרים שנצפו אי-פעם בקבצי הרשת">מוצרים (יש/חסר)</th>
        <th>מחירים נוכחיים</th>
        <th>סריקה אחרונה</th>
        <th>ETA</th>
        <th></th>
      </tr></thead>
      <tbody>${rows.map(rowHtml).join("")}</tbody>
    </table>`;
  // open per-retailer modal on row click / button
  wrap.querySelectorAll(".ret-row").forEach(row => {
    const open = () => openChainStoresModal(row.dataset.code);
    row.addEventListener("click", e => {
      if (e.target.closest("button[data-action='view-stores']")) {
        e.preventDefault(); open();
      }
    });
    row.addEventListener("keydown", e => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); }
    });
  });
}

function diffRender(rows) {
  // First-render path: full HTML. Otherwise: only update cells that changed
  // so the table doesn't flicker the user out of selection.
  if (!lastRows.length) {
    render(rows);
    lastRows = rows;
    return;
  }
  const tbody = document.querySelector(".scrape-table tbody");
  if (!tbody) { render(rows); lastRows = rows; return; }
  const byCode = new Map(rows.map(r => [r.code, r]));
  const seenCodes = new Set();
  for (const tr of tbody.querySelectorAll("tr")) {
    const code = tr.dataset.code;
    seenCodes.add(code);
    const r = byCode.get(code);
    if (!r) { tr.remove(); continue; }
    const prev = lastRows.find(p => p.code === code);
    const cellsChanged = !prev ||
      prev.current_prices   !== r.current_prices   ||
      prev.stores_with_data !== r.stores_with_data ||
      prev.stores_total     !== r.stores_total     ||
      prev.products_priced  !== r.products_priced  ||
      prev.products_observed!== r.products_observed||
      prev.status           !== r.status           ||
      prev.health           !== r.health           ||
      prev.running_now      !== r.running_now      ||
      prev.last_scrape_at   !== r.last_scrape_at;
    if (cellsChanged) {
      const newRow = document.createElement("tbody");
      newRow.innerHTML = rowHtml(r);
      const replacement = newRow.firstElementChild;
      tr.replaceWith(replacement);
      replacement.classList.add("flash");
      setTimeout(() => replacement.classList.remove("flash"), 1600);
      attachRowHandlers(replacement);
    } else {
      // refresh ETA cell only (it's a countdown so it ticks every sec anyway)
      const etaTd = tr.querySelector(".eta-cell");
      if (etaTd) etaTd.innerHTML = etaCell(r);
    }
  }
  // append new chains (rare)
  for (const r of rows) {
    if (!seenCodes.has(r.code)) {
      const newRow = document.createElement("tbody");
      newRow.innerHTML = rowHtml(r);
      const tr = newRow.firstElementChild;
      tbody.appendChild(tr);
      attachRowHandlers(tr);
    }
  }
  $("scrape-summary").innerHTML = summaryHtml(rows);
  lastRows = rows;
}

function attachRowHandlers(row) {
  const open = () => openChainStoresModal(row.dataset.code);
  row.addEventListener("click", e => {
    if (e.target.closest("button[data-action='view-stores']")) { e.preventDefault(); open(); }
  });
  row.addEventListener("keydown", e => {
    if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); }
  });
}

// ---------- per-retailer stores modal (shared with dashboard) ----------

async function openChainStoresModal(code) {
  const modal   = $("modal");
  const title   = $("modal-title");
  const content = $("modal-content");
  if (!modal || !title || !content) return;
  title.textContent = "טוען חנויות…";
  content.innerHTML = '<div class="empty">טוען…</div>';
  modal.classList.remove("hidden");
  modal.hidden = false;
  try {
    const j = await api(`/api/stats/chain-stores/${encodeURIComponent(code)}`);
    const ch = j.chain || { name_he: code, name_en: "", portal_url: "" };
    const t = j.totals || {};
    title.textContent = `${ch.name_he || code} · ${fmtNum(t.total)} חנויות`;
    let body = `
      <div class="store-list-summary">
        <span>חנויות במאגר: <b>${fmtNum(t.total)}</b></span>
        <span>· עם מחירים: <b class="ok">${fmtNum(t.with_prices)}</b></span>
        <span>· ללא מחירים: <b class="err">${fmtNum(t.missing_prices)}</b></span>
        <span>· עם נתוני עיר: <b>${fmtNum(t.with_city)}</b></span>
        ${ch.portal_url ? `· <a href="${escapeHtml(ch.portal_url)}" target="_blank" rel="noopener">פורטל</a>` : ""}
      </div>`;
    if (!j.stores.length) {
      body += '<div class="empty">לא נמצאו חנויות במאגר עבור הרשת הזו.</div>';
    } else {
      body += `
        <table class="dash-stores-table">
          <thead><tr><th>קוד</th><th>שם</th><th>עיר</th><th>כתובת</th><th>מחירים</th><th>עודכן</th></tr></thead>
          <tbody>`;
      for (const s of j.stores) {
        const cls = (s.prices || 0) > 0 ? "store-ok" : "store-empty";
        body += `<tr class="${cls}">
          <td class="num">${escapeHtml(s.store_code || "")}</td>
          <td>${escapeHtml(s.name || "")}</td>
          <td>${escapeHtml(s.city || "—")}</td>
          <td>${escapeHtml(s.address || "")}</td>
          <td class="num">${fmtNum(s.prices)}</td>
          <td>${fmtTs(s.last_priced)}</td>
        </tr>`;
      }
      body += "</tbody></table>";
    }
    content.innerHTML = body;
  } catch (e) {
    content.innerHTML = `<div class="err">שגיאה בטעינה: ${escapeHtml(e?.detail || "")}</div>`;
  }
}

// ---------- polling ----------

async function fetchAndRender() {
  try {
    const rows = await api("/api/stats/retailers-status");
    const wasAnyRunning = lastRows.some(r => r.running_now);
    diffRender(rows);
    $("scrape-last-update").textContent = "עודכן " + new Date().toLocaleTimeString("he-IL");
    const nowAnyRunning = rows.some(r => r.running_now);
    if (wasAnyRunning !== nowAnyRunning) scheduleNextPoll();
  } catch (e) {
    if (!lastRows.length) {
      renderError($("scrape-table-wrap"), e, fetchAndRender);
    }
  }
}

function startPolling() {
  stopPolling();
  fetchAndRender();
  scheduleNextPoll();
  // Tick the ETA countdowns every second so the user sees them moving even
  // between full-refresh polls.
  if (!countdownTick) countdownTick = setInterval(tickCountdowns, 1000);
}

function scheduleNextPoll() {
  if (timer) clearInterval(timer);
  // Faster cadence when at least one chain is mid-scrape; slow when idle so
  // we don't hammer the API for nothing.
  const anyRunning = lastRows.some(r => r.running_now);
  const interval = document.hidden
    ? POLL_MS_HIDDEN
    : (anyRunning ? POLL_MS_VISIBLE_ACTIVE : POLL_MS_VISIBLE);
  timer = setInterval(fetchAndRender, interval);
}

function stopPolling() {
  if (timer) { clearInterval(timer); timer = null; }
  if (countdownTick) { clearInterval(countdownTick); countdownTick = null; }
}

function tickCountdowns() {
  document.querySelectorAll(".scrape-table .eta").forEach(el => {
    const eta  = +el.dataset.eta;
    const next = +el.dataset.next;
    let n = el.classList.contains("running") ? eta : next;
    if (Number.isFinite(n) && n > 0) {
      n -= 1;
      if (el.classList.contains("running")) el.dataset.eta = String(n);
      else el.dataset.next = String(n);
      const span = el.querySelector(".dot-pulse");
      const prefix = el.classList.contains("running") ? "מתעדכן · " : "בעוד ";
      el.innerHTML = (span ? span.outerHTML + " " : "") + prefix + fmtSecs(n);
    }
  });
}

function autoToggle() {
  if ($("scrape-auto").checked) startPolling();
  else stopPolling();
}

export function initScrapeStatus() {
  if (!$("pane-scrape")) return;
  $("scrape-refresh")?.addEventListener("click", fetchAndRender);
  $("scrape-auto")?.addEventListener("change", autoToggle);
  document.addEventListener("visibilitychange", () => {
    if (current().tab !== "scrape") return;
    if (document.hidden) {
      stopPolling();
      // resume at slower cadence to keep totals fresh-ish
      timer = setInterval(fetchAndRender, POLL_MS_HIDDEN);
    } else {
      startPolling();
    }
  });
  // Hash-routing handles tab visibility; respond to that.
  window.addEventListener("hashchange", maybeStart);
  maybeStart();
}

function maybeStart() {
  if (current().tab === "scrape" && $("scrape-auto")?.checked) {
    startPolling();
  } else {
    stopPolling();
  }
}
