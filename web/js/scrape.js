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
let dataRows = [];   // raw API data (unsorted), preserved across renders
let sortCol = null;  // active sort column index, or null
let sortDir = 1;     // 1 = asc, -1 = desc
let etaTick = null;
let countdownTick = null;

function progressPct(r) {
  if (r.running_now) return r.progress?.pct ?? 0;
  if (r.status === 'never' || r.status === 'gone' || r.health === 'dead') return -1;
  const t = r.stores_total || 0, h = r.stores_with_data || 0;
  return t > 0 ? (h / t) * 100 : 0;
}

const COL_SORT_FNS = [
  r => (r.name_he || r.code).toLowerCase(),
  r => progressPct(r),
  r => r.stores_with_data || 0,
  r => r.products_priced || 0,
  r => r.current_prices || 0,
  r => r.last_scrape_at
    ? new Date((r.last_scrape_at.endsWith('Z') || r.last_scrape_at.includes('+'))
        ? r.last_scrape_at : r.last_scrape_at + 'Z').getTime()
    : 0,
  r => r.running_now ? (r.eta_seconds || 0) : (r.next_run_eta_seconds ?? 1e9),
];

function applySortedRows(rows) {
  if (sortCol === null || !COL_SORT_FNS[sortCol]) return rows;
  const fn = COL_SORT_FNS[sortCol];
  return [...rows].sort((a, b) => {
    const va = fn(a), vb = fn(b);
    if (va < vb) return -sortDir;
    if (va > vb) return sortDir;
    return 0;
  });
}

// Reorder <tr> elements and update arrow indicators without re-rendering the table.
function sortInPlace() {
  if (sortCol === null) return;
  const tbody = document.querySelector(".scrape-table tbody");
  if (!tbody || !dataRows.length) return;
  const fn = COL_SORT_FNS[sortCol];
  if (!fn) return;

  // update header arrows and active class
  document.querySelectorAll(".scrape-table th[data-sort-col]").forEach(th => {
    const col = +th.dataset.sortCol;
    const active = col === sortCol;
    th.classList.toggle("th-sorted", active);
    const span = th.querySelector(".th-arrow");
    if (span) span.textContent = active ? (sortDir === 1 ? ' ▲' : ' ▼') : ' ⇅';
  });

  // reorder existing <tr> elements in-place
  const byCode = new Map(dataRows.map(r => [r.code, r]));
  const trs = [...tbody.querySelectorAll("tr[data-code]")];
  trs.sort((a, b) => {
    const ra = byCode.get(a.dataset.code);
    const rb = byCode.get(b.dataset.code);
    if (!ra || !rb) return 0;
    const va = fn(ra), vb = fn(rb);
    if (va < vb) return -sortDir;
    if (va > vb) return sortDir;
    return 0;
  });
  trs.forEach(tr => tbody.appendChild(tr)); // moves each tr to end in sorted order
}

const COLS = [
  { label: 'רשת' },
  { label: 'התקדמות' },
  { label: 'חנויות (יש/חסר)', title: 'חנויות במאגר / מתוכן עם מחירים נוכחיים' },
  { label: 'מוצרים (יש/חסר)', title: 'מוצרים שיש להם מחיר נוכחי / מוצרים שנצפו אי-פעם בקבצי הרשת' },
  { label: 'מחירים נוכחיים' },
  { label: 'סריקה אחרונה' },
  { label: 'ETA' },
  { label: '' },
];

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

function fmtBytes(b) {
  if (b == null) return "—";
  if (b < 1024) return b + " B";
  if (b < 1024 * 1024) return (b / 1024).toFixed(1) + " KB";
  if (b < 1024 * 1024 * 1024) return (b / 1024 / 1024).toFixed(1) + " MB";
  return (b / 1024 / 1024 / 1024).toFixed(2) + " GB";
}

// Unified progress: one bar replacing the old "סטטוס" + "בריאות" pair.
// Combines run state, coverage, and freshness into a single 0-100% with label.
function progressCell(r) {
  if (r.running_now) {
    const p = r.progress || {};
    const pct = Math.min(100, Math.max(0, p.pct ?? 0));
    return `
      <div class="prog prog-running">
        <div class="prog-bar"><span style="width:${pct}%"></span></div>
        <div class="prog-label"><span class="dot-pulse"></span>מתעדכן · ${pct}%</div>
      </div>`;
  }
  if (r.status === 'never') {
    return `<div class="prog prog-miss">
      <div class="prog-bar"><span style="width:0%"></span></div>
      <div class="prog-label">לא נסרק</div>
    </div>`;
  }
  if (r.status === 'gone' || r.health === 'dead') {
    return `<div class="prog prog-miss">
      <div class="prog-bar"><span style="width:0%"></span></div>
      <div class="prog-label">${r.status === 'gone' ? 'פג תוקף' : 'מת'}</div>
    </div>`;
  }
  const total = r.stores_total || 0;
  const has   = r.stores_with_data || 0;
  const pct   = total > 0 ? Math.round((has / total) * 100) : 0;
  let cls = 'prog-ok';
  if (r.status === 'stale' || r.health === 'partial' || r.health === 'thin' || pct < 80) cls = 'prog-warn';
  if (pct < 30) cls = 'prog-miss';
  return `<div class="prog ${cls}">
    <div class="prog-bar"><span style="width:${pct}%"></span></div>
    <div class="prog-label">${pct}% · ${has}/${total} חנויות</div>
  </div>`;
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
      <td class="prog-cell">${progressCell(r)}</td>
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

// Big gauge at top of the scrape tab.
// Aggregate progress = average per-chain progress %, where each chain gets:
//   running_now → its live progress.pct
//   never/gone  → 0
//   fresh/stale → stores_with_data / stores_total
function bigGaugeHtml(rows) {
  if (!rows.length) return '';
  let sum = 0, n = 0, runningCount = 0, doneCount = 0, missCount = 0;
  let totalFiles = 0, doneFiles = 0, totalRows = 0;
  let maxEta = 0, sumRate = 0;
  for (const r of rows) {
    let pct = 0;
    if (r.running_now) {
      const p = r.progress || {};
      pct = Math.min(100, Math.max(0, p.pct ?? 0));
      runningCount++;
      totalFiles += p.files_total || 0;
      doneFiles  += p.files_done  || 0;
      totalRows  += p.rows_written|| 0;
      sumRate    += p.rate_files_per_sec || 0;
      if (r.eta_seconds && r.eta_seconds > maxEta) maxEta = r.eta_seconds;
    } else if (r.status === 'never' || r.status === 'gone' || r.health === 'dead') {
      pct = 0;
      missCount++;
    } else {
      const t = r.stores_total || 0;
      const h = r.stores_with_data || 0;
      pct = t > 0 ? (h / t) * 100 : 0;
      if (pct >= 80) doneCount++;
    }
    sum += pct; n++;
  }
  const avg = Math.round(sum / n);
  let cls = 'big-ok';
  if (runningCount > 0) cls = 'big-running';
  else if (avg < 30) cls = 'big-miss';
  else if (avg < 80) cls = 'big-warn';

  const headline = runningCount > 0 ? 'סורק כעת' :
                   avg >= 80 ? 'נתונים מלאים' :
                   avg >= 30 ? 'כיסוי חלקי' : 'אין כיסוי מספיק';

  // build stat tiles (use _lastHealth from outer scope if present)
  const h = _lastHealth || {};
  const tiles = [];
  // Live progress info (only when scraping)
  if (runningCount > 0) {
    tiles.push(['🔄', 'סורק עכשיו', `${runningCount} רשתות`]);
    tiles.push(['📂', 'קבצים', `${fmtNum(doneFiles)} / ${fmtNum(totalFiles)}`]);
    tiles.push(['⏱️', 'ETA', fmtSecs(maxEta || null)]);
    tiles.push(['⚡', 'קצב', `${sumRate.toFixed(2)} קבצים/שנ׳`]);
  } else {
    tiles.push(['✅', 'מלאות', `${doneCount}`]);
    tiles.push(['⚫', 'ללא נתונים', `${missCount}`]);
    tiles.push(['📊', 'רשתות סה"כ', `${rows.length}`]);
  }
  // Cumulative DB stats
  tiles.push(['💰', 'מחירים נוכחיים', fmtNum(h.current_prices)]);
  tiles.push(['📦', 'מוצרים', fmtNum(h.products)]);
  tiles.push(['🏪', 'חנויות', fmtNum(h.stores)]);
  // Disk
  tiles.push(['💾', 'גודל DB', fmtBytes(h.db_bytes)]);
  if (h.raw_bytes != null) tiles.push(['📥', 'קבצי GZ', fmtBytes(h.raw_bytes)]);
  if (h.free_bytes != null) tiles.push(['🆓', 'פנוי בדיסק', fmtBytes(h.free_bytes)]);

  const tilesHtml = tiles.map(([ico, label, val]) => `
    <div class="big-tile">
      <div class="big-tile-ico">${ico}</div>
      <div class="big-tile-val">${val}</div>
      <div class="big-tile-label">${label}</div>
    </div>`).join('');

  return `
    <div class="big-gauge-inner ${cls}">
      <div class="big-gauge-circle" style="--pct:${avg}">
        <div class="big-gauge-pct">${avg}<small>%</small></div>
        <div class="big-gauge-label">סטטוס סריקה</div>
      </div>
      <div class="big-gauge-side">
        <div class="big-gauge-headline">
          ${runningCount > 0 ? '<span class="big-pulse"></span>' : ''}${headline}
        </div>
        <div class="big-tiles">${tilesHtml}</div>
      </div>
    </div>`;
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
  $("scrape-big-gauge").innerHTML = bigGaugeHtml(rows);
  const theadHtml = COLS.map((c, i) => {
    if (!c.label) return '<th></th>';
    const isSorted = sortCol === i;
    const arrow = isSorted ? (sortDir === 1 ? ' ▲' : ' ▼') : ' ⇅';
    const cls = isSorted ? ' class="th-sort th-sorted"' : ' class="th-sort"';
    const titleAttr = c.title ? ` title="${c.title}"` : '';
    return `<th${cls}${titleAttr} data-sort-col="${i}">${c.label}<span class="th-arrow">${arrow}</span></th>`;
  }).join('');
  wrap.innerHTML = `
    <table class="dash-retailers-table scrape-table">
      <thead><tr>${theadHtml}</tr></thead>
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
  dataRows = rows;
  // First-render path: full HTML. Otherwise: only update cells that changed
  // so the table doesn't flicker the user out of selection.
  if (!lastRows.length) {
    render(applySortedRows(rows));
    lastRows = rows;
    return;
  }
  const tbody = document.querySelector(".scrape-table tbody");
  if (!tbody) { render(applySortedRows(rows)); lastRows = rows; return; }
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
      (prev.progress?.pct ?? 0) !== (r.progress?.pct ?? 0) ||
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
  $("scrape-big-gauge").innerHTML = bigGaugeHtml(rows);
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

let _lastHealth = null;
async function fetchAndRender() {
  try {
    const [rows, health] = await Promise.all([
      api("/api/stats/retailers-status"),
      api("/api/health").catch(() => null),
    ]);
    _lastHealth = health;
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
    if (el.classList.contains("running")) {
      const n = +el.dataset.eta;
      if (Number.isFinite(n) && n > 0) {
        el.dataset.eta = String(n - 1);
        const timeSpan = el.querySelector(".eta-time");
        if (timeSpan) timeSpan.textContent = `· ${fmtSecs(n - 1)}`;
      }
    } else {
      const n = +el.dataset.next;
      if (Number.isFinite(n) && n > 0) {
        el.dataset.next = String(n - 1);
        el.textContent = `בעוד ${fmtSecs(n - 1)}`;
      }
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

  // Single document-level listener — survives table re-renders.
  // closest() on the target walks up and finds the <th data-sort-col> if it exists.
  document.addEventListener("click", e => {
    const th = e.target.closest("#scrape-table-wrap th[data-sort-col]");
    if (!th) return;
    const col = +th.dataset.sortCol;
    if (sortCol === col) sortDir *= -1;
    else { sortCol = col; sortDir = 1; }
    sortInPlace();
  });

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
