// Price-history chart. One series per chain (min price per day). Uses uPlot via
// CDN <script>, so `uPlot` is a global. If the CDN is blocked we fall back to a
// plain table.

import { api } from "./api.js";
import { shekel, renderError, escapeHtml, debounce } from "./ui.js";
import { current, setRoute } from "./router.js";

const form    = () => document.getElementById("form-trends");
const chart   = () => document.getElementById("trend-chart");
const table   = () => document.getElementById("trend-table");
const search  = () => document.getElementById("trend-search");
const hidden  = () => document.getElementById("trend-barcode");
const suggest = () => document.getElementById("trend-suggest");
const meta    = () => document.getElementById("trend-meta");

const PALETTE = ["#5aa6ff", "#7bd389", "#f2c14e", "#ef6f6c", "#c58bff", "#6ad8d8", "#ffa35a", "#a2ce6a"];
let plot = null;

function dayBucket(ts) {
  // YYYY-MM-DD → unix seconds at UTC midnight
  const d = new Date(ts);
  const s = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) / 1000;
  return s;
}

function toSeries(points) {
  // group by chain, then by day-min-price
  const chains = new Map();  // code → { name, code, byDay: Map<sec, min> }
  const allDays = new Set();
  for (const p of points) {
    if (!p.observed_at && !p.date) continue;
    const day = dayBucket(p.observed_at || p.date);
    allDays.add(day);
    const cur = chains.get(p.chain_code) ||
      chains.set(p.chain_code, { name: p.chain_name_he, code: p.chain_code, byDay: new Map() })
            .get(p.chain_code);
    const v = Number(p.price);
    const prev = cur.byDay.get(day);
    cur.byDay.set(day, prev == null ? v : Math.min(prev, v));
  }
  const xs = [...allDays].sort((a, b) => a - b);
  const series = [...chains.values()].map(c => ({
    name: c.name, code: c.code,
    values: xs.map(d => c.byDay.has(d) ? c.byDay.get(d) : null),
  }));
  return { xs, series };
}

function drawTable(series) {
  let html = "<table><thead><tr><th>רשת</th><th>נקודות</th><th>טווח</th></tr></thead><tbody>";
  for (const s of series) {
    const vals = s.values.filter(v => v != null);
    if (!vals.length) continue;
    const lo = Math.min(...vals), hi = Math.max(...vals);
    html += `<tr>
      <td>${escapeHtml(s.name)}</td>
      <td class="num">${vals.length}</td>
      <td class="num">${shekel(lo)} – ${shekel(hi)}</td>
    </tr>`;
  }
  html += "</tbody></table>";
  table().innerHTML = html;
}

function drawChart(xs, series) {
  const host = chart();
  host.innerHTML = "";
  if (!xs.length || !series.length) {
    host.innerHTML = '<div class="empty">אין היסטוריה</div>';
    return;
  }
  if (typeof uPlot === "undefined") {
    host.innerHTML = '<div class="empty">(uPlot לא נטען — מציג טבלה בלבד)</div>';
    return;
  }

  const data = [xs, ...series.map(s => s.values)];
  const opts = {
    width: Math.min(host.clientWidth || 720, 1100),
    height: 280,
    scales: { x: { time: true } },
    axes: [
      { stroke: "#9aa3c7", grid: { stroke: "rgba(255,255,255,.04)" } },
      { stroke: "#9aa3c7", grid: { stroke: "rgba(255,255,255,.04)" },
        values: (_, vals) => vals.map(v => "₪" + v.toFixed(2)) },
    ],
    series: [
      { label: "תאריך" },
      ...series.map((s, i) => ({
        label: s.name,
        stroke: PALETTE[i % PALETTE.length],
        width: 1.8,
        spanGaps: true,
        points: { show: true, size: 4 },
        value: (_, v) => v == null ? "—" : "₪" + Number(v).toFixed(2),
      })),
    ],
    legend: { live: true },
  };
  plot = new uPlot(opts, data, host);
  window.addEventListener("resize", () => {
    if (plot) plot.setSize({ width: Math.min(host.clientWidth || 720, 1100), height: 280 });
  }, { once: true });
}

async function runTrend(bc, days, push = true) {
  const root = table();
  chart().innerHTML = "";
  if (!bc) { root.innerHTML = '<div class="empty">הזן ברקוד</div>'; return; }
  if (push) setRoute("trends", { bc, days });

  root.innerHTML = '<div class="loading">טוען…</div>';
  try {
    const points = await api(`/api/trends/${encodeURIComponent(bc)}`, { days });
    if (!points.length) {
      chart().innerHTML = "";
      root.innerHTML = '<div class="empty">אין היסטוריה</div>';
      return;
    }
    const { xs, series } = toSeries(points);
    drawChart(xs, series);
    drawTable(series);
  } catch (e) {
    renderError(root, e, () => runTrend(bc, days, false));
  }
}

// ---- product autocomplete ----

let activeIdx = -1;
let activeRows = [];

function isBarcode(s) {
  return /^\d{6,}$/.test(s.trim());
}

function closeSuggest() {
  suggest().hidden = true;
  suggest().innerHTML = "";
  activeRows = [];
  activeIdx = -1;
}

function pickSuggestion(row) {
  hidden().value = row.barcode;
  search().value = row.name || row.barcode;
  meta().textContent = `${row.barcode} · ${row.name || ""}${row.manufacturer ? " · " + row.manufacturer : ""}`;
  closeSuggest();
}

function renderSuggest(rows) {
  if (!rows.length) {
    suggest().innerHTML = '<li class="empty">אין תוצאות</li>';
    suggest().hidden = false; activeRows = []; activeIdx = -1; return;
  }
  activeRows = rows;
  activeIdx = -1;
  suggest().innerHTML = rows.map((r, i) => `
    <li role="option" data-i="${i}" class="suggest-row">
      <div class="s-name">${escapeHtml(r.name || "—")}</div>
      <div class="s-sub">${escapeHtml(r.manufacturer || "")} · ${escapeHtml(r.barcode)} · מ-${shekel(r.min_price)}</div>
    </li>
  `).join("");
  suggest().hidden = false;
  suggest().querySelectorAll(".suggest-row").forEach(li => {
    li.addEventListener("mousedown", e => {
      e.preventDefault();
      pickSuggestion(rows[+li.dataset.i]);
    });
  });
}

const fetchSuggestions = debounce(async (q) => {
  if (!q || q.length < 2) { closeSuggest(); return; }
  // exact-barcode shortcut
  if (isBarcode(q)) {
    hidden().value = q.trim();
    closeSuggest();
    meta().textContent = `ברקוד: ${q.trim()}`;
    return;
  }
  try {
    const rows = await api("/api/search", { q, limit: 8 });
    renderSuggest(rows);
  } catch {
    closeSuggest();
  }
}, 220);

function onSearchInput() {
  hidden().value = "";
  meta().textContent = "";
  fetchSuggestions(search().value);
}

function onSearchKeydown(e) {
  if (suggest().hidden) return;
  const max = activeRows.length - 1;
  if (e.key === "ArrowDown") {
    e.preventDefault(); activeIdx = Math.min(max, activeIdx + 1); highlight();
  } else if (e.key === "ArrowUp") {
    e.preventDefault(); activeIdx = Math.max(0, activeIdx - 1); highlight();
  } else if (e.key === "Enter" && activeIdx >= 0) {
    e.preventDefault(); pickSuggestion(activeRows[activeIdx]);
  } else if (e.key === "Escape") {
    closeSuggest();
  }
}

function highlight() {
  suggest().querySelectorAll(".suggest-row").forEach((li, i) => {
    li.classList.toggle("active", i === activeIdx);
  });
}

export function initTrends() {
  form().addEventListener("submit", async e => {
    e.preventDefault();
    let bc = hidden().value.trim();
    const q  = search().value.trim();
    // If user typed a barcode directly and didn't pick a suggestion, accept it.
    if (!bc && isBarcode(q)) bc = q;
    // If user typed a name without picking anything, look it up server-side
    // and pick the cheapest match.
    if (!bc && q) {
      try {
        const rows = await api("/api/search", { q, limit: 1 });
        if (rows.length) { bc = rows[0].barcode; pickSuggestion(rows[0]); }
      } catch { /* leave bc empty, fall through to error message */ }
    }
    const days = form().elements.days.value;
    runTrend(bc, days);
  });

  search().addEventListener("input", onSearchInput);
  search().addEventListener("keydown", onSearchKeydown);
  search().addEventListener("blur", () => setTimeout(closeSuggest, 150));

  const { tab, params } = current();
  if (tab === "trends") applyParams(params);
}

export function onRouted(state) {
  if (state.tab !== "trends") return;
  applyParams(state.params);
}

async function applyParams(params) {
  const bc = params.get("bc") || "";
  const days = params.get("days") || "30";
  form().elements.days.value = days;
  if (bc) {
    hidden().value = bc;
    // try to fill in the name for context
    try {
      const p = await api(`/api/products/${encodeURIComponent(bc)}`);
      search().value = p.name || bc;
      meta().textContent = `${bc} · ${p.name || ""}${p.manufacturer ? " · " + p.manufacturer : ""}`;
    } catch { search().value = bc; }
    runTrend(bc, days, false);
  }
}
