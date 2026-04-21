// Price-history chart. One series per chain (min price per day). Uses uPlot via
// CDN <script>, so `uPlot` is a global. If the CDN is blocked we fall back to a
// plain table.

import { api } from "./api.js";
import { shekel, renderError, escapeHtml } from "./ui.js";
import { current, setRoute } from "./router.js";

const form   = () => document.getElementById("form-trends");
const chart  = () => document.getElementById("trend-chart");
const table  = () => document.getElementById("trend-table");

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

export function initTrends() {
  form().addEventListener("submit", e => {
    e.preventDefault();
    const bc = form().elements.barcode.value.trim();
    const days = form().elements.days.value;
    runTrend(bc, days);
  });

  const { tab, params } = current();
  if (tab === "trends") applyParams(params);
}

export function onRouted(state) {
  if (state.tab !== "trends") return;
  applyParams(state.params);
}

function applyParams(params) {
  const bc = params.get("bc") || "";
  const days = params.get("days") || "30";
  form().elements.barcode.value = bc;
  form().elements.days.value = days;
  if (bc) runTrend(bc, days, false);
}
