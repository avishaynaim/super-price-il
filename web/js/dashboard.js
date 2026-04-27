// Dashboard tab — aggregate overview with charts & tables.
// Hits /api/stats/* endpoints, renders uPlot bar chart for chain coverage,
// plus lists for scrape health, top-spread products, promos, cities, and run history.
//
// All location-aware cards (chains, top-spread, promos) honor the user's
// saved location prefs (city, coords+radius). The dashboard also renders its
// own inline location bar (loc-city, loc-radius, loc-geo) which writes to the
// same prefs store — changes trigger a re-fetch across cards.

import { api } from "./api.js";
import { shekel, escapeHtml, renderError, toast } from "./ui.js";
import { current } from "./router.js";
import { openProduct } from "./product.js";
import { getPrefs, setPrefs, onPrefsChange, prefsQuery } from "./prefs.js";

const PALETTE = ["#5aa6ff", "#7bd389", "#f2c14e", "#ef6f6c", "#c58bff", "#6ad8d8", "#ffa35a", "#a2ce6a"];

let chainPlot = null;
let loaded = false;
let citiesLoaded = false;

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

function statusPill(s) {
  const cls = s === "ok" ? "ok" : s === "partial" ? "warn" : s === "running" ? "running" : "err";
  return `<span class="pill ${cls}">${escapeHtml(s || "—")}</span>`;
}

// ---------- location bar ----------

function locFields() {
  return {
    city:   document.getElementById("loc-city"),
    radius: document.getElementById("loc-radius"),
    label:  document.getElementById("loc-radius-label"),
    geo:    document.getElementById("loc-geo"),
    clear:  document.getElementById("loc-clear"),
    status: document.getElementById("loc-status"),
  };
}

function syncLocBarFromPrefs() {
  const f = locFields();
  if (!f.city) return;
  const p = getPrefs();
  f.city.value = p.city || "";
  f.radius.value = String(p.radius_km || 0);
  updateRadiusLabel();
  renderLocStatus();
}

function updateRadiusLabel() {
  const f = locFields();
  if (!f.label) return;
  const n = +f.radius.value || 0;
  f.label.textContent = n === 0 ? '(ללא סינון טווח)' : `בתוך ${n} ק"מ`;
}

function renderLocStatus() {
  const f = locFields();
  if (!f.status) return;
  const p = getPrefs();
  const bits = [];
  if (p.coords && p.radius_km > 0) {
    bits.push(`מציג חנויות בתוך ${p.radius_km} ק"מ ממיקום שמור`);
  } else if (p.city) {
    bits.push(`מציג חנויות בעיר: ${p.city}`);
  } else {
    bits.push("מציג נתונים לכל הארץ");
  }
  const nChains = (p.preferred_chains || []).length;
  if (nChains > 0) {
    bits.push(`רשתות מועדפות: ${nChains} (ערוך ב-⚙)`);
  }
  if (p.coords && p.radius_km > 0) {
    bits.push('הערה: כ-97% מהחנויות ללא נתוני עיר — הסינון יצמצם משמעותית.');
  }
  f.status.textContent = bits.join(" · ");
}

async function ensureCitiesLoaded() {
  if (citiesLoaded) return;
  try {
    const rows = await api("/api/cities");
    const dl = document.getElementById("cities-datalist");
    if (dl) {
      dl.innerHTML = rows.map(r => {
        const label = r.stores ? ` (${r.stores})` : "";
        return `<option value="${escapeHtml(r.name_he)}">${escapeHtml(r.name_he + label)}</option>`;
      }).join("");
    }
    citiesLoaded = true;
  } catch {
    // non-fatal — input still works as free text
  }
}

async function useMyLocation() {
  if (!navigator.geolocation) { toast("הדפדפן לא תומך ב-geolocation", "err"); return; }
  const f = locFields();
  f.status.textContent = "מבקש הרשאה…";
  navigator.geolocation.getCurrentPosition(async pos => {
    const lat = pos.coords.latitude, lng = pos.coords.longitude;
    try {
      const near = await api("/api/nearest-city", { lat, lng });
      const p = getPrefs();
      const radius = p.radius_km > 0 ? p.radius_km : 10;
      setPrefs({ coords: { lat, lng }, city: near.name_he, radius_km: radius });
      toast(`אותר: ${near.name_he} (${near.distance_km} ק"מ) · טווח ${radius} ק"מ`, "ok");
    } catch {
      setPrefs({ coords: { lat, lng } });
      toast("שמרנו מיקום אך לא נמצאה עיר קרובה", "warn");
    }
  }, err => {
    renderLocStatus();
    toast(err.message || "איתור נדחה", "err");
  }, { enableHighAccuracy: false, timeout: 8000, maximumAge: 60000 });
}

function initLocBar() {
  const f = locFields();
  if (!f.city) return;

  f.city.addEventListener("change", () => {
    const v = f.city.value.trim() || null;
    setPrefs({ city: v });
  });

  f.radius.addEventListener("input", updateRadiusLabel);
  f.radius.addEventListener("change", () => {
    const n = Math.max(0, Math.min(500, +f.radius.value || 0));
    setPrefs({ radius_km: n });
  });

  f.geo.addEventListener("click", useMyLocation);

  f.clear.addEventListener("click", () => {
    setPrefs({ city: null, coords: null, radius_km: 0 });
  });

  syncLocBarFromPrefs();
  ensureCitiesLoaded();
}

// ---------- KPI strip ----------
function isScoped() {
  const p = getPrefs();
  return !!p.city
    || (p.coords && p.radius_km > 0)
    || (Array.isArray(p.preferred_chains) && p.preferred_chains.length > 0);
}

async function loadKPIs() {
  // When no scope is active, show global counts from /api/health.
  // When scope is active, KPIs are overwritten by loadChains() using the
  // scoped chain-coverage rows (see renderScopedKPIs).
  if (isScoped()) return;
  const host = document.getElementById("dash-kpis");
  host.innerHTML = '<div class="loading">טוען…</div>';
  try {
    const h = await api("/api/health");
    host.innerHTML = `
      <div class="kpi"><div class="k">רשתות</div><div class="v">${fmtNum(h.chains_active)}</div></div>
      <div class="kpi"><div class="k">חנויות</div><div class="v">${fmtNum(h.stores)}</div></div>
      <div class="kpi"><div class="k">מוצרים</div><div class="v">${fmtNum(h.products)}</div></div>
      <div class="kpi"><div class="k">מחירים נוכחיים</div><div class="v">${fmtNum(h.current_prices)}</div></div>
      <div class="kpi"><div class="k">תצפיות היסטוריות</div><div class="v">${fmtNum(h.price_observations)}</div></div>
    `;
  } catch (e) {
    renderError(host, e, loadKPIs);
  }
}

function renderScopedKPIs(rows) {
  if (!isScoped()) return;
  const host = document.getElementById("dash-kpis");
  const chains = rows.length;
  const stores       = rows.reduce((s, r) => s + (+r.stores         || 0), 0);
  const current      = rows.reduce((s, r) => s + (+r.current_prices || 0), 0);
  const observations = rows.reduce((s, r) => s + (+r.observations   || 0), 0);
  const products     = rows.reduce((s, r) => s + (+r.products_priced || 0), 0);
  host.innerHTML = `
    <div class="kpi"><div class="k">רשתות בטווח</div><div class="v">${fmtNum(chains)}</div></div>
    <div class="kpi"><div class="k">חנויות בטווח</div><div class="v">${fmtNum(stores)}</div></div>
    <div class="kpi"><div class="k">פריטים (לפי רשת)</div><div class="v">${fmtNum(products)}</div></div>
    <div class="kpi"><div class="k">מחירים נוכחיים</div><div class="v">${fmtNum(current)}</div></div>
    <div class="kpi"><div class="k">תצפיות היסטוריות</div><div class="v">${fmtNum(observations)}</div></div>
  `;
}

// ---------- chain coverage: bar chart + sub-table ----------
function drawChainChart(rows) {
  const host = document.getElementById("dash-chain-chart");
  host.innerHTML = "";
  if (!rows.length) { host.innerHTML = '<div class="empty">אין נתונים למיקום הנבחר</div>'; return; }
  if (typeof uPlot === "undefined") {
    host.innerHTML = '<div class="empty">(uPlot לא נטען)</div>';
    return;
  }

  const data = rows.slice(0, 20);
  const xs = data.map((_, i) => i);
  const ys = data.map(r => r.products_priced || 0);

  const opts = {
    width: Math.min(host.clientWidth || 720, 1100),
    height: 320,
    padding: [12, 16, 36, 12],
    scales: {
      x: { time: false, range: [-0.5, xs.length - 0.5] },
      y: { range: (_u, min, max) => [0, Math.max(max * 1.05, 10)] },
    },
    axes: [
      {
        stroke: "#9aa3c7",
        grid: { show: false },
        values: (_u, vals) => vals.map(v => data[v]?.name_he || ""),
        rotate: -35,
        size: 80,
      },
      {
        stroke: "#9aa3c7",
        grid: { stroke: "rgba(255,255,255,.04)" },
        values: (_u, vals) => vals.map(v => v >= 1000 ? (v / 1000).toFixed(0) + "k" : String(v)),
      },
    ],
    series: [
      { label: "רשת" },
      {
        label: "מוצרים עם מחיר",
        stroke: PALETTE[0],
        fill: PALETTE[0] + "55",
        width: 0,
        paths: uPlot.paths.bars({ size: [0.7, 40], align: 0 }),
        points: { show: false },
        value: (_u, v, _s, idx) => v == null ? "—" : `${fmtNum(v)} (${data[idx]?.name_he || ""})`,
      },
    ],
    legend: { show: false },
    cursor: { points: { show: false }, drag: { x: false, y: false } },
  };
  chainPlot = new uPlot(opts, [xs, ys], host);
}

async function loadChains() {
  const host = document.getElementById("dash-chain-chart");
  host.innerHTML = '<div class="loading">טוען…</div>';
  try {
    const rows = await api("/api/stats/chains", prefsQuery({ includeChain: true }));
    drawChainChart(rows);
    drawHealth(rows);
    renderScopedKPIs(rows);
  } catch (e) {
    renderError(host, e, loadChains);
  }
}

// ---------- scrape health per chain ----------
function drawHealth(rows) {
  const host = document.getElementById("dash-health");
  if (!rows.length) { host.innerHTML = '<div class="empty">אין ריצות סריקה</div>'; return; }
  const sorted = [...rows].sort((a, b) => {
    const ra = a.last_status || "";
    const rb = b.last_status || "";
    const rank = s => s === "ok" ? 3 : s === "partial" ? 2 : s === "running" ? 1 : 0;
    return rank(ra) - rank(rb) || String(b.last_started_at || "").localeCompare(String(a.last_started_at || ""));
  });
  host.innerHTML = sorted.map(r => `
    <div class="dash-row">
      <div class="dash-row-main">
        <div class="nm">${escapeHtml(r.name_he)}</div>
        <div class="sub">${fmtTs(r.last_started_at)} · files ${fmtNum(r.last_files_ok || 0)}/${fmtNum((r.last_files_ok || 0) + (r.last_files_failed || 0))} · rows ${fmtNum(r.last_rows_written || 0)}</div>
      </div>
      <div>${statusPill(r.last_status)}</div>
    </div>
  `).join("");
}

// ---------- top-spread products ----------
async function loadSpread() {
  const host = document.getElementById("dash-spread");
  host.innerHTML = '<div class="loading">טוען…</div>';
  try {
    // If the user picked N preferred chains, min_chains can't exceed N —
    // otherwise we'd always return zero rows. Floor at 2 (spread needs at least 2).
    const nChains = (getPrefs().preferred_chains || []).length;
    const minChains = nChains > 0 ? Math.min(4, Math.max(2, nChains)) : 4;
    const rows = await api("/api/stats/top-spread", {
      limit: 20, min_chains: minChains, min_price: 5,
      ...prefsQuery({ includeChain: true }),
    });
    if (!rows.length) { host.innerHTML = '<div class="empty">אין מוצרים מתאימים למיקום הנבחר</div>'; return; }
    host.innerHTML = rows.map(r => `
      <div class="dash-row clickable" data-barcode="${escapeHtml(r.barcode)}" role="button" tabindex="0">
        <div class="dash-row-main">
          <div class="nm">${escapeHtml(r.name || "—")}</div>
          <div class="sub">${escapeHtml(r.manufacturer || "")} · ${fmtNum(r.chains_with_price)} רשתות · ${shekel(r.min_price)} – ${shekel(r.max_price)}</div>
        </div>
        <div class="spread-pct">${Number(r.spread_pct).toFixed(0)}%</div>
      </div>
    `).join("");
    host.querySelectorAll(".dash-row.clickable").forEach(el => {
      const bc = el.dataset.barcode;
      el.addEventListener("click", () => openProduct(bc));
      el.addEventListener("keydown", e => {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); openProduct(bc); }
      });
    });
  } catch (e) {
    renderError(host, e, loadSpread);
  }
}

// ---------- recent promotions ----------
async function loadPromos() {
  const host = document.getElementById("dash-promos");
  host.innerHTML = '<div class="loading">טוען…</div>';
  try {
    const rows = await api("/api/stats/recent-promotions", { limit: 25, ...prefsQuery({ includeChain: true }) });
    if (!rows.length) { host.innerHTML = '<div class="empty">אין מבצעים עדכניים למיקום הנבחר</div>'; return; }
    host.innerHTML = rows.map(r => {
      const disc = r.discount_price != null
        ? shekel(r.discount_price)
        : r.discount_rate != null ? `${Number(r.discount_rate).toFixed(0)}% הנחה` : "";
      return `
        <div class="dash-row">
          <div class="dash-row-main">
            <div class="nm">${escapeHtml(r.description || r.promo_code || "—")}</div>
            <div class="sub">${escapeHtml(r.chain_name_he)} · ${fmtNum(r.items)} פריטים · ${fmtTs(r.starts_at)}${r.ends_at ? " – " + fmtTs(r.ends_at) : ""}</div>
          </div>
          <div class="price min">${escapeHtml(disc)}</div>
        </div>`;
    }).join("");
  } catch (e) {
    renderError(host, e, loadPromos);
  }
}

// ---------- cities (scoped by location + preferred chains) ----------
async function loadCities() {
  const host = document.getElementById("dash-cities");
  host.innerHTML = '<div class="loading">טוען…</div>';
  try {
    const rows = await api("/api/stats/cities", { limit: 25, ...prefsQuery({ includeChain: true }) });
    if (!rows.length) { host.innerHTML = '<div class="empty">אין נתוני ערים</div>'; return; }
    const max = Math.max(...rows.map(r => r.stores));
    host.innerHTML = rows.map(r => `
      <div class="dash-row">
        <div class="dash-row-main">
          <div class="nm">${escapeHtml(r.city)}</div>
          <div class="sub">${fmtNum(r.chains)} רשתות</div>
          <div class="bar-mini"><span style="width:${(r.stores / max * 100).toFixed(1)}%"></span></div>
        </div>
        <div class="count">${fmtNum(r.stores)}</div>
      </div>
    `).join("");
  } catch (e) {
    renderError(host, e, loadCities);
  }
}

// ---------- run history (unscoped; ops view) ----------
async function loadRuns() {
  const host = document.getElementById("dash-runs");
  host.innerHTML = '<div class="loading">טוען…</div>';
  try {
    const rows = await api("/api/stats/scrape-runs", { limit: 60 });
    if (!rows.length) { host.innerHTML = '<div class="empty">אין ריצות</div>'; return; }
    const body = rows.map(r => `
      <tr>
        <td>${escapeHtml(r.chain_name_he)}</td>
        <td>${fmtTs(r.started_at)}</td>
        <td>${fmtTs(r.finished_at)}</td>
        <td>${statusPill(r.status)}</td>
        <td class="num">${fmtNum(r.files_ok)}</td>
        <td class="num">${fmtNum(r.files_failed)}</td>
        <td class="num">${fmtNum(r.rows_written)}</td>
      </tr>
    `).join("");
    host.innerHTML = `
      <table class="dash-runs-table">
        <thead><tr>
          <th>רשת</th><th>התחלה</th><th>סיום</th><th>סטטוס</th>
          <th>קבצים</th><th>כשלים</th><th>שורות</th>
        </tr></thead>
        <tbody>${body}</tbody>
      </table>`;
  } catch (e) {
    renderError(host, e, loadRuns);
  }
}

// ---------- retailers status (per-chain ingest health) ----------

async function loadRetailersStatus() {
  const host = document.getElementById("dash-retailers");
  if (!host) return;
  try {
    const rows = await api("/api/stats/retailers-status");
    const totalChains = rows.length;
    const fresh = rows.filter(r => r.status === "fresh").length;
    const stale = rows.filter(r => r.status === "stale").length;
    const gone  = rows.filter(r => r.status === "gone").length;
    const never = rows.filter(r => r.status === "never").length;
    const totMissingStores   = rows.reduce((a, r) => a + (r.stores_missing   || 0), 0);
    const totMissingProducts = rows.reduce((a, r) => a + (r.products_missing || 0), 0);

    const statusIcon = s => ({
      fresh: '<span class="pill ok">🟢 היום</span>',
      stale: '<span class="pill warn">🟡 ישן</span>',
      gone:  '<span class="pill err">🔴 פג</span>',
      never: '<span class="pill err">⚫ אין</span>',
    })[s] || s;

    const healthIcon = h => ({
      ok:      '<span class="pill ok">✅ תקין</span>',
      partial: '<span class="pill warn">🟡 חלקי</span>',
      thin:    '<span class="pill warn">⚠️ דליל</span>',
      dead:    '<span class="pill err">💀 מת</span>',
    })[h] || h;

    let html = `
      <div class="ret-summary">
        <span>סה"כ <b>${totalChains}</b></span>
        <span>· היום <b class="ok">${fresh}</b></span>
        <span>· ישן <b class="warn">${stale}</b></span>
        <span>· פג רטנציה <b class="err">${gone}</b></span>
        <span>· מעולם לא <b class="err">${never}</b></span>
        <span>· חנויות חסרות <b>${fmtNum(totMissingStores)}</b></span>
        <span>· מוצרים חסרים <b>${fmtNum(totMissingProducts)}</b></span>
      </div>
      <table class="dash-retailers-table">
        <thead><tr>
          <th>רשת</th>
          <th>סטטוס</th>
          <th>בריאות</th>
          <th title="חנויות במאגר / מתוכן עם מחירים נוכחיים">חנויות (יש/חסר)</th>
          <th title="מוצרים שיש להם מחיר נוכחי / מוצרים שנצפו אי-פעם בקבצי הרשת">מוצרים (יש/חסר)</th>
          <th>מחירים נוכחיים</th>
          <th>סריקה אחרונה</th>
          <th></th>
        </tr></thead>
        <tbody>`;
    for (const r of rows) {
      const ageTxt = r.days_stale == null ? "—" :
        (r.days_stale === 0 ? "היום" : `לפני ${r.days_stale} ימים`);
      const stHas = r.stores_with_data || 0, stTot = r.stores_total || 0;
      const stMiss = r.stores_missing || 0;
      const prHas = r.products_priced || 0;
      const prMiss = r.products_missing || 0;
      const prTot = r.products_observed || 0;
      html += `
        <tr class="ret-row" data-code="${escapeHtml(r.code)}" tabindex="0">
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
          <td><button class="btn-small" type="button" data-action="view-stores">צפה בחנויות</button></td>
        </tr>`;
    }
    html += "</tbody></table>";
    host.innerHTML = html;

    // Click on the row OR the button → open the per-retailer stores modal.
    host.querySelectorAll(".ret-row").forEach(row => {
      const open = () => openChainStoresModal(row.dataset.code);
      row.addEventListener("click", e => {
        if (e.target.closest("button[data-action='view-stores']")) {
          e.preventDefault(); open(); return;
        }
        // ignore clicks on cells with no special meaning — only the button
        // opens the modal, but pressing Enter/Space on the row also works.
      });
      row.addEventListener("keydown", e => {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); }
      });
    });
  } catch (e) {
    renderError(host, e, loadRetailersStatus);
  }
}

// ---------- per-retailer stores modal ----------

async function openChainStoresModal(code) {
  const modal   = document.getElementById("modal");
  const title   = document.getElementById("modal-title");
  const content = document.getElementById("modal-content");
  if (!modal || !title || !content) return;

  title.textContent = "טוען חנויות…";
  content.innerHTML = '<div class="empty">טוען…</div>';
  modal.classList.remove("hidden");
  modal.hidden = false;
  // close on backdrop click / × button (handled in product.js initModal already)
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
          <thead><tr>
            <th>קוד</th><th>שם</th><th>עיר</th><th>כתובת</th>
            <th>מחירים</th><th>עודכן</th>
          </tr></thead>
          <tbody>`;
      for (const s of j.stores) {
        const cls = (s.prices || 0) > 0 ? "store-ok" : "store-empty";
        body += `
          <tr class="${cls}">
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

// ---------- orchestration ----------

// Location-aware cards — re-fetch whenever prefs change.
function loadLocationAware() {
  loadKPIs();     // global counts when unscoped; loadChains overwrites when scoped
  loadChains();
  loadSpread();
  loadPromos();
  loadCities();   // cities ranking within the active scope
}

// Ops-only cards — fetched once per session, independent of user prefs.
function loadStatic() {
  loadRuns();
}

async function loadAll() {
  if (!loaded) {
    loaded = true;
    loadStatic();
  }
  loadLocationAware();
}

export function initDashboard() {
  initLocBar();
  // Re-run location-aware cards whenever prefs change, but only when the
  // dashboard has already been opened at least once (otherwise we'd fetch
  // before the user has seen the tab).
  onPrefsChange(() => {
    syncLocBarFromPrefs();
    if (loaded && current().tab === "dashboard") loadLocationAware();
  });

  const { tab } = current();
  if (tab === "dashboard") loadAll();
}

export function onRouted(state) {
  if (state.tab !== "dashboard") return;
  loadAll();
  setTimeout(() => {
    const host = document.getElementById("dash-chain-chart");
    if (chainPlot && host) chainPlot.setSize({ width: Math.min(host.clientWidth || 720, 1100), height: 320 });
  }, 50);
}

export function refreshDashboard() {
  loaded = false;
  loadAll();
}
