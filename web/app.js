// super-price-il vanilla JS UI. No build step; served directly by FastAPI.
const api = (p, q = {}) => {
  const u = new URL(p, location.origin);
  for (const [k, v] of Object.entries(q)) if (v !== "" && v != null) u.searchParams.set(k, v);
  return fetch(u).then(r => r.ok ? r.json() : r.json().then(j => Promise.reject(j)));
};
const shekel = n => n == null ? "—" : "₪" + Number(n).toFixed(2);

// -- tabs --
document.querySelectorAll(".tab").forEach(t => t.addEventListener("click", () => {
  document.querySelectorAll(".tab").forEach(x => x.classList.remove("active"));
  document.querySelectorAll(".pane").forEach(x => x.classList.remove("active"));
  t.classList.add("active");
  document.getElementById("pane-" + t.dataset.tab).classList.add("active");
}));

// -- health + chains bootstrap --
api("/api/health").then(h => {
  document.getElementById("health").textContent =
    `חיבור: ${h.status} · רשתות פעילות ${h.chains_active} · חנויות ${h.stores} · מוצרים ${h.products.toLocaleString()} · מחירים נוכחיים ${h.current_prices.toLocaleString()}`;
}).catch(e => document.getElementById("health").textContent = "health err");

api("/api/chains").then(list => {
  const sel = document.querySelector('#form-search select[name="chain"]');
  list.filter(c => c.active).forEach(c => {
    const o = document.createElement("option");
    o.value = c.code; o.textContent = c.name_he;
    sel.appendChild(o);
  });
});

// -- result row renderer --
function renderRows(root, rows, options = {}) {
  root.innerHTML = "";
  if (!rows.length) { root.innerHTML = '<div class="empty">אין תוצאות</div>'; return; }
  rows.forEach(r => {
    const d = document.createElement("div");
    d.className = "row";
    const promoBadge = r.has_promo ? '<span class="promo-badge" title="במבצע">%</span>' : "";
    d.innerHTML = `
      <div>
        <div class="nm">${r.name || "—"} ${promoBadge}</div>
        <div class="mf">${r.manufacturer || ""} <span class="bc">${r.barcode}</span></div>
      </div>
      <div class="price min">${shekel(r.min_price)}</div>
      <div class="price max">${shekel(r.max_price)}</div>
      <div class="chains">${r.chains_with_price ?? ""}</div>
      <div><button class="btn" type="button">השוואה</button></div>
    `;
    d.addEventListener("click", () => openProduct(r.barcode));
    root.appendChild(d);
  });
}

// -- search --
document.getElementById("form-search").addEventListener("submit", e => {
  e.preventDefault();
  const f = e.target;
  const params = {
    q: f.q.value.trim(), chain: f.chain.value, city: f.city.value.trim(), limit: 40,
  };
  const root = document.getElementById("results-search");
  if (!params.q) { root.innerHTML = '<div class="empty">הקלד שם מוצר או ברקוד</div>'; return; }
  root.innerHTML = '<div class="loading">מחפש…</div>';
  api("/api/search", params).then(rows => renderRows(root, rows))
    .catch(err => root.innerHTML = `<div class="err">${err.detail || err}</div>`);
});

// -- NL --
document.getElementById("form-nl").addEventListener("submit", e => {
  e.preventDefault();
  const q = e.target.query.value.trim();
  const expl = document.getElementById("nl-explain");
  const tool = document.getElementById("nl-tool");
  const root = document.getElementById("results-nl");
  expl.textContent = ""; tool.textContent = "";
  root.innerHTML = '<div class="loading">שואל את Claude…</div>';
  fetch("/api/nl-filter", {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({ query: q }),
  }).then(r => r.json()).then(resp => {
    if (resp.detail) { root.innerHTML = `<div class="err">${resp.detail}</div>`; return; }
    if (resp.explanation) expl.textContent = resp.explanation;
    tool.textContent = "filter → " + JSON.stringify(resp.tool_call?.input || {}, null, 0);
    renderRows(root, resp.rows || []);
  }).catch(err => root.innerHTML = `<div class="err">${err.message || err}</div>`);
});

// -- trends --
document.getElementById("form-trends").addEventListener("submit", e => {
  e.preventDefault();
  const f = e.target;
  const bc = f.barcode.value.trim();
  const days = f.days.value;
  const root = document.getElementById("trend-table");
  if (!bc) { root.innerHTML = '<div class="empty">הזן ברקוד</div>'; return; }
  root.innerHTML = '<div class="loading">טוען…</div>';
  api(`/api/trends/${encodeURIComponent(bc)}`, { days }).then(points => {
    if (!points.length) { root.innerHTML = '<div class="empty">אין היסטוריה</div>'; return; }
    const byChain = {};
    points.forEach(p => (byChain[p.chain_code] ||= { name: p.chain_name_he, pts: [] }).pts.push(p));
    let html = "<table><thead><tr><th>רשת</th><th>נקודות</th><th>טווח</th><th>גרף</th></tr></thead><tbody>";
    for (const [code, g] of Object.entries(byChain)) {
      const prices = g.pts.map(p => p.price);
      const lo = Math.min(...prices), hi = Math.max(...prices);
      const poly = sparkline(g.pts.map(p => p.price));
      html += `<tr><td>${g.name}</td><td class="num">${g.pts.length}</td>
        <td class="num">${shekel(lo)} – ${shekel(hi)}</td>
        <td>${poly}</td></tr>`;
    }
    html += "</tbody></table>";
    root.innerHTML = html;
  }).catch(e => root.innerHTML = `<div class="err">${e.detail || e}</div>`);
});

function sparkline(vals) {
  if (vals.length < 2) return "";
  const w = 180, h = 28, lo = Math.min(...vals), hi = Math.max(...vals);
  const span = hi - lo || 1;
  const pts = vals.map((v, i) => {
    const x = (i / (vals.length - 1)) * (w - 4) + 2;
    const y = h - 2 - ((v - lo) / span) * (h - 4);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return `<svg class="spark" viewBox="0 0 ${w} ${h}"><polyline points="${pts}"/></svg>`;
}

// -- product modal (per-chain cheapest) --
function openProduct(barcode) {
  const m = document.getElementById("modal");
  const c = document.getElementById("modal-content");
  document.getElementById("modal-title").textContent = "טוען…";
  c.innerHTML = '<div class="loading">…</div>';
  m.classList.remove("hidden");

  Promise.all([
    api(`/api/products/${encodeURIComponent(barcode)}`),
    api(`/api/compare/${encodeURIComponent(barcode)}`),
    api(`/api/promotions/${encodeURIComponent(barcode)}`).catch(() => []),
  ]).then(([p, cmp, promos]) => {
    document.getElementById("modal-title").textContent = `${p.name || barcode}  ·  ${barcode}`;
    if (!cmp.length) { c.innerHTML = '<div class="empty">אין מחירים</div>'; return; }
    const cheapest = cmp[0];
    let html = '<h3 style="margin:10px 0 6px">השוואה בין רשתות</h3><table><thead><tr><th>רשת</th><th>הכי זול</th><th>חנויות</th></tr></thead><tbody>';
    for (const r of cmp) {
      const cls = r.chain_code === cheapest.chain_code ? "cheapest" : "";
      html += `<tr><td>${r.chain_name_he}</td><td class="num ${cls}">${shekel(r.min_price)}</td><td class="num">${r.stores_with}</td></tr>`;
    }
    html += "</tbody></table>";

    if (promos && promos.length) {
      html += `<h3 style="margin:16px 0 6px">מבצעים פעילים (${promos.length})</h3>
        <table><thead><tr><th>רשת</th><th>תיאור</th><th>מינ׳</th><th>הנחה</th><th>עד</th></tr></thead><tbody>`;
      for (const pr of promos.slice(0, 20)) {
        const disc = pr.discount_price != null ? shekel(pr.discount_price)
                   : (pr.discount_rate != null ? pr.discount_rate + "%" : "—");
        html += `<tr><td>${pr.chain_name_he}</td><td>${pr.description || ""}</td>
          <td class="num">${pr.min_qty ?? ""}</td>
          <td class="num">${disc}</td>
          <td class="num">${(pr.ends_at || "").slice(0, 10)}</td></tr>`;
      }
      html += "</tbody></table>";
    }

    html += '<h3 style="margin:16px 0 6px">כל החנויות</h3><table><thead><tr><th>רשת</th><th>חנות</th><th>עיר</th><th>מחיר</th><th>עדכון</th></tr></thead><tbody>';
    for (const r of p.prices.slice(0, 80)) {
      html += `<tr><td>${r.chain_name_he}</td><td>${r.store_name ?? r.store_id}</td><td>${r.store_city || ""}</td><td class="num">${shekel(r.price)}</td><td class="num">${r.updated_at?.slice(0,16) || ""}</td></tr>`;
    }
    html += "</tbody></table>";
    c.innerHTML = html;
  }).catch(e => c.innerHTML = `<div class="err">${e.detail || e}</div>`);
}
function closeModal() { document.getElementById("modal").classList.add("hidden"); }
document.getElementById("modal").addEventListener("click", e => {
  if (e.target.id === "modal") closeModal();
});

// -- receipt --
document.getElementById("form-receipt").addEventListener("submit", async e => {
  e.preventDefault();
  const fd = new FormData(e.target);
  const root = document.getElementById("receipt-result");
  root.innerHTML = '<div class="loading">מעלה ומעבד…</div>';
  try {
    const r = await fetch("/api/receipts", { method: "POST", body: fd });
    const j = await r.json();
    if (!r.ok) { root.innerHTML = `<div class="err">${j.detail || "שגיאה"}</div>`; return; }
    renderReceipt(root, j);
  } catch (err) {
    root.innerHTML = `<div class="err">${err.message || err}</div>`;
  }
});

function renderReceipt(root, j) {
  let html = `<h3 style="margin:10px 0">סיכום קבלה</h3>
    <div>פריטים זוהו: <b>${j.items.length}</b> · נטען מ־<b>${j.source}</b> · סך הכל מוערך: <b>${shekel(j.total_paid)}</b></div>`;
  if (j.alternatives?.length) {
    html += `<h3 style="margin:16px 0 6px">חלופות חסכוניות ${j.city ? `באזור "${j.city}"` : ""}</h3>
      <table><thead><tr><th>רשת</th><th>מחיר סל</th><th>הפרש</th></tr></thead><tbody>`;
    const base = j.total_paid;
    for (const a of j.alternatives) {
      const diff = base - a.basket_total;
      const cls = diff > 0 ? "cheapest" : "";
      html += `<tr><td>${a.chain_name_he}</td><td class="num">${shekel(a.basket_total)}</td><td class="num ${cls}">${diff > 0 ? "-" : "+"}${shekel(Math.abs(diff))}</td></tr>`;
    }
    html += "</tbody></table>";
  }
  html += `<h3 style="margin:16px 0 6px">פריטים</h3>
    <table><thead><tr><th>פריט</th><th>ברקוד</th><th>כמות</th><th>סה"כ</th><th>התאמה</th></tr></thead><tbody>`;
  for (const it of j.items) {
    html += `<tr><td>${it.name || it.raw_name || ""}</td><td class="num">${it.barcode || ""}</td><td class="num">${it.quantity ?? ""}</td><td class="num">${shekel(it.line_total)}</td><td class="num">${it.match_confidence != null ? (it.match_confidence * 100).toFixed(0) + "%" : ""}</td></tr>`;
  }
  html += "</tbody></table>";
  root.innerHTML = html;
}
