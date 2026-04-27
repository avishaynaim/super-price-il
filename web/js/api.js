// Thin fetch wrapper.  On localhost → FastAPI.  On any other host → Supabase REST.
import { SUPABASE_URL, SUPABASE_KEY, USE_SUPABASE } from "./config.js";

// ── Supabase REST helpers ──────────────────────────────────────────────────

const SB_HDR = () => ({
  "apikey": SUPABASE_KEY,
  "Authorization": "Bearer " + SUPABASE_KEY,
  "Content-Type": "application/json",
  "Prefer": "return=representation",
});

async function sbRpc(fn, params = {}) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${fn}`, {
    method: "POST",
    headers: SB_HDR(),
    body: JSON.stringify(params),
  });
  if (!r.ok) {
    let msg = `Supabase RPC ${fn} → HTTP ${r.status}`;
    try { const j = await r.json(); msg = j.message || j.hint || msg; } catch {}
    throw { detail: msg, status: r.status };
  }
  return r.json();
}

async function sbGet(table, query = {}) {
  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
  for (const [k, v] of Object.entries(query)) url.searchParams.set(k, v);
  const r = await fetch(url, { headers: SB_HDR() });
  if (!r.ok) throw { detail: `Supabase GET ${table} → HTTP ${r.status}`, status: r.status };
  return r.json();
}

async function sbCount(table, query = {}) {
  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
  url.searchParams.set("select", "id");
  for (const [k, v] of Object.entries(query)) url.searchParams.set(k, v);
  const r = await fetch(url, {
    method: "HEAD",
    headers: { ...SB_HDR(), "Prefer": "count=exact" },
  });
  const cr = r.headers.get("content-range") || "";
  return parseInt(cr.split("/")[1] || "0", 10);
}

// ── Supabase-backed API surface ────────────────────────────────────────────

async function sbDispatch(path, query = {}, init = {}) {
  // POST endpoints (NL, receipts) — not available without the FastAPI server
  if (init.method === "POST") {
    throw { detail: "תכונה זו זמינה רק בשרת המקומי", status: 503 };
  }

  // GET /api/health
  if (path === "/api/health") {
    const [chains_active, stores, products, current_prices] = await Promise.all([
      sbCount("chains", { "active": "eq.true" }),
      sbCount("stores"),
      sbCount("products"),
      sbCount("current_prices"),
    ]);
    return { status: "ok", chains_active, stores, products, current_prices };
  }

  // GET /api/chains
  if (path === "/api/chains") {
    return sbGet("chains", {
      "active": "eq.true",
      "select": "code,name_he,name_en,portal_url,active",
      "order": "name_he",
    });
  }

  // GET /api/search
  if (path === "/api/search") {
    return sbRpc("search_products", {
      q:            query.q || "",
      chain_codes:  query.chain ? [query.chain] : (query.chains ? query.chains.split(",") : null),
      city_spellings: query.city ? [query.city] : null,
      limit_n:      parseInt(query.limit) || 50,
    });
  }

  // GET /api/products/:barcode
  const prodM = path.match(/^\/api\/products\/(.+)$/);
  if (prodM) {
    const barcode = prodM[1];
    const [prods, prices] = await Promise.all([
      sbGet("products", { "barcode": `eq.${barcode}`, "select": "barcode,name,manufacturer,unit_qty,unit_type" }),
      sbRpc("get_product_prices", {
        p_barcode:     barcode,
        chain_codes:   query.chains ? query.chains.split(",") : null,
        city_spellings: query.city ? [query.city] : null,
      }),
    ]);
    if (!prods.length) throw { detail: "product not found", status: 404 };
    return { ...prods[0], prices };
  }

  // GET /api/compare/:barcode
  const cmpM = path.match(/^\/api\/compare\/(.+)$/);
  if (cmpM) {
    return sbRpc("compare_product", {
      p_barcode:     cmpM[1],
      chain_codes:   query.chains ? query.chains.split(",") : null,
      city_spellings: query.city ? [query.city] : null,
    });
  }

  // GET /api/trends/:barcode — no history in current schema
  if (path.startsWith("/api/trends/")) return [];

  // GET /api/promotions/:barcode
  if (path.startsWith("/api/promotions/")) return [];

  // GET /api/stats/chains
  if (path === "/api/stats/chains") return sbRpc("chain_coverage_stats");

  // GET /api/stats/top-spread
  if (path === "/api/stats/top-spread") {
    return sbRpc("top_price_spread", {
      city_spellings: query.city ? [query.city] : null,
      chain_codes:    query.chains ? query.chains.split(",") : null,
      limit_n:        parseInt(query.limit) || 20,
    });
  }

  // GET /api/stats/retailers-status
  if (path === "/api/stats/retailers-status") return sbRpc("retailers_status");

  // GET /api/stats/scrape-runs
  if (path === "/api/stats/scrape-runs") {
    const rows = await sbGet("scrape_runs", {
      "select": "id,chain_id,started_at,finished_at,status,files_ok,files_failed,files_total,rows_written,error_msg",
      "order":  "started_at.desc",
      "limit":  query.limit || "60",
    });
    // attach chain codes from chains table
    const chainRows = await sbGet("chains", { "select": "id,code,name_he" });
    const chainMap = {};
    for (const c of chainRows) chainMap[c.id] = c;
    return rows.map(r => ({
      ...r,
      chain_code:    chainMap[r.chain_id]?.code    || "",
      chain_name_he: chainMap[r.chain_id]?.name_he || "",
    }));
  }

  // GET /api/stats/recent-promotions
  if (path === "/api/stats/recent-promotions") return [];

  // GET /api/stats/cities
  if (path === "/api/stats/cities") {
    const rows = await sbGet("stores", { "select": "city", "not.city": "is.null", "limit": "5000" });
    const counts = {};
    for (const r of rows) {
      const c = (r.city || "").trim();
      if (c) counts[c] = (counts[c] || 0) + 1;
    }
    return Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 50)
      .map(([city, stores]) => ({ city, stores }));
  }

  // GET /api/cities  (geo module)
  if (path === "/api/cities") {
    const rows = await sbGet("stores", { "select": "city", "limit": "5000" });
    const counts = {};
    for (const r of rows) {
      const c = (r.city || "").trim();
      if (c) counts[c] = (counts[c] || 0) + 1;
    }
    return Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .map(([name_he, store_count]) => ({ name_he, store_count }));
  }

  // GET /api/stores
  if (path === "/api/stores") {
    const q = {
      "select": "id,store_code,name,city,address,chain_id",
      "limit":  query.limit || "500",
    };
    if (query.city) q["city"] = `ilike.*${query.city}*`;
    return sbGet("stores", q);
  }

  throw { detail: `לא נמצא: ${path}`, status: 404 };
}

// ── Public helpers ─────────────────────────────────────────────────────────

export async function api(path, query = {}, init = {}) {
  if (USE_SUPABASE) return sbDispatch(path, query, init);

  // Local FastAPI path
  const url = new URL(path, location.origin);
  for (const [k, v] of Object.entries(query)) {
    if (v !== "" && v != null) url.searchParams.set(k, v);
  }
  let resp;
  try {
    resp = await fetch(url, init);
  } catch (e) {
    throw { detail: navigator.onLine ? "שגיאת רשת" : "אין חיבור לאינטרנט", network: true };
  }
  let body = null;
  try { body = await resp.json(); } catch { /* non-JSON body */ }
  if (!resp.ok) {
    throw { detail: (body && body.detail) || `HTTP ${resp.status}`, status: resp.status };
  }
  return body;
}

// Upload with XHR (only works with local FastAPI server).
export function upload(path, formData, onProgress) {
  if (USE_SUPABASE) {
    return Promise.reject({ detail: "העלאת קבלות זמינה רק בשרת המקומי", status: 503 });
  }
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", path);
    xhr.upload.onprogress = e => {
      if (e.lengthComputable && onProgress) onProgress(e.loaded / e.total);
    };
    xhr.onload = () => {
      let j = null;
      try { j = JSON.parse(xhr.responseText); } catch { /* leave null */ }
      if (xhr.status >= 200 && xhr.status < 300) resolve(j);
      else reject({ detail: (j && j.detail) || `HTTP ${xhr.status}`, status: xhr.status });
    };
    xhr.onerror = () => reject({ detail: "שגיאת רשת", network: true });
    xhr.send(formData);
  });
}
