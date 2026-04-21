// Search tab: debounced live search + keyboard navigation + hash sync.

import { api } from "./api.js";
import { debounce, renderRows, renderError, toast } from "./ui.js";
import { openProduct } from "./product.js";
import { current, setRoute } from "./router.js";

const root   = () => document.getElementById("results-search");
const form   = () => document.getElementById("form-search");
const qEl    = () => form().elements.q;
const chainEl = () => form().elements.chain;
const cityEl  = () => form().elements.city;

let lastToken = 0;

async function runSearch(push = true) {
  const q     = qEl().value.trim();
  const chain = chainEl().value;
  const city  = cityEl().value.trim();
  const r = root();

  if (push) setRoute("search", { q, chain, city });
  if (!q) { r.innerHTML = '<div class="empty">הקלד שם מוצר או ברקוד</div>'; return; }

  const token = ++lastToken;
  r.innerHTML = '<div class="loading">מחפש…</div>';
  try {
    const rows = await api("/api/search", { q, chain, city, limit: 40 });
    if (token !== lastToken) return; // stale response, ignore
    renderRows(r, rows, { onOpen: openProduct });
  } catch (err) {
    if (token !== lastToken) return;
    renderError(r, err, () => runSearch(false));
    if (err.network) toast(err.detail, "err");
  }
}

const debounced = debounce(() => runSearch(true), 250);

export function initSearch() {
  form().addEventListener("submit", e => { e.preventDefault(); runSearch(true); });
  qEl().addEventListener("input", debounced);
  chainEl().addEventListener("change", () => runSearch(true));
  cityEl().addEventListener("input", debounced);

  // Restore state from URL.
  const { tab, params } = current();
  if (tab === "search") applyParams(params);
}

export function onRouted(state) {
  if (state.tab !== "search") return;
  applyParams(state.params);
}

function applyParams(params) {
  const q = params.get("q") || "";
  const chain = params.get("chain") || "";
  const city = params.get("city") || "";
  if (qEl().value !== q) qEl().value = q;
  if (chainEl().value !== chain) chainEl().value = chain;
  if (cityEl().value !== city) cityEl().value = city;
  if (q) runSearch(false);
}

// Chain dropdown bootstrap.
export async function loadChains() {
  try {
    const list = await api("/api/chains");
    const sel = chainEl();
    const keep = sel.value;
    for (const c of list.filter(x => x.active)) {
      const o = document.createElement("option");
      o.value = c.code; o.textContent = c.name_he;
      sel.appendChild(o);
    }
    if (keep) sel.value = keep;
  } catch (e) {
    console.warn("chains load failed", e);
  }
}
