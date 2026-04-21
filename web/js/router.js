// Hash router: #search, #nl, #trends?bc=7290..., #receipt
// Separates the tab id from per-tab query params — tabs can save/restore state.

const VALID = new Set(["search", "nl", "trends", "receipt"]);

function parse() {
  const h = location.hash.replace(/^#/, "");
  const [tab, qs] = h.split("?");
  const id = VALID.has(tab) ? tab : "search";
  const params = new URLSearchParams(qs || "");
  return { tab: id, params };
}

const listeners = new Set();
export function onRoute(fn) {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

function emit() {
  const state = parse();
  listeners.forEach(fn => {
    try { fn(state); } catch (e) { console.error("router listener", e); }
  });
}

window.addEventListener("hashchange", emit);

export function current() { return parse(); }

// Set tab + optional params. Preserves scroll position.
export function setRoute(tab, params = {}) {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== "" && v != null) qs.set(k, v);
  }
  const s = qs.toString();
  location.hash = s ? `${tab}?${s}` : tab;
}

// Fire once on startup so tabs sync on load.
export function bootstrap() { emit(); }
