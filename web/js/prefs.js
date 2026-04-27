// Per-user preferences persisted to localStorage.
//
// Stored shape (JSON in "superprice:prefs"):
//   {
//     city:            "תל אביב-יפו" | null,   // canonical Hebrew name
//     coords:          { lat: 32.08, lng: 34.78 } | null,   // only set when geolocated
//     radius_km:       0,                      // 0 = off
//     preferred_chains:["shufersal","rami_levi"],
//   }
//
// Any subscriber registered via onPrefsChange() is called with the new prefs
// object whenever anything is set.

const KEY = "superprice:prefs";

const DEFAULTS = Object.freeze({
  city:             null,
  coords:           null,
  radius_km:        0,
  preferred_chains: [],
});

let cached = null;

function readRaw() {
  try {
    const s = localStorage.getItem(KEY);
    if (!s) return null;
    const p = JSON.parse(s);
    return (p && typeof p === "object") ? p : null;
  } catch { return null; }
}

export function getPrefs() {
  if (cached) return cached;
  cached = { ...DEFAULTS, ...(readRaw() || {}) };
  // normalize
  if (!Array.isArray(cached.preferred_chains)) cached.preferred_chains = [];
  if (typeof cached.radius_km !== "number" || cached.radius_km < 0) cached.radius_km = 0;
  return cached;
}

const listeners = new Set();
export function onPrefsChange(fn) {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

function emit() {
  const p = getPrefs();
  listeners.forEach(fn => { try { fn(p); } catch (e) { console.error("prefs listener", e); } });
}

export function setPrefs(patch) {
  cached = { ...getPrefs(), ...patch };
  try { localStorage.setItem(KEY, JSON.stringify(cached)); } catch { /* quota etc. */ }
  emit();
  return cached;
}

export function clearPrefs() {
  cached = { ...DEFAULTS };
  try { localStorage.removeItem(KEY); } catch { /* ignore */ }
  emit();
}

// Query-string fragment to append to API calls whose endpoints accept
// city/lat/lng/radius_km/chain filters. Coords are only sent if radius is
// also set (no point).
export function prefsQuery({ includeCity = true, includeChain = false } = {}) {
  const p = getPrefs();
  const q = {};
  if (includeCity && p.city) q.city = p.city;
  if (p.coords && p.radius_km > 0) {
    q.lat = p.coords.lat;
    q.lng = p.coords.lng;
    q.radius_km = p.radius_km;
    // when we have coords+radius, drop city (radius is the stricter filter)
    delete q.city;
  }
  if (includeChain && Array.isArray(p.preferred_chains) && p.preferred_chains.length > 0) {
    q.chains = p.preferred_chains.join(",");
  }
  return q;
}

// Cross-tab sync (another tab writes → we update)
window.addEventListener("storage", e => {
  if (e.key !== KEY) return;
  cached = null;
  emit();
});
