// Backend config.
// On localhost → FastAPI (/api/*).  On any other host (e.g. GitHub Pages) → Supabase REST.
export const SUPABASE_URL = "https://axdluubyohjrfjqxgpft.supabase.co";
export const SUPABASE_KEY = "sb_publishable_exsa07S_Mna-E50bAMKDMQ_HMQ1bQNP";

const h = location.hostname;
// Treat any private/loopback address as local FastAPI (not Supabase).
// This covers LAN access (192.168.x.x, 10.x.x.x) in addition to localhost.
const _isLocal = h === "" || h === "localhost" || h === "127.0.0.1" || h === "0.0.0.0"
  || /^192\.168\./.test(h) || /^10\./.test(h) || /^172\.(1[6-9]|2\d|3[01])\./.test(h)
  || /^100\.64\./.test(h)   // Tailscale / CGNAT range used in some Android setups
  || location.port === "8000"; // any host serving on port 8000 is our FastAPI
export const USE_SUPABASE = !_isLocal;
