// Backend config.
// On localhost → FastAPI (/api/*).  On any other host (e.g. GitHub Pages) → Supabase REST.
export const SUPABASE_URL = "https://axdluubyohjrfjqxgpft.supabase.co";
export const SUPABASE_KEY = "sb_publishable_exsa07S_Mna-E50bAMKDMQ_HMQ1bQNP";

const h = location.hostname;
export const USE_SUPABASE = h !== "localhost" && h !== "127.0.0.1" && h !== "0.0.0.0" && h !== "";
