// Entry point. Wires tabs to the router, loads health/chains, initializes
// each tab module. All tab state lives in the URL hash.

import { api } from "./api.js";
import { toast } from "./ui.js";
import { onRoute, current, setRoute, bootstrap } from "./router.js";
import { initModal } from "./product.js";
import { initSearch, loadChains, onRouted as onSearchRoute } from "./search.js";
import { initNL,      onRouted as onNLRoute }     from "./nl.js";
import { initTrends,  onRouted as onTrendsRoute } from "./trends.js";
import { initReceipt } from "./receipt.js";

// ---- tabs ----
const TABS = ["search", "nl", "trends", "receipt"];
function activateTab(id) {
  for (const t of TABS) {
    const btn  = document.querySelector(`.tab[data-tab="${t}"]`);
    const pane = document.getElementById("pane-" + t);
    const on = t === id;
    btn?.classList.toggle("active", on);
    btn?.setAttribute("aria-selected", String(on));
    if (pane) {
      pane.classList.toggle("active", on);
      pane.hidden = !on;
    }
  }
}

document.querySelectorAll(".tab").forEach(t => {
  t.addEventListener("click", () => setRoute(t.dataset.tab));
});

// keyboard nav between tabs (Left/Right arrows)
document.querySelector(".tabs")?.addEventListener("keydown", e => {
  const btns = [...document.querySelectorAll(".tab")];
  const idx = btns.indexOf(document.activeElement);
  if (idx < 0) return;
  let next = idx;
  if (e.key === "ArrowRight") next = (idx + 1) % btns.length;
  else if (e.key === "ArrowLeft") next = (idx - 1 + btns.length) % btns.length;
  else if (e.key === "Home") next = 0;
  else if (e.key === "End") next = btns.length - 1;
  if (next !== idx) { e.preventDefault(); btns[next].focus(); btns[next].click(); }
});

// ---- health/chains ----
function loadHealth() {
  api("/api/health").then(h => {
    document.getElementById("health").textContent =
      `חיבור: ${h.status} · רשתות פעילות ${h.chains_active} · חנויות ${h.stores} ` +
      `· מוצרים ${h.products.toLocaleString()} · מחירים נוכחיים ${h.current_prices.toLocaleString()}`;
  }).catch(() => {
    document.getElementById("health").textContent = "health err";
  });
}

// ---- offline detection ----
function updateOnline() {
  const banner = document.getElementById("offline");
  if (navigator.onLine) {
    banner.hidden = true;
  } else {
    banner.hidden = false;
    toast("החיבור נפל — חלק מהפעולות לא יעבדו", "warn", 3200);
  }
}
window.addEventListener("online",  () => { updateOnline(); toast("החיבור חזר", "ok"); loadHealth(); });
window.addEventListener("offline", updateOnline);

// ---- global errors → toasts (so unhandled rejections surface) ----
window.addEventListener("unhandledrejection", e => {
  const msg = e.reason?.detail || e.reason?.message || "שגיאה לא טופלה";
  console.error("unhandled", e.reason);
  toast(msg, "err");
});

// ---- init ----
loadHealth();
loadChains();
initModal();
initSearch();
initNL();
initTrends();
initReceipt();

onRoute(state => {
  activateTab(state.tab);
  onSearchRoute(state);
  onNLRoute(state);
  onTrendsRoute(state);
});

bootstrap();
updateOnline();

// default to search tab if no hash
if (!location.hash) setRoute("search");
