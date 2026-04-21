// Product detail modal: opens via openProduct(barcode).
// Accessibility: focus trap, Escape closes, returns focus to opener.

import { api } from "./api.js";
import { shekel, escapeHtml, renderError } from "./ui.js";

const modal    = () => document.getElementById("modal");
const content  = () => document.getElementById("modal-content");
const titleEl  = () => document.getElementById("modal-title");
const closeBtn = () => document.getElementById("modal-close");

let lastFocus = null;

function focusableInModal() {
  return [...modal().querySelectorAll('button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])')]
    .filter(el => !el.disabled);
}

function onKey(e) {
  if (e.key === "Escape") { e.preventDefault(); closeModal(); return; }
  if (e.key !== "Tab") return;
  const items = focusableInModal();
  if (!items.length) return;
  const first = items[0], last = items[items.length - 1];
  if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
  else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
}

export function closeModal() {
  modal().classList.add("hidden");
  modal().setAttribute("hidden", "");
  document.removeEventListener("keydown", onKey);
  if (lastFocus) { try { lastFocus.focus(); } catch { /* element may be gone */ } }
}

export function initModal() {
  closeBtn().addEventListener("click", closeModal);
  modal().addEventListener("click", e => {
    if (e.target === modal()) closeModal();
  });
}

export async function openProduct(barcode) {
  lastFocus = document.activeElement;
  const m = modal();
  m.classList.remove("hidden");
  m.removeAttribute("hidden");
  titleEl().textContent = "טוען…";
  content().innerHTML = '<div class="loading">…</div>';
  document.addEventListener("keydown", onKey);
  closeBtn().focus();

  try {
    const [p, cmp, promos] = await Promise.all([
      api(`/api/products/${encodeURIComponent(barcode)}`),
      api(`/api/compare/${encodeURIComponent(barcode)}`),
      api(`/api/promotions/${encodeURIComponent(barcode)}`).catch(() => []),
    ]);
    titleEl().textContent = `${p.name || barcode}  ·  ${barcode}`;
    content().innerHTML = render(p, cmp, promos);
  } catch (e) {
    renderError(content(), e, () => openProduct(barcode));
  }
}

function render(p, cmp, promos) {
  if (!cmp.length) return '<div class="empty">אין מחירים</div>';

  const cheapest = cmp[0];
  let html = '<h3 style="margin:10px 0 6px">השוואה בין רשתות</h3>';
  html += '<table><thead><tr><th>רשת</th><th>הכי זול</th><th>חנויות</th></tr></thead><tbody>';
  for (const r of cmp) {
    const cls = r.chain_code === cheapest.chain_code ? "cheapest" : "";
    html += `<tr>
      <td>${escapeHtml(r.chain_name_he)}</td>
      <td class="num ${cls}">${shekel(r.min_price)}</td>
      <td class="num">${r.stores_with}</td>
    </tr>`;
  }
  html += "</tbody></table>";

  if (promos && promos.length) {
    html += `<h3 style="margin:16px 0 6px">מבצעים פעילים (${promos.length})</h3>
      <table><thead><tr><th>רשת</th><th>תיאור</th><th>מינ׳</th><th>הנחה</th><th>עד</th></tr></thead><tbody>`;
    for (const pr of promos.slice(0, 20)) {
      const disc = pr.discount_price != null
        ? shekel(pr.discount_price)
        : (pr.discount_rate != null ? pr.discount_rate + "%" : "—");
      html += `<tr>
        <td>${escapeHtml(pr.chain_name_he)}</td>
        <td>${escapeHtml(pr.description || "")}</td>
        <td class="num">${pr.min_qty ?? ""}</td>
        <td class="num">${escapeHtml(disc)}</td>
        <td class="num">${escapeHtml((pr.ends_at || "").slice(0, 10))}</td>
      </tr>`;
    }
    html += "</tbody></table>";
  }

  html += '<h3 style="margin:16px 0 6px">כל החנויות</h3>';
  html += '<table><thead><tr><th>רשת</th><th>חנות</th><th>עיר</th><th>מחיר</th><th>עדכון</th></tr></thead><tbody>';
  for (const r of (p.prices || []).slice(0, 80)) {
    html += `<tr>
      <td>${escapeHtml(r.chain_name_he)}</td>
      <td>${escapeHtml(r.store_name ?? r.store_id)}</td>
      <td>${escapeHtml(r.store_city || "")}</td>
      <td class="num">${shekel(r.price)}</td>
      <td class="num">${escapeHtml((r.updated_at || "").slice(0, 16))}</td>
    </tr>`;
  }
  html += "</tbody></table>";
  return html;
}
