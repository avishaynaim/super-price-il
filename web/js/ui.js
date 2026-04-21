// Shared UI helpers — formatters, debounce, toasts, rendering, error panels.

export const shekel = n => n == null || Number.isNaN(+n) ? "—" : "₪" + Number(n).toFixed(2);

export const escapeHtml = s => String(s ?? "")
  .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
  .replace(/"/g, "&quot;").replace(/'/g, "&#39;");

export function debounce(fn, ms) {
  let t;
  const wrapped = (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
  wrapped.cancel = () => clearTimeout(t);
  return wrapped;
}

// --- toasts ---
const stack = () => document.getElementById("toast-stack");
export function toast(msg, kind = "ok", ms = 2600) {
  const el = document.createElement("div");
  el.className = "toast " + kind;
  el.setAttribute("role", kind === "err" ? "alert" : "status");
  el.textContent = msg;
  stack().appendChild(el);
  setTimeout(() => {
    el.style.opacity = 0;
    el.style.transition = "opacity .18s";
    setTimeout(() => el.remove(), 200);
  }, ms);
}

// --- error panel with retry button ---
export function renderError(root, err, onRetry) {
  const msg = escapeHtml(err?.detail || err?.message || err || "שגיאה");
  root.innerHTML = `
    <div class="err" role="alert">
      <span>${msg}</span>
      ${onRetry ? '<button class="retry" type="button">נסה שוב</button>' : ""}
    </div>`;
  if (onRetry) root.querySelector(".retry").addEventListener("click", onRetry);
}

// --- AI observability panel ---
export function renderAIPanel(root, ai) {
  if (!ai) { root.hidden = true; root.innerHTML = ""; return; }
  const hit = ai.cache_read_input_tokens || 0;
  const write = ai.cache_creation_input_tokens || 0;
  const input = ai.input_tokens || 0;
  const output = ai.output_tokens || 0;
  const cacheClass = hit > 0 ? "hit" : "";
  root.hidden = false;
  root.innerHTML = `
    <div><div class="k">Model</div><div class="v">${escapeHtml(ai.model || "—")}</div></div>
    <div><div class="k">Latency</div><div class="v ok">${ai.latency_ms} ms</div></div>
    <div><div class="k">Input</div><div class="v">${input.toLocaleString()}</div></div>
    <div><div class="k">Output</div><div class="v">${output.toLocaleString()}</div></div>
    <div><div class="k">Cache hit</div><div class="v ${cacheClass}">${hit.toLocaleString()}</div></div>
    <div><div class="k">Cache write</div><div class="v">${write.toLocaleString()}</div></div>
    <div><div class="k">Stop</div><div class="v">${escapeHtml(ai.stop_reason || "—")}</div></div>
  `;
}

// --- result row renderer — used by search, NL, and any listing tab ---
export function renderRows(root, rows, { onOpen } = {}) {
  root.innerHTML = "";
  if (!rows.length) {
    root.innerHTML = '<div class="empty">אין תוצאות</div>';
    return;
  }
  const frag = document.createDocumentFragment();
  rows.forEach((r, i) => {
    const d = document.createElement("div");
    d.className = "row";
    d.setAttribute("role", "option");
    d.setAttribute("tabindex", i === 0 ? "0" : "-1");
    d.dataset.barcode = r.barcode;
    const promoBadge = r.has_promo
      ? '<span class="promo-badge" title="במבצע" aria-label="מוצר במבצע">%</span>'
      : "";
    d.innerHTML = `
      <div>
        <div class="nm">${escapeHtml(r.name || "—")} ${promoBadge}</div>
        <div class="mf">${escapeHtml(r.manufacturer || "")}
          <span class="bc">${escapeHtml(r.barcode)}</span></div>
      </div>
      <div class="price min">${shekel(r.min_price)}</div>
      <div class="price max">${shekel(r.max_price)}</div>
      <div class="chains" aria-label="מספר רשתות עם מחיר">${r.chains_with_price ?? ""}</div>
      <div><button class="btn" type="button" tabindex="-1">השוואה</button></div>
    `;
    d.addEventListener("click", () => onOpen && onOpen(r.barcode));
    d.addEventListener("keydown", e => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        onOpen && onOpen(r.barcode);
      }
    });
    frag.appendChild(d);
  });
  root.appendChild(frag);
  attachArrowNav(root);
}

// Arrow-key roving tabindex within a listbox container.
export function attachArrowNav(root) {
  root.addEventListener("keydown", e => {
    const rows = [...root.querySelectorAll(".row")];
    if (!rows.length) return;
    const active = document.activeElement;
    const idx = rows.indexOf(active);
    let next = -1;
    if (e.key === "ArrowDown") next = Math.min(rows.length - 1, idx + 1);
    else if (e.key === "ArrowUp") next = Math.max(0, idx - 1);
    else if (e.key === "Home") next = 0;
    else if (e.key === "End") next = rows.length - 1;
    if (next < 0) return;
    e.preventDefault();
    rows.forEach((r, i) => r.setAttribute("tabindex", i === next ? "0" : "-1"));
    rows.forEach(r => r.classList.remove("focused"));
    rows[next].classList.add("focused");
    rows[next].focus();
  });
}
