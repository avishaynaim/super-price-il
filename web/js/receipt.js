// Receipt upload tab: drag-drop + image preview + XHR progress + price comparison + basket.

import { upload } from "./api.js";
import { shekel, escapeHtml, renderError, renderAIPanel, toast } from "./ui.js";

const form    = () => document.getElementById("form-receipt");
const input   = () => document.getElementById("receipt-file");
const dz      = () => document.getElementById("dropzone");
const preview = () => document.getElementById("dz-preview");
const prog    = () => document.getElementById("receipt-progress");
const bar     = () => document.getElementById("receipt-bar");
const progLbl = () => document.getElementById("receipt-progress-label");
const aiEl    = () => document.getElementById("receipt-ai");
const result  = () => document.getElementById("receipt-result");

// ── basket (localStorage) ─────────────────────────────────────────────────────

const BASKET_KEY = "super_price_basket";

function loadBasket() {
  try { return JSON.parse(localStorage.getItem(BASKET_KEY) || "[]"); }
  catch { return []; }
}

function saveBasket(items) {
  localStorage.setItem(BASKET_KEY, JSON.stringify(items));
  _emitBasketChange();
}

function _emitBasketChange() {
  window.dispatchEvent(new CustomEvent("basket-change", { detail: loadBasket() }));
}

export function addToBasket(item) {
  const basket = loadBasket();
  const existing = basket.find(b => b.barcode && b.barcode === item.barcode);
  if (existing) {
    existing.qty = (existing.qty || 1) + 1;
  } else {
    basket.push({ ...item, qty: 1, added_at: Date.now() });
  }
  saveBasket(basket);
  toast(`נוסף לסל: ${item.name || item.barcode}`, "ok");
}

export function removeFromBasket(barcode) {
  saveBasket(loadBasket().filter(b => b.barcode !== barcode));
}

export function clearBasket() {
  saveBasket([]);
}

export function getBasket() {
  return loadBasket();
}

// ── file preview ──────────────────────────────────────────────────────────────

function showPreview(files) {
  const p = preview();
  p.hidden = false;
  p.innerHTML = "";
  const arr = [...files];
  if (arr.length === 1 && arr[0].type.startsWith("image/")) {
    const img = document.createElement("img");
    img.alt = "תצוגה מקדימה";
    img.src = URL.createObjectURL(arr[0]);
    img.onload = () => URL.revokeObjectURL(img.src);
    p.appendChild(img);
  } else {
    // Multiple files — show thumbnail strip
    const strip = document.createElement("div");
    strip.className = "dz-strip";
    arr.forEach((f, i) => {
      const wrap = document.createElement("div");
      wrap.className = "dz-thumb";
      if (f.type.startsWith("image/")) {
        const img = document.createElement("img");
        img.src = URL.createObjectURL(f);
        img.alt = `תמונה ${i + 1}`;
        img.onload = () => URL.revokeObjectURL(img.src);
        wrap.appendChild(img);
      } else {
        wrap.textContent = "📄";
      }
      const lbl = document.createElement("div");
      lbl.className = "dz-thumb-lbl";
      lbl.textContent = `${i + 1}`;
      wrap.appendChild(lbl);
      strip.appendChild(wrap);
    });
    const info = document.createElement("div");
    info.className = "dz-multi-info";
    info.textContent = `${arr.length} תמונות נבחרו — יאוחדו לקבלה אחת`;
    p.appendChild(strip);
    p.appendChild(info);
  }
}

function setProgress(frac, label) {
  prog().hidden = false;
  bar().style.width = Math.round(frac * 100) + "%";
  progLbl().textContent = label;
}

function hideProgress() {
  prog().hidden = true;
  bar().style.width = "0%";
}

// ── init ──────────────────────────────────────────────────────────────────────

export function initReceipt() {
  dz().addEventListener("click", () => input().click());
  dz().addEventListener("keydown", e => {
    if (e.key === "Enter" || e.key === " ") { e.preventDefault(); input().click(); }
  });

  ["dragenter", "dragover"].forEach(ev => dz().addEventListener(ev, e => {
    e.preventDefault(); e.stopPropagation();
    dz().classList.add("drag");
  }));
  ["dragleave", "drop"].forEach(ev => dz().addEventListener(ev, e => {
    e.preventDefault(); e.stopPropagation();
    dz().classList.remove("drag");
  }));
  dz().addEventListener("drop", e => {
    const files = e.dataTransfer?.files;
    if (files?.length) { input().files = files; showPreview(files); }
  });
  input().addEventListener("change", () => {
    if (input().files?.length) showPreview(input().files);
  });

  form().addEventListener("submit", async e => {
    e.preventDefault();
    const files = input().files;
    if (!files?.length) { toast("בחר תמונה אחת או יותר", "warn"); return; }

    const fd = new FormData();
    [...files].forEach(f => fd.append("files", f));
    const city = form().querySelector("[name=city]")?.value;
    if (city) fd.append("city", city);

    aiEl().hidden = true;
    result().innerHTML = "";
    const submitBtn = form().querySelector('button[type="submit"]');
    submitBtn.disabled = true;

    const isMulti = files.length > 1;
    const endpoint = isMulti ? "/api/receipts/multi" : "/api/receipts";
    if (!isMulti) {
      // Single file — use original endpoint with single `file` field
      fd.delete("files");
      fd.append("file", files[0]);
    }

    const label = isMulti
      ? `מעבד ${files.length} תמונות ומאחד…`
      : "מנתח קבלה…";

    setProgress(0, "מעלה…");
    try {
      const j = await upload(endpoint, fd, frac => {
        if (frac >= 0.99) setProgress(1, label);
        else setProgress(frac, `מעלה… ${Math.round(frac * 100)}%`);
      });
      hideProgress();
      renderAIPanel(aiEl(), j.ai);
      renderReceipt(result(), j);
      const msg = isMulti
        ? `${files.length} תמונות אוחדו — נמצאו ${j.items?.length || 0} פריטים`
        : "הקבלה עובדה בהצלחה";
      toast(msg, "ok");
    } catch (err) {
      hideProgress();
      renderError(result(), err, () => form().requestSubmit());
    } finally {
      submitBtn.disabled = false;
    }
  });
}

// ── render ─────────────────────────────────────────────────────────────────────

function renderReceipt(root, j) {
  const matchedItems = (j.items || []).filter(it => it.product_id || it.barcode);

  // ── chain comparison table ──
  let html = `<h3 style="margin:10px 0">סיכום קבלה</h3>
    <div>פריטים: <b>${j.items.length}</b>
      · מקור: <b>${escapeHtml(j.source)}</b>
      · שילמת: <b>${shekel(j.total_paid)}</b></div>`;

  if (j.alternatives?.length) {
    const totalItems = j.alternatives[0]?.items_total || j.items.length;
    const best = j.alternatives.find(a => !a.low_coverage);
    html += `<h3 style="margin:16px 0 6px">השוואת רשתות
      <span class="hint" style="font-size:12px;font-weight:400"> — סל מלא מוערך (${totalItems} פריטים)</span></h3>`;
    html += `<table><thead><tr>
      <th>רשת</th><th>כיסוי</th><th>מחיר נמצא</th><th>סל מלא (מוערך)</th><th>הפרש</th>
    </tr></thead><tbody>`;
    for (const a of j.alternatives) {
      const pct = Math.round((a.coverage || 0) * 100);
      const coverageClass = a.low_coverage ? "cov-low" : pct >= 80 ? "cov-ok" : "cov-mid";
      const refTotal = best ? best.estimated_total : a.estimated_total;
      const diff = a.estimated_total - refTotal;
      const diffTxt = diff === 0 ? "הכי זול" : `+${shekel(diff)}`;
      html += `<tr class="${a.low_coverage ? "alt-low-cov" : ""}">
        <td>${escapeHtml(a.chain_name_he)}${a.low_coverage ? ' <span class="cov-warn" title="כיסוי נמוך">⚠</span>' : ''}</td>
        <td class="num"><span class="${coverageClass}">${pct}%</span> <span class="muted">${a.items_found}/${totalItems}</span></td>
        <td class="num">${shekel(a.basket_total)}</td>
        <td class="num"><b>${shekel(a.estimated_total)}</b></td>
        <td class="num ${diff === 0 ? "cheapest" : ""}">${diffTxt}</td>
      </tr>`;
    }
    html += `</tbody></table>`;
  }

  // ── per-item price comparison + basket ──
  const itemsWithCmp = matchedItems.filter(it => it.price_cmp?.cheapest || it.price_cmp?.priciest);

  if (itemsWithCmp.length) {
    const totalSavings = itemsWithCmp.reduce((acc, it) => {
      const s = it.price_cmp?.savings;
      return acc + (s > 0 ? s : 0);
    }, 0);

    html += `<div style="display:flex;align-items:center;gap:12px;margin:20px 0 8px">
      <h3 style="margin:0">מחירים לפריט — הזול והיקר בארץ</h3>
      <button class="btn-add-all" id="btn-add-all-receipt" style="margin-right:auto">
        + הוסף הכל לסל (${itemsWithCmp.length} פריטים)
      </button>
    </div>`;
    if (totalSavings > 0.5) {
      html += `<div class="savings-banner">
        💰 תוכל לחסוך עד <b>${shekel(totalSavings)}</b> על הסל הזה אם תקנה בחנויות הזולות ביותר
      </div>`;
    }

    html += `<table class="price-cmp-table"><thead><tr>
      <th>פריט</th><th>ברקוד</th><th>שילמת</th>
      <th class="best-price-col">הכי זול <span class="hint">(רשת)</span></th>
      <th class="worst-price-col">הכי יקר <span class="hint">(רשת)</span></th>
      <th>פוטנציאל חיסכון</th>
      <th>סל</th>
    </tr></thead><tbody>`;

    for (const it of itemsWithCmp) {
      const cmp      = it.price_cmp || {};
      const name     = escapeHtml(it.name || it.raw_name || it.barcode || "");
      const barcode  = escapeHtml(it.barcode || "");
      const youPaid  = it.unit_price != null ? shekel(it.unit_price) : "—";
      const cheapest = cmp.cheapest
        ? `<b>${shekel(cmp.cheapest.price)}</b> <span class="chain-tag">${escapeHtml(cmp.cheapest.chain_name_he)}</span>`
        : "—";
      const priciest = cmp.priciest
        ? `${shekel(cmp.priciest.price)} <span class="chain-tag chain-tag-hi">${escapeHtml(cmp.priciest.chain_name_he)}</span>`
        : "—";
      const savings = cmp.savings > 0.05
        ? `<span class="savings-pos">חסוך ${shekel(cmp.savings)}</span>`
        : cmp.savings < -0.05
          ? `<span class="savings-neg">יקר ב-${shekel(-cmp.savings)}</span>`
          : '<span class="muted">בסדר</span>';

      const itemData = JSON.stringify({
        barcode:  it.barcode,
        name:     it.name || it.raw_name,
        price:    cmp.cheapest?.price ?? it.unit_price,
        chain:    cmp.cheapest?.chain_name_he,
      }).replace(/"/g, "&quot;");

      html += `<tr>
        <td>${name}</td>
        <td class="num mono">${barcode}</td>
        <td class="num">${youPaid}</td>
        <td class="num best-price-col">${cheapest}</td>
        <td class="num worst-price-col">${priciest}</td>
        <td class="num">${savings}</td>
        <td><button class="btn-add-item" data-item="${itemData}">+ סל</button></td>
      </tr>`;
    }
    html += `</tbody></table>`;
  }

  // ── full items list ──
  html += `<h3 style="margin:20px 0 6px">כל הפריטים (${j.items.length})</h3>
    <table><thead><tr><th>פריט</th><th>ברקוד</th><th>כמות</th><th>סה"כ</th><th>התאמה</th></tr></thead><tbody>`;
  for (const it of j.items) {
    const conf = it.match_confidence != null ? (it.match_confidence * 100).toFixed(0) + "%" : "";
    html += `<tr>
      <td>${escapeHtml(it.name || it.raw_name || "")}</td>
      <td class="num">${escapeHtml(it.barcode || "")}</td>
      <td class="num">${it.quantity ?? ""}</td>
      <td class="num">${shekel(it.line_total)}</td>
      <td class="num">${conf}</td>
    </tr>`;
  }
  html += `</tbody></table>`;

  root.innerHTML = html;

  // Wire "add single item" buttons
  root.querySelectorAll(".btn-add-item").forEach(btn => {
    btn.addEventListener("click", () => {
      try { addToBasket(JSON.parse(btn.dataset.item)); }
      catch { toast("שגיאה בהוספה לסל", "error"); }
    });
  });

  // Wire "add all to basket"
  const addAllBtn = root.querySelector("#btn-add-all-receipt");
  if (addAllBtn) {
    addAllBtn.addEventListener("click", () => {
      itemsWithCmp.forEach(it => {
        const cmp = it.price_cmp || {};
        addToBasket({
          barcode: it.barcode,
          name:    it.name || it.raw_name,
          price:   cmp.cheapest?.price ?? it.unit_price,
          chain:   cmp.cheapest?.chain_name_he,
        });
      });
      toast(`${itemsWithCmp.length} פריטים נוספו לסל`, "ok");
    });
  }
}
