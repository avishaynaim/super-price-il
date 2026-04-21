// Receipt upload tab: drag-drop + image preview + XHR progress + AI panel.

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

function showPreview(file) {
  const p = preview();
  p.hidden = false;
  p.innerHTML = "";
  if (file.type.startsWith("image/")) {
    const img = document.createElement("img");
    img.alt = "תצוגה מקדימה של הקבלה";
    img.src = URL.createObjectURL(file);
    img.onload = () => URL.revokeObjectURL(img.src);
    p.appendChild(img);
  } else if (file.type === "application/pdf" || file.name.toLowerCase().endsWith(".pdf")) {
    const t = document.createElement("div");
    t.className = "pdf-tag";
    t.textContent = `📄 ${file.name} · ${(file.size / 1024).toFixed(0)} KB`;
    p.appendChild(t);
  } else {
    const t = document.createElement("div");
    t.className = "pdf-tag";
    t.textContent = file.name;
    p.appendChild(t);
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

export function initReceipt() {
  // open file dialog from dropzone click / Enter / Space
  dz().addEventListener("click", e => {
    if (e.target.tagName !== "INPUT") input().click();
  });
  dz().addEventListener("keydown", e => {
    if (e.key === "Enter" || e.key === " ") { e.preventDefault(); input().click(); }
  });

  // drag-drop
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
    if (files && files[0]) {
      input().files = files;
      showPreview(files[0]);
    }
  });
  input().addEventListener("change", () => {
    if (input().files[0]) showPreview(input().files[0]);
  });

  form().addEventListener("submit", async e => {
    e.preventDefault();
    if (!input().files[0]) { toast("בחר קובץ קבלה", "warn"); return; }
    const fd = new FormData(form());

    aiEl().hidden = true;
    result().innerHTML = "";
    setProgress(0, "מעלה…");
    const submitBtn = form().querySelector('button[type="submit"]');
    submitBtn.disabled = true;

    try {
      const j = await upload("/api/receipts", fd, frac => {
        if (frac >= 0.99) setProgress(1, "Claude מעבד את הקבלה…");
        else setProgress(frac, `מעלה… ${Math.round(frac * 100)}%`);
      });
      hideProgress();
      renderAIPanel(aiEl(), j.ai);
      renderReceipt(result(), j);
      toast("הקבלה עובדה בהצלחה", "ok");
    } catch (err) {
      hideProgress();
      renderError(result(), err, () => form().requestSubmit());
    } finally {
      submitBtn.disabled = false;
    }
  });
}

function renderReceipt(root, j) {
  let html = `<h3 style="margin:10px 0">סיכום קבלה</h3>
    <div>פריטים זוהו: <b>${j.items.length}</b>
      · נטען מ־<b>${escapeHtml(j.source)}</b>
      · סך הכל מוערך: <b>${shekel(j.total_paid)}</b></div>`;

  if (j.alternatives?.length) {
    html += `<h3 style="margin:16px 0 6px">חלופות חסכוניות ${j.city ? `באזור "${escapeHtml(j.city)}"` : ""}</h3>
      <table><thead><tr><th>רשת</th><th>מחיר סל</th><th>הפרש</th></tr></thead><tbody>`;
    const base = j.total_paid;
    for (const a of j.alternatives) {
      const diff = base - a.basket_total;
      const cls = diff > 0 ? "cheapest" : "";
      const sign = diff > 0 ? "-" : "+";
      html += `<tr>
        <td>${escapeHtml(a.chain_name_he)}</td>
        <td class="num">${shekel(a.basket_total)}</td>
        <td class="num ${cls}">${sign}${shekel(Math.abs(diff))}</td>
      </tr>`;
    }
    html += "</tbody></table>";
  }

  html += `<h3 style="margin:16px 0 6px">פריטים</h3>
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
  html += "</tbody></table>";
  root.innerHTML = html;
}
