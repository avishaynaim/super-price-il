// Settings modal — edits the localStorage prefs.
// Trigger: gear button in the header ("btn-settings").

import { api } from "./api.js";
import { getPrefs, setPrefs, clearPrefs, getProfiles, saveProfile, deleteProfile, activateProfile, getActiveProfileId } from "./prefs.js";
import { escapeHtml, toast } from "./ui.js";

const modal     = () => document.getElementById("settings-modal");
const closeBtn  = () => document.getElementById("settings-close");
const form      = () => document.getElementById("settings-form");
const cityInput = () => document.getElementById("pref-city");
const radiusIn  = () => document.getElementById("pref-radius");
const radiusLbl = () => document.getElementById("pref-radius-label");
const geoBtn    = () => document.getElementById("pref-geo");
const geoStatus = () => document.getElementById("pref-geo-status");
const chainsBox = () => document.getElementById("pref-chains");
const clearBtn  = () => document.getElementById("pref-clear");
const cityList  = () => document.getElementById("cities-datalist");
const retentionIn  = () => document.getElementById("pref-retention");
const retentionLbl = () => document.getElementById("pref-retention-label");

let lastFocus = null;
let citiesLoaded = false;

function renderProfilesList() {
  const el = document.getElementById("pref-profiles-list");
  if (!el) return;
  const profiles = getProfiles();
  const activeId = getActiveProfileId();
  if (!profiles.length) {
    el.innerHTML = '<div class="hint">אין פרופילים שמורים עדיין.</div>';
    return;
  }
  el.innerHTML = profiles.map(prof => {
    const meta = [prof.city, prof.radius_km > 0 ? `${prof.radius_km} ק"מ` : null,
      prof.preferred_chains?.length ? `${prof.preferred_chains.length} רשתות` : null]
      .filter(Boolean).join(", ") || "ללא סינון";
    const isActive = prof.id === activeId;
    return `<div class="profile-list-row">
      <span class="profile-name">${escapeHtml(prof.name)}${isActive ? ' <span style="color:var(--accent-2);font-size:11px">(פעיל)</span>' : ""}</span>
      <span class="profile-meta">${escapeHtml(meta)}</span>
      <button type="button" class="btn-secondary" style="padding:3px 8px;font-size:12px" data-activate="${escapeHtml(prof.id)}">טען</button>
      <button type="button" class="link" style="color:var(--err);font-size:12px" data-delete="${escapeHtml(prof.id)}">מחק</button>
    </div>`;
  }).join("");
  el.querySelectorAll("[data-activate]").forEach(btn => {
    btn.addEventListener("click", () => {
      const prof = getProfiles().find(p => p.id === btn.dataset.activate);
      if (prof) {
        activateProfile(prof);
        const p2 = getPrefs();
        cityInput().value = p2.city || "";
        radiusIn().value  = String(p2.radius_km || 0);
        updateRadiusLabel();
        renderGeoStatus();
        renderProfilesList();
        toast(`פרופיל "${prof.name}" נטען`, "ok");
      }
    });
  });
  el.querySelectorAll("[data-delete]").forEach(btn => {
    btn.addEventListener("click", () => {
      const prof = getProfiles().find(p => p.id === btn.dataset.delete);
      if (prof && confirm(`למחוק את הפרופיל "${prof.name}"?`)) {
        deleteProfile(prof.id);
        renderProfilesList();
        toast(`פרופיל "${prof.name}" נמחק`, "ok");
      }
    });
  });
}

function openModal() {
  lastFocus = document.activeElement;
  const p = getPrefs();
  cityInput().value    = p.city || "";
  radiusIn().value     = String(p.radius_km || 0);
  updateRadiusLabel();
  renderGeoStatus();
  ensureCitiesLoaded();
  ensureChainsLoaded(p.preferred_chains);
  loadRetention();
  renderProfilesList();
  modal().classList.remove("hidden");
  modal().hidden = false;
  setTimeout(() => cityInput().focus(), 40);
  document.addEventListener("keydown", onKey);
}

function updateRetentionLabel() {
  const n = +retentionIn().value || 7;
  retentionLbl().textContent = `(${n} ימים)`;
}

async function loadRetention() {
  try {
    const s = await api("/api/app-settings");
    retentionIn().value = String(s.retention_days || 7);
  } catch {
    retentionIn().value = "7";
  }
  updateRetentionLabel();
}

async function saveRetention() {
  const n = Math.max(1, Math.min(30, +retentionIn().value || 7));
  try {
    await fetch("/api/app-settings", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ retention_days: n }),
    }).then(r => { if (!r.ok) throw new Error("HTTP " + r.status); });
  } catch (e) {
    toast("לא הצלחנו לשמור ימי שמירה", "err");
  }
}

function closeModal() {
  modal().classList.add("hidden");
  modal().hidden = true;
  document.removeEventListener("keydown", onKey);
  lastFocus?.focus?.();
}

function onKey(e) {
  if (e.key === "Escape") { e.preventDefault(); closeModal(); }
}

function updateRadiusLabel() {
  const n = +radiusIn().value || 0;
  radiusLbl().textContent = n === 0 ? "ללא סינון מרחק" : `בתוך ${n} ק"מ`;
}

function renderGeoStatus() {
  const p = getPrefs();
  if (p.coords) {
    geoStatus().innerHTML = `מיקום שמור: ${p.coords.lat.toFixed(3)}, ${p.coords.lng.toFixed(3)}` +
      ` <button type="button" class="link" id="pref-geo-clear">נקה</button>`;
    document.getElementById("pref-geo-clear").addEventListener("click", () => {
      setPrefs({ coords: null });
      renderGeoStatus();
    });
  } else {
    geoStatus().textContent = "לא הוגדר מיקום מדויק — טווח ק\"מ יעבוד רק כשמיקום מוגדר";
  }
}

async function ensureCitiesLoaded() {
  if (citiesLoaded) return;
  try {
    const rows = await api("/api/cities");
    const dl = cityList();
    dl.innerHTML = rows.map(r => {
      const label = r.stores ? ` (${r.stores} חנויות)` : "";
      return `<option value="${escapeHtml(r.name_he)}">${escapeHtml(r.name_he + label)}</option>`;
    }).join("");
    citiesLoaded = true;
  } catch (e) {
    toast("טעינת רשימת ערים נכשלה", "err");
  }
}

let chainsLoaded = false;
async function ensureChainsLoaded(selected) {
  if (chainsLoaded) return;
  try {
    const rows = await api("/api/chains");
    chainsBox().innerHTML = rows.map(r => {
      const on = selected.includes(r.code);
      return `
        <label class="chain-chip">
          <input type="checkbox" name="chain" value="${escapeHtml(r.code)}"${on ? " checked" : ""}/>
          <span>${escapeHtml(r.name_he)}</span>
        </label>`;
    }).join("");
    chainsLoaded = true;
  } catch (e) {
    toast("טעינת רשימת רשתות נכשלה", "err");
  }
}

async function useMyLocation() {
  if (!navigator.geolocation) {
    toast("הדפדפן לא תומך ב-geolocation", "err");
    return;
  }
  geoStatus().textContent = "מבקש הרשאה…";
  navigator.geolocation.getCurrentPosition(async pos => {
    const lat = pos.coords.latitude;
    const lng = pos.coords.longitude;
    try {
      const near = await api("/api/nearest-city", { lat, lng });
      setPrefs({ coords: { lat, lng }, city: near.name_he });
      cityInput().value = near.name_he;
      renderGeoStatus();
      toast(`אותר: ${near.name_he} (${near.distance_km} ק"מ)`, "ok");
    } catch (e) {
      setPrefs({ coords: { lat, lng } });
      renderGeoStatus();
      toast("שמרנו מיקום אך לא נמצאה עיר קרובה", "warn");
    }
  }, err => {
    geoStatus().textContent = "איתור נכשל";
    toast(err.message || "איתור נדחה", "err");
  }, { enableHighAccuracy: false, timeout: 8000, maximumAge: 60000 });
}

async function save(e) {
  e.preventDefault();
  const city = cityInput().value.trim() || null;
  const radius_km = Math.max(0, Math.min(500, +radiusIn().value || 0));
  const chains = [...chainsBox().querySelectorAll('input[name="chain"]:checked')].map(i => i.value);
  setPrefs({ city, radius_km, preferred_chains: chains });
  await saveRetention();
  toast("ההעדפות נשמרו", "ok");
  closeModal();
}

export function initSettings() {
  const btn = document.getElementById("btn-settings");
  btn?.addEventListener("click", openModal);
  closeBtn()?.addEventListener("click", closeModal);
  form()?.addEventListener("submit", save);
  geoBtn()?.addEventListener("click", useMyLocation);
  radiusIn()?.addEventListener("input", updateRadiusLabel);
  retentionIn()?.addEventListener("input", updateRetentionLabel);
  clearBtn()?.addEventListener("click", () => {
    if (confirm("למחוק את כל ההעדפות?")) {
      clearPrefs();
      closeModal();
      toast("ההעדפות נוקו", "ok");
    }
  });
  document.getElementById("pref-save-profile")?.addEventListener("click", () => {
    const nameEl = document.getElementById("pref-new-profile-name");
    const name = nameEl?.value.trim();
    if (!name) { toast("הזן שם לפרופיל", "warn"); return; }
    saveProfile(name);
    if (nameEl) nameEl.value = "";
    renderProfilesList();
    toast(`פרופיל "${name}" נשמר`, "ok");
  });
  // click-outside to close
  modal()?.addEventListener("click", e => { if (e.target === modal()) closeModal(); });
}
