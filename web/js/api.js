// Thin fetch wrapper. Returns parsed JSON; rejects with {detail, status} on error.
export async function api(path, query = {}, init = {}) {
  const url = new URL(path, location.origin);
  for (const [k, v] of Object.entries(query)) {
    if (v !== "" && v != null) url.searchParams.set(k, v);
  }
  let resp;
  try {
    resp = await fetch(url, init);
  } catch (e) {
    throw { detail: navigator.onLine ? "שגיאת רשת" : "אין חיבור לאינטרנט", network: true };
  }
  let body = null;
  try { body = await resp.json(); } catch { /* non-JSON body; leave null */ }
  if (!resp.ok) {
    throw { detail: (body && body.detail) || `HTTP ${resp.status}`, status: resp.status };
  }
  return body;
}

// Upload with XHR so we can report progress. Resolves with parsed JSON.
export function upload(path, formData, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", path);
    xhr.upload.onprogress = e => {
      if (e.lengthComputable && onProgress) onProgress(e.loaded / e.total);
    };
    xhr.onload = () => {
      let j = null;
      try { j = JSON.parse(xhr.responseText); } catch { /* leave null */ }
      if (xhr.status >= 200 && xhr.status < 300) resolve(j);
      else reject({ detail: (j && j.detail) || `HTTP ${xhr.status}`, status: xhr.status });
    };
    xhr.onerror = () => reject({ detail: "שגיאת רשת", network: true });
    xhr.send(formData);
  });
}
