// Natural-language AI search tab.
// Renders the same row grid as /search, plus an observability panel showing
// latency + token usage so you can see when prompt caching kicks in.

import { api } from "./api.js";
import { renderRows, renderError, renderAIPanel, escapeHtml } from "./ui.js";
import { openProduct } from "./product.js";
import { current, setRoute } from "./router.js";

const form    = () => document.getElementById("form-nl");
const results = () => document.getElementById("results-nl");
const explain = () => document.getElementById("nl-explain");
const toolEl  = () => document.getElementById("nl-tool");
const aiEl    = () => document.getElementById("nl-ai");

async function submitQuery(query, push = true) {
  const q = (query ?? form().elements.query.value).trim();
  if (!q) return;
  if (push) setRoute("nl", { q });

  explain().textContent = "";
  toolEl().textContent = "";
  aiEl().hidden = true;
  results().innerHTML = '<div class="loading">שואל את Claude…</div>';

  try {
    const resp = await api("/api/nl-filter", {}, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: q }),
    });
    if (resp.explanation) explain().textContent = resp.explanation;
    toolEl().textContent = "filter → " + JSON.stringify(resp.tool_call?.input || {}, null, 0);
    renderAIPanel(aiEl(), resp.ai);
    renderRows(results(), resp.rows || [], { onOpen: openProduct });
  } catch (err) {
    renderError(results(), err, () => submitQuery(q, false));
  }
}

export function initNL() {
  form().addEventListener("submit", e => {
    e.preventDefault();
    submitQuery(form().elements.query.value);
  });

  const { tab, params } = current();
  if (tab === "nl") {
    const q = params.get("q");
    if (q) { form().elements.query.value = q; submitQuery(q, false); }
  }
}

export function onRouted(state) {
  if (state.tab !== "nl") return;
  const q = state.params.get("q");
  if (q && form().elements.query.value !== q) {
    form().elements.query.value = q;
    submitQuery(q, false);
  }
}
