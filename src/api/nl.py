"""Natural-language → structured filter via Claude tool use.

Takes a Hebrew (or English) query like:
  "חלב תנובה 3% הכי זול בתל אביב"
  "cheapest milk 1L in Jerusalem across chains"

and runs it through claude-sonnet-4-6 with a single tool (`query_products`) whose
schema is exactly what /api/search+/api/compare take. The tool call args are the
structured filter; the server executes it and returns rows.

Why tool use: lets the model decline to fabricate data and forces a schema-valid
filter. Why caching: the system prompt (which names chains, cities, units) is
large and reused across requests.

Set ANTHROPIC_API_KEY in env. Model fallback lives in MODEL below.
"""
from __future__ import annotations

import os
import time
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..db.connection import connect

nl_router = APIRouter()

MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")

QUERY_TOOL = {
    "name": "query_products",
    "description": (
        "Return matching products with their current prices across chains. "
        "Prefer this over answering from memory; only real DB rows are authoritative."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "q": {
                "type": "string",
                "description": "Search keyword (Hebrew or English). Use product name, brand, or barcode. Single concept, not a full sentence.",
            },
            "chain": {
                "type": "string",
                "description": "Restrict to one chain code. One of: shufersal, rami_levi, victory, yohananof, tiv_taam, king_store, mega, hazi_hinam.",
            },
            "city": {
                "type": "string",
                "description": "Restrict to stores whose city contains this substring (Hebrew allowed).",
            },
            "mode": {
                "type": "string",
                "enum": ["search", "cheapest_per_chain"],
                "description": "search = product list; cheapest_per_chain = one row per chain with min price (needs a specific product — caller should pick a hit first).",
            },
            "barcode": {
                "type": "string",
                "description": "13-digit GS1 for cheapest_per_chain mode.",
            },
            "limit": {"type": "integer", "minimum": 1, "maximum": 200},
        },
        "required": ["q"],
    },
}


SYSTEM = (
    "You translate shopping queries into a single call of the query_products tool. "
    "Do not answer from memory. Supported chains (Hebrew names → codes): "
    "שופרסל=shufersal, רמי לוי=rami_levi, ויקטורי=victory, יוחננוף=yohananof, "
    "טיב טעם=tiv_taam, קינג סטור=king_store, מגה/קארפור=mega, חצי חינם=hazi_hinam. "
    "If the user asks 'where is X cheapest', still start with mode=search, then "
    "the API may follow up with cheapest_per_chain once a specific barcode is known. "
    "Keep `q` to the product concept (e.g. 'חלב 3%', not the full sentence)."
)


class NLRequest(BaseModel):
    query: str


class AIUsage(BaseModel):
    model: str
    latency_ms: int
    input_tokens: int | None = None
    output_tokens: int | None = None
    cache_read_input_tokens: int | None = None
    cache_creation_input_tokens: int | None = None
    stop_reason: str | None = None


class NLResponse(BaseModel):
    tool_call: dict[str, Any]
    rows: list[dict[str, Any]]
    explanation: str | None = None
    ai: AIUsage | None = None


def _run_tool(args: dict[str, Any]) -> list[dict[str, Any]]:
    mode = args.get("mode", "search")
    q = args.get("q", "")
    chain = args.get("chain")
    city = args.get("city")
    limit = min(int(args.get("limit") or 50), 200)
    barcode = args.get("barcode")

    conn = connect()
    try:
        if mode == "cheapest_per_chain" and barcode:
            sql = """
                SELECT ch.code AS chain_code, ch.name_he AS chain_name_he,
                       MIN(cp.price) AS min_price,
                       COUNT(DISTINCT s.id) AS stores_with
                  FROM current_prices cp
                  JOIN products p ON p.id = cp.product_id
                  JOIN stores s   ON s.id = cp.store_id
                  JOIN chains ch  ON ch.id = s.chain_id
                 WHERE p.barcode = ?
            """
            params: list = [barcode]
            if city:
                sql += " AND s.city LIKE ?"; params.append(f"%{city}%")
            sql += " GROUP BY ch.id ORDER BY min_price ASC LIMIT ?"
            params.append(limit)
            return [dict(r) for r in conn.execute(sql, params).fetchall()]

        is_bc = q.isdigit() and len(q) >= 6
        sql = """
            SELECT p.barcode, p.name, p.manufacturer,
                   MIN(cp.price) AS min_price,
                   MAX(cp.price) AS max_price,
                   COUNT(DISTINCT ch.id) AS chains_with_price
              FROM products p
              JOIN current_prices cp ON cp.product_id = p.id
              JOIN stores s          ON s.id = cp.store_id
              JOIN chains ch         ON ch.id = s.chain_id
        """
        params = []
        if is_bc:
            sql += " WHERE p.barcode = ?"; params.append(q)
        else:
            sql += " WHERE p.name LIKE ?"; params.append(f"%{q}%")
        if chain:
            sql += " AND ch.code = ?"; params.append(chain)
        if city:
            sql += " AND s.city LIKE ?"; params.append(f"%{city}%")
        sql += " GROUP BY p.id ORDER BY chains_with_price DESC, min_price ASC LIMIT ?"
        params.append(limit)
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


@nl_router.post("/nl-filter", response_model=NLResponse)
def nl_filter(req: NLRequest) -> NLResponse:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(500, "ANTHROPIC_API_KEY not set")

    from anthropic import Anthropic
    client = Anthropic(api_key=api_key)

    t0 = time.perf_counter()
    resp = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        # Cache the system prompt + tool schema — reused every request.
        system=[{"type": "text", "text": SYSTEM, "cache_control": {"type": "ephemeral"}}],
        tools=[QUERY_TOOL],
        tool_choice={"type": "tool", "name": "query_products"},
        messages=[{"role": "user", "content": req.query}],
    )
    latency_ms = int((time.perf_counter() - t0) * 1000)

    tool_block = next((b for b in resp.content if b.type == "tool_use"), None)
    if not tool_block:
        raise HTTPException(502, "model did not emit a tool call")

    args = tool_block.input or {}
    rows = _run_tool(args)
    text_blocks = [b.text for b in resp.content if b.type == "text"]

    u = getattr(resp, "usage", None)
    ai = AIUsage(
        model=MODEL,
        latency_ms=latency_ms,
        input_tokens=getattr(u, "input_tokens", None),
        output_tokens=getattr(u, "output_tokens", None),
        cache_read_input_tokens=getattr(u, "cache_read_input_tokens", None),
        cache_creation_input_tokens=getattr(u, "cache_creation_input_tokens", None),
        stop_reason=getattr(resp, "stop_reason", None),
    )

    return NLResponse(
        tool_call={"name": tool_block.name, "input": args},
        rows=rows,
        explanation="\n".join(text_blocks) or None,
        ai=ai,
    )
