"""In-memory price index for sub-millisecond barcode lookups.

Loaded once at server startup from SQLite. Provides O(1) dict access instead
of a 52ms JOIN query per barcode. Falls back to live SQL for cache misses.

Usage:
    from src.db.price_index import price_index
    info = price_index.lookup("7290110115388")
    # {"product_id": 42, "name": "...", "chains": [{"chain_name_he": "שופרסל", "min_price": 7.30, ...}]}
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

log = logging.getLogger(__name__)

# ── barcode → full price info ─────────────────────────────────────────────────

_PRICE_SQL = """
    SELECT ch.code    AS chain_code,
           ch.name_he AS chain_name_he,
           MIN(cp.price) AS min_price,
           MAX(cp.price) AS max_price
      FROM current_prices cp
      JOIN stores s  ON s.id  = cp.store_id
      JOIN chains ch ON ch.id = s.chain_id
     WHERE cp.product_id = ?
  GROUP BY ch.id
  ORDER BY min_price
"""


class PriceIndex:
    """Lazily-populated, thread-safe in-memory cache of barcode → price data."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # barcode → {"product_id": int, "name": str, "manufacturer": str,
        #             "chains": [...], "cheapest": {...}, "priciest": {...}}
        self._cache: dict[str, dict[str, Any]] = {}
        # product_id → barcode (for reverse lookups)
        self._pid_to_bc: dict[int, str] = {}
        self._preloaded = False

    # ── public API ────────────────────────────────────────────────────────────

    def lookup(self, barcode: str, raise_errors: bool = False) -> dict[str, Any] | None:
        """Return price info for a barcode. Cache miss → live SQL (52ms).

        Returns None when not found. DB errors are logged; they return None
        too unless raise_errors=True, in which case they propagate."""
        with self._lock:
            if barcode in self._cache:
                return self._cache[barcode]

        try:
            result = self._fetch(barcode)
        except Exception:
            if raise_errors:
                raise
            return None
        if result:
            with self._lock:
                self._cache[barcode] = result
                self._pid_to_bc[result["product_id"]] = barcode
        return result

    def preload(self) -> None:
        """Background preload: load all products + prices into RAM.

        On a 738MB DB with 9.9M price rows and a product_id index this
        takes ~20–40s and uses ~250MB RAM. Skipped if already loaded.
        """
        if self._preloaded:
            return
        t0 = time.perf_counter()
        log.info("PriceIndex: preloading...")
        try:
            from .local import cursor
            # Load all products with barcodes first
            with cursor() as conn:
                products = conn.execute(
                    "SELECT id, barcode, name, manufacturer FROM products WHERE barcode IS NOT NULL"
                ).fetchall()

            pid_to_meta: dict[int, dict] = {}
            bc_to_pid: dict[str, int] = {}
            for p in products:
                pid_to_meta[p["id"]] = {
                    "product_id": p["id"],
                    "name": p["name"],
                    "manufacturer": p["manufacturer"],
                    "chains": [],
                }
                bc_to_pid[p["barcode"]] = p["id"]

            # Load all chain prices per product in one pass
            with cursor() as conn:
                rows = conn.execute("""
                    SELECT cp.product_id,
                           ch.code    AS chain_code,
                           ch.name_he AS chain_name_he,
                           MIN(cp.price) AS min_price,
                           MAX(cp.price) AS max_price
                      FROM current_prices cp
                      JOIN stores s  ON s.id  = cp.store_id
                      JOIN chains ch ON ch.id = s.chain_id
                  GROUP BY cp.product_id, ch.id
                """).fetchall()

            for r in rows:
                meta = pid_to_meta.get(r["product_id"])
                if meta:
                    meta["chains"].append({
                        "chain_code":    r["chain_code"],
                        "chain_name_he": r["chain_name_he"],
                        "min_price":     r["min_price"],
                        "max_price":     r["max_price"],
                    })

            # Compute cheapest / priciest and populate cache
            new_cache: dict[str, dict] = {}
            new_pid_bc: dict[int, str] = {}
            for barcode, pid in bc_to_pid.items():
                meta = pid_to_meta.get(pid)
                if not meta:
                    continue
                chains = sorted(meta["chains"], key=lambda c: c["min_price"])
                cheapest = chains[0] if chains else None
                priciest = max(chains, key=lambda c: c["max_price"]) if chains else None
                new_cache[barcode] = {
                    **meta,
                    "chains":   chains,
                    "cheapest": cheapest,
                    "priciest": priciest,
                }
                new_pid_bc[pid] = barcode

            with self._lock:
                self._cache.update(new_cache)
                self._pid_to_bc.update(new_pid_bc)
                self._preloaded = True

            elapsed = time.perf_counter() - t0
            log.info(f"PriceIndex: preloaded {len(new_cache):,} products in {elapsed:.1f}s")

        except Exception as e:
            log.warning(f"PriceIndex preload failed: {e}")

    def preload_background(self) -> None:
        """Kick off preload in a daemon thread so server starts immediately."""
        t = threading.Thread(target=self.preload, daemon=True, name="price-index-preload")
        t.start()

    def stats(self) -> dict:
        with self._lock:
            return {"cached": len(self._cache), "preloaded": self._preloaded}

    # ── private ───────────────────────────────────────────────────────────────

    def _fetch(self, barcode: str) -> dict[str, Any] | None:
        """Live SQL fetch for a cache miss. Uses the product_id index → ~52ms."""
        try:
            from .local import cursor
            with cursor() as conn:
                row = conn.execute(
                    "SELECT id, name, manufacturer FROM products WHERE barcode=?", (barcode,)
                ).fetchone()
            if not row:
                return None
            pid = row["id"]
            with cursor() as conn:
                chain_rows = conn.execute(_PRICE_SQL, (pid,)).fetchall()
            chains = [dict(r) for r in chain_rows]
            cheapest = chains[0] if chains else None
            priciest = max(chains, key=lambda c: c["max_price"]) if chains else None
            return {
                "product_id":   pid,
                "name":         row["name"],
                "manufacturer": row["manufacturer"],
                "chains":       chains,
                "cheapest":     cheapest,
                "priciest":     priciest,
            }
        except Exception:
            log.exception("PriceIndex: live fetch failed for barcode %r", barcode)
            raise


# Singleton — import and use directly
price_index = PriceIndex()
