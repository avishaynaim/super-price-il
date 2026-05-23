"""One-time migration: merge zero-padding ghost stores.

For each chain, finds pairs of stores whose codes differ only in leading-zero
padding (e.g. '002' vs '0002'). Merges the one WITHOUT a name (phantom, created
by price-file processing) INTO the one WITH a name (from Stores XML), by
re-pointing all current_prices rows, then deletes the phantom.

Run with: python3 -m src.cli.fix_store_codes
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

DB = Path(__file__).resolve().parents[2] / "data" / "local_prices.db"


def run() -> None:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")

    chains = conn.execute("SELECT id, code FROM chains").fetchall()
    total_merged = 0

    for chain in chains:
        chain_id, chain_code = chain["id"], chain["code"]

        # Find all numeric store codes for this chain
        stores = conn.execute(
            """SELECT id, store_code, name
               FROM stores WHERE chain_id=? AND store_code GLOB '[0-9]*'
               ORDER BY store_code""",
            (chain_id,),
        ).fetchall()

        # Group by integer value
        by_int: dict[int, list[dict]] = {}
        for s in stores:
            try:
                key = int(s["store_code"])
            except ValueError:
                continue
            by_int.setdefault(key, []).append(dict(s))

        for key, group in by_int.items():
            if len(group) < 2:
                continue

            # Keep the store with a name (from Stores XML); merge the rest into it.
            named = [s for s in group if s["name"]]
            unnamed = [s for s in group if not s["name"]]

            if not named or not unnamed:
                # All named or all unnamed — skip (no clear winner)
                continue

            # Prefer the named store as the canonical record
            canonical = named[0]
            phantoms = unnamed

            for phantom in phantoms:
                price_count = conn.execute(
                    "SELECT COUNT(*) FROM current_prices WHERE store_id=?",
                    (phantom["id"],),
                ).fetchone()[0]

                if price_count > 0:
                    # Re-point prices to the canonical store (ignore conflicts —
                    # canonical may already have a price for the same product).
                    conn.execute(
                        """INSERT OR IGNORE INTO current_prices
                               (store_id, product_id, price, unit_price, updated_at)
                           SELECT ?, product_id, price, unit_price, updated_at
                           FROM current_prices WHERE store_id=?""",
                        (canonical["id"], phantom["id"]),
                    )
                    conn.execute(
                        "DELETE FROM current_prices WHERE store_id=?",
                        (phantom["id"],),
                    )

                # Remove the phantom store
                conn.execute("DELETE FROM stores WHERE id=?", (phantom["id"],))
                print(
                    f"  {chain_code}: merged store '{phantom['store_code']}' "
                    f"({price_count} prices) → '{canonical['store_code']}' "
                    f"({canonical['name']})"
                )
                total_merged += 1

    conn.commit()

    # Refresh store_prices_cache and chain_stats_cache
    print("\nRefreshing caches...")
    try:
        from ..db import router as db
        chain_ids = [r["id"] for r in conn.execute("SELECT id FROM chains").fetchall()]
        for cid in chain_ids:
            try:
                db.refresh_caches(cid)
            except Exception:
                pass
    except Exception as e:
        print(f"  cache refresh skipped: {e}")

    conn.close()
    print(f"\nDone. Merged {total_merged} phantom store(s).")


if __name__ == "__main__":
    run()
