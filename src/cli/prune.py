"""Enforce retention window: keep the last N days of price_observations +
raw gz dumps, drop the rest.

`current_prices` is preserved (it's a materialized latest-value per (store,product)
and is updated in-place by the scraper — so retention pruning leaves it intact).

Run manually:  python -m src.cli.prune --retain 7
Auto: called by the backfill CLI after each scrape unless --no-prune."""
from __future__ import annotations

import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import typer
from rich.console import Console

from ..db.connection import connect

RAW_ROOT = Path(__file__).resolve().parents[2] / "data" / "raw"

app = typer.Typer(help="Drop price_observations + raw files older than retention window")
console = Console()


def prune(retain_days: int = 7) -> tuple[int, int]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=retain_days)
    cutoff_iso = cutoff.isoformat(timespec="seconds")

    conn = connect()
    try:
        n_rows = conn.execute(
            "DELETE FROM price_observations WHERE fetched_at < ?",
            (cutoff_iso,),
        ).rowcount
        conn.execute(
            "DELETE FROM scrape_runs WHERE started_at < ?",
            (cutoff_iso,),
        )
        conn.execute("VACUUM")
    finally:
        conn.close()

    n_files = 0
    if RAW_ROOT.exists():
        for chain_dir in RAW_ROOT.iterdir():
            if not chain_dir.is_dir():
                continue
            for day_dir in chain_dir.iterdir():
                if not day_dir.is_dir():
                    continue
                try:
                    day = datetime.strptime(day_dir.name, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                if day < cutoff:
                    shutil.rmtree(day_dir)
                    n_files += 1

    console.print(f"pruned: {n_rows} price_observations, {n_files} raw day-dirs (cutoff {cutoff_iso})")
    return n_rows, n_files


@app.command()
def main(retain: int = typer.Option(7, help="keep this many days")) -> None:
    prune(retain_days=retain)


if __name__ == "__main__":
    app()
