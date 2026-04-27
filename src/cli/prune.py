"""Remove raw gz files older than 1 day.

With Supabase and 1-day retention, current_prices is replaced on every
scrape run (delete + insert), so there's nothing to prune in the DB.
Only the local raw/ dump directory needs periodic cleanup.
"""
from __future__ import annotations

import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import typer
from rich.console import Console

RAW_ROOT = Path(__file__).resolve().parents[2] / "data" / "raw"
app = typer.Typer(help="Remove raw files older than 1 day")
console = Console()


def prune(retain_days: int = 1) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=retain_days)
    n = 0
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
                    n += 1
    console.print(f"pruned {n} raw day-dirs (cutoff {cutoff.date()})")
    return n


@app.command()
def main(retain: int = typer.Option(1, help="days of raw files to keep")) -> None:
    prune(retain_days=retain)


if __name__ == "__main__":
    app()
