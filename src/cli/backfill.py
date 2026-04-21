"""Scrape one day of price data per chain (default) and prune anything older than
the retention window. Designed to be invoked daily (cron/systemd timer).

Default policy (2026-04-20): pull yesterday + today, keep last 7 days, drop the rest.
Override with --days / --retain."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import httpx
import typer
from rich.console import Console

from ..db.connection import connect
from ..db.migrate import migrate
from ..db.seed import seed_chains
from ..db.upsert import (
    chain_id_for_code,
    get_or_create_store_by_code,
    insert_observations,
    insert_promotions,
    upsert_store,
)
from ..parser import pricefull, promofull, stores as stores_parser
from ..scraper.chains.shufersal import ShufersalScraper
from ..scraper.chains.publishedprices import (
    PublishedPricesScraper,
    make_client_for_publishedprices,
)
from ..scraper.chains.laibcatalog import (
    LaibcatalogScraper,
    make_client_for_laibcatalog,
)
from ..scraper.chains.binaprojects import (
    BinaprojectsScraper,
    make_client_for_binaprojects,
)
from ..scraper.chains.custom import (
    MegaScraper, make_client_for_mega,
    HaziHinamScraper, make_client_for_hazi_hinam,
)
from ..scraper.registry import BY_CODE

app = typer.Typer(help="Backfill price data into prices.db")
console = Console()

SCRAPERS = {
    "shufersal":           ShufersalScraper,
    "rami_levi":           PublishedPricesScraper,
    "yohananof":           PublishedPricesScraper,
    "tiv_taam":            PublishedPricesScraper,
    # Re-enabled 2026-04-21 — gov.il confirms they serve via Cerberus:
    "osher_ad":            PublishedPricesScraper,
    "keshet":              PublishedPricesScraper,
    # Added 2026-04-21 — more publishedprices users from gov.il:
    "dor_alon":            PublishedPricesScraper,
    "super_cofix":         PublishedPricesScraper,
    "politzer":            PublishedPricesScraper,
    "salach_dabah":        PublishedPricesScraper,
    "freshmarket":         PublishedPricesScraper,
    "paz_yellow":          PublishedPricesScraper,
    "super_yuda":          PublishedPricesScraper,
    "stop_market":         PublishedPricesScraper,   # different Cerberus host; scraper reads from spec.portal_url
    "victory":             LaibcatalogScraper,
    "mega":                MegaScraper,
    "hazi_hinam":          HaziHinamScraper,
    # All binaprojects chains use the same scraper — only the spec differs:
    "king_store":          BinaprojectsScraper,
    "maayan2000":          BinaprojectsScraper,
    "good_pharm":          BinaprojectsScraper,
    "zolvebegadol":        BinaprojectsScraper,
    "supersapir":          BinaprojectsScraper,
    "superbareket":        BinaprojectsScraper,
    "shuk_hayir":          BinaprojectsScraper,
    "shefa_berkat_hashem": BinaprojectsScraper,
    "citymarket_kiryatgat": BinaprojectsScraper,
    "ktshivuk":            BinaprojectsScraper,
}

PUBLISHEDPRICES_CODES = {
    "rami_levi", "yohananof", "tiv_taam", "osher_ad", "keshet",
    "dor_alon", "super_cofix", "politzer", "salach_dabah",
    "freshmarket", "paz_yellow", "super_yuda", "stop_market",
}

# Codes whose portals are on the binaprojects.com platform.
BINAPROJECTS_CODES = {
    "king_store", "maayan2000", "good_pharm", "zolvebegadol",
    "supersapir", "superbareket", "shuk_hayir", "shefa_berkat_hashem",
    "citymarket_kiryatgat", "ktshivuk",
}

# Chains whose HTTPS cert chain doesn't validate on this proot env.
NEEDS_INSECURE = (
    PUBLISHEDPRICES_CODES
    | BINAPROJECTS_CODES
    | {"victory", "mega", "hazi_hinam"}
)


async def run_chain(
    code: str,
    since: datetime,
    limit: int | None,
    kinds: set[str] | None = None,
) -> tuple[int, int]:
    spec = BY_CODE[code]
    scraper_cls = SCRAPERS.get(code)
    if scraper_cls is None:
        console.print(f"[yellow]no scraper yet for {code}; skipping[/yellow]")
        return (0, 0)

    if code == "victory":
        client_cm = make_client_for_laibcatalog()
    elif code in BINAPROJECTS_CODES:
        client_cm = make_client_for_binaprojects()
    elif code == "mega":
        client_cm = make_client_for_mega()
    elif code == "hazi_hinam":
        client_cm = make_client_for_hazi_hinam()
    elif code in NEEDS_INSECURE:
        client_cm = make_client_for_publishedprices()
    else:
        client_cm = httpx.AsyncClient(
            headers={"User-Agent": "super-price-il/0.1 (research)"},
            timeout=httpx.Timeout(connect=15, read=120, write=30, pool=30),
            follow_redirects=True,
        )
    async with client_cm as client:
        scraper = scraper_cls(spec, client)
        files = await scraper.run(since=since, limit=limit, kinds=kinds)

    conn = connect()
    chain_id = chain_id_for_code(conn, code)
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn.execute(
        "INSERT INTO scrape_runs(chain_id, started_at, status) VALUES (?, ?, 'running')",
        (chain_id, started),
    )
    run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    files_ok = files_failed = rows_written = 0
    try:
        for df in files:
            try:
                if df.remote.kind in {"Stores", "StoresFull"}:
                    for sr in stores_parser.parse(df.xml_bytes):
                        upsert_store(conn, chain_id, sr)
                elif df.remote.kind in {"PriceFull", "Price"}:
                    header, rows = pricefull.parse(df.xml_bytes)
                    store_code = df.remote.store_code or header.store_id
                    if not store_code:
                        files_failed += 1
                        continue
                    store_id = get_or_create_store_by_code(conn, chain_id, store_code)
                    rows_written += insert_observations(conn, store_id, rows, str(df.path))
                elif df.remote.kind in {"PromoFull", "Promo"}:
                    header, promos = promofull.parse(df.xml_bytes)
                    store_code = df.remote.store_code or header.store_id
                    store_id = (
                        get_or_create_store_by_code(conn, chain_id, store_code)
                        if store_code else None
                    )
                    rows_written += insert_promotions(conn, chain_id, store_id, promos)
                files_ok += 1
            except Exception as e:
                console.print(f"[red]{df.remote.filename}: {e}[/red]")
                files_failed += 1
        conn.execute(
            "UPDATE scrape_runs SET finished_at=?, status=?, files_ok=?, files_failed=?, rows_written=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(timespec="seconds"),
             "ok" if files_failed == 0 else "partial",
             files_ok, files_failed, rows_written, run_id),
        )
    finally:
        conn.close()

    return files_ok, rows_written


@app.command()
def main(
    chain: str = typer.Option("shufersal", help="chain code or 'all'"),
    days: int = typer.Option(1, help="how many days back to fetch (default: 1)"),
    retain: int = typer.Option(7, help="days of history to keep in DB + raw/ (default: 7)"),
    limit: int = typer.Option(0, help="cap files per chain (0 = no cap)"),
    kinds: str = typer.Option("", help="comma-sep subset: PriceFull,Price,PromoFull,Promo,Stores,StoresFull"),
    no_prune: bool = typer.Option(False, "--no-prune", help="skip retention prune after scrape"),
) -> None:
    migrate()
    seed_chains()

    since = datetime.now(timezone.utc) - timedelta(days=days)
    chains = list(SCRAPERS.keys()) if chain == "all" else [chain]
    cap = limit or None
    kind_set = {k.strip() for k in kinds.split(",") if k.strip()} or None

    for c in chains:
        console.rule(f"[bold]{c}")
        files_ok, rows = asyncio.run(run_chain(c, since, cap, kind_set))
        console.print(f"{c}: files_ok={files_ok} rows={rows}")

    if not no_prune:
        from .prune import prune
        console.rule("[bold]prune")
        prune(retain_days=retain)


if __name__ == "__main__":
    app()
