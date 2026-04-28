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

from ..db import supa
from ..parser import pricefull, promofull, stores as stores_parser
from ..scraper.base import RAW_ROOT
from ..scraper.chains.shufersal import ShufersalScraper
from ..scraper.chains.publishedprices import (
    PublishedPricesScraper,
    make_client_for_publishedprices,
)
from ..scraper.chains.laibcatalog import (
    LaibcatalogScraper,
    make_client_for_laibcatalog,
)
from ..scraper.chains.laibcatalog_v2 import (
    LaibcatalogV2Scraper,
    make_client_for_laibcatalog_v2,
)
from ..scraper.chains.binaprojects import (
    BinaprojectsScraper,
    make_client_for_binaprojects,
)
from ..scraper.chains.custom import (
    MegaScraper, make_client_for_mega,
    HaziHinamScraper, make_client_for_hazi_hinam,
    SuperPharmScraper, make_client_for_superpharm,
    WoltScraper, make_client_for_wolt,
    CityMarketScraper, make_client_for_citymarket,
    ChpKtScraper, make_client_for_chpkt,
)
from ..scraper.chains.netiv import NetivScraper, make_client_for_netiv
from ..scraper.registry import BY_CODE

app = typer.Typer(help="Backfill price data into prices.db")
console = Console()

SCRAPERS = {
    # --- fast chains first so they always complete before the giants ---
    # binaprojects: small catalogs, each <5 min
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
    # custom small scrapers
    "chp_kt":              ChpKtScraper,
    "wolt":                WoltScraper,
    "citymarket":          CityMarketScraper,
    "netiv_hahesed":       NetivScraper,
    "cohen_h":             LaibcatalogScraper,
    # laibcatalog v2
    "victory":             LaibcatalogV2Scraper,
    "machsanei_hashuk":    LaibcatalogV2Scraper,
    # medium publishedprices chains (many files but login-gated → smaller sets)
    "super_cofix":         PublishedPricesScraper,
    "paz_yellow":          PublishedPricesScraper,
    "super_yuda":          PublishedPricesScraper,
    "stop_market":         PublishedPricesScraper,
    "politzer":            PublishedPricesScraper,
    "salach_dabah":        PublishedPricesScraper,
    "freshmarket":         PublishedPricesScraper,
    "dor_alon":            PublishedPricesScraper,
    "keshet":              PublishedPricesScraper,
    "osher_ad":            PublishedPricesScraper,
    # medium custom
    "hazi_hinam":          HaziHinamScraper,
    "super_pharm":         SuperPharmScraper,
    # large publishedprices chains
    "yohananof":           PublishedPricesScraper,
    "tiv_taam":            PublishedPricesScraper,
    # --- giants last — if the timer kills them, smaller chains already ran ---
    "mega":                MegaScraper,
    "rami_levi":           PublishedPricesScraper,
    "shufersal":           ShufersalScraper,
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
    | {"victory", "machsanei_hashuk", "cohen_h", "mega", "hazi_hinam",
       "super_pharm", "wolt", "citymarket", "chp_kt", "netiv_hahesed"}
)


def _stores_stale_chains(chains: list[str], stale_days: int) -> list[str]:
    """Return chains where no Stores/StoresFull file exists in data/raw/ within stale_days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=stale_days)
    stale = []
    for code in chains:
        chain_raw = RAW_ROOT / code
        if not chain_raw.exists():
            stale.append(code)
            continue
        latest: datetime | None = None
        for day_dir in chain_raw.iterdir():
            if not day_dir.is_dir():
                continue
            for f in day_dir.iterdir():
                if f.name.lower().startswith(("stores", "storefull")):
                    mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
                    if latest is None or mtime > latest:
                        latest = mtime
        if latest is None or latest < cutoff:
            stale.append(code)
    return stale


def _filter_overdue(chains: list[str], threshold_days: int) -> list[str]:
    """Drop chains that finished a successful scrape within `threshold_days`."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=threshold_days)).isoformat(timespec="seconds")
    res = supa.sb().table("scrape_runs").select("chain_id,finished_at,chains(code)").eq("status", "ok").execute()
    last_ok: dict[str, str] = {}
    for r in (res.data or []):
        code = r.get("chains", {}).get("code", "")
        fa = r.get("finished_at", "")
        if code and fa and fa > last_ok.get(code, ""):
            last_ok[code] = fa
    overdue, skipped = [], []
    for code in chains:
        l = last_ok.get(code)
        if l and l >= cutoff:
            skipped.append((code, l))
        else:
            overdue.append(code)
    if skipped:
        console.print(
            f"[dim]skipping {len(skipped)} recently-scraped chain(s) "
            f"(within {threshold_days}d): {', '.join(c for c, _ in skipped)}[/dim]"
        )
    console.print(f"[bold]running {len(overdue)} chain(s)[/bold] (overdue or never scraped)")
    return overdue


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

    if spec.auth_kind == "laibcatalog_v2":
        client_cm = make_client_for_laibcatalog_v2()
    elif code in {"victory", "machsanei_hashuk", "cohen_h"}:
        client_cm = make_client_for_laibcatalog()
    elif code in BINAPROJECTS_CODES:
        client_cm = make_client_for_binaprojects()
    elif code == "mega":
        client_cm = make_client_for_mega()
    elif code == "hazi_hinam":
        client_cm = make_client_for_hazi_hinam()
    elif code == "super_pharm":
        client_cm = make_client_for_superpharm()
    elif code == "wolt":
        client_cm = make_client_for_wolt()
    elif code == "citymarket":
        client_cm = make_client_for_citymarket()
    elif code == "chp_kt":
        client_cm = make_client_for_chpkt()
    elif code == "netiv_hahesed":
        client_cm = make_client_for_netiv()
    elif code in NEEDS_INSECURE:
        client_cm = make_client_for_publishedprices()
    else:
        client_cm = httpx.AsyncClient(
            headers={"User-Agent": "super-price-il/0.1 (research)"},
            timeout=httpx.Timeout(connect=15, read=120, write=30, pool=30),
            follow_redirects=True,
        )

    chain_id = supa.chain_id_for_code(code)
    run_id = supa.scrape_run_start(chain_id)

    def _on_listed(total: int) -> None:
        try:
            supa.scrape_run_update(run_id, files_total=total)
        except Exception:
            pass

    def _on_downloaded(done: int, total: int) -> None:
        try:
            supa.scrape_run_update(run_id, files_ok=done, files_total=total)
        except Exception:
            pass

    STORE_KINDS = {"Stores", "StoresFull"}
    # Stores files are published infrequently; always fetch all of them
    # regardless of the date window when doing a stores-only run.
    effective_since = None if (kinds and kinds <= STORE_KINDS) else since

    try:
        async with client_cm as client:
            scraper = scraper_cls(spec, client)
            files = await scraper.run(
                since=effective_since, limit=limit, kinds=kinds,
                on_listed=_on_listed, on_downloaded=_on_downloaded,
            )
    except Exception as exc:
        msg = f"{type(exc).__name__}: {exc}"
        console.print(f"[red]{code}: scrape failed — {msg}[/red]")
        supa.scrape_run_finish(run_id, "error", 0, 0, 0, error_msg=msg)
        return (0, 0)

    # Wipe this chain's previous prices before inserting fresh ones (1-day retention).
    supa.delete_chain_current_prices(chain_id)

    files_ok = files_failed = rows_written = 0
    last_heartbeat = 0.0
    HEARTBEAT_EVERY_SEC = 10.0
    # Per-run product barcode→id cache so repeated products across files skip extra round-trips.
    product_cache: dict[str, int] = {}

    for df in files:
        try:
            if df.remote.kind in {"Stores", "StoresFull"}:
                for sr in stores_parser.parse(df.xml_bytes):
                    supa.upsert_store(chain_id, sr)
            elif df.remote.kind in {"PriceFull", "Price"}:
                header, rows = pricefull.parse(df.xml_bytes)
                store_code = df.remote.store_code or header.store_id
                if not store_code:
                    files_failed += 1
                    continue
                store_id = supa.get_or_create_store_by_code(chain_id, store_code)
                rows_written += supa.insert_observations(
                    chain_id, store_id, rows, str(df.path), product_cache
                )
            elif df.remote.kind in {"PromoFull", "Promo"}:
                header, promo_rows = promofull.parse(df.xml_bytes)
                store_code = df.remote.store_code or header.store_id
                store_id = supa.get_or_create_store_by_code(chain_id, store_code) if store_code else None
                rows_written += supa.upsert_promotions(
                    chain_id, store_id, list(promo_rows), product_cache
                )
            files_ok += 1
        except Exception as e:
            console.print(f"[red]{df.remote.filename}: {e}[/red]")
            files_failed += 1

        now_ts = datetime.now(timezone.utc).timestamp()
        if now_ts - last_heartbeat >= HEARTBEAT_EVERY_SEC:
            try:
                supa.scrape_run_update(run_id, files_ok=files_ok, files_failed=files_failed, rows_written=rows_written)
            except Exception:
                pass
            last_heartbeat = now_ts

    status = "ok" if files_failed == 0 else "partial"
    supa.scrape_run_finish(run_id, status, files_ok, files_failed, rows_written)
    # Refresh dashboard cache tables for this chain only (fast — <1s per chain).
    try:
        supa.refresh_caches(chain_id)
    except Exception as e:
        console.print(f"[dim]cache refresh skipped: {e}[/dim]")
    return files_ok, rows_written


@app.command()
def main(
    chain: str = typer.Option("shufersal", help="chain code or 'all'"),
    days: int = typer.Option(1, help="how many days back to fetch (default: 1)"),
    retain: int = typer.Option(0, help="days of history to keep in DB + raw/ (0 = settings.retention_days)"),
    limit: int = typer.Option(0, help="cap files per chain (0 = no cap)"),
    kinds: str = typer.Option("", help="comma-sep subset: PriceFull,Price,PromoFull,Promo,Stores,StoresFull"),
    no_prune: bool = typer.Option(False, "--no-prune", help="skip retention prune after scrape"),
    skip_recent: int = typer.Option(
        0,
        help="skip chains whose last successful scrape finished within N days "
             "(0 = always scrape; weekly cron uses 7).",
    ),
    refresh_stores_days: int = typer.Option(
        7,
        help="also do a stores-only pass for chains whose last Stores file is "
             "older than N days (0 = disabled).",
    ),
) -> None:
    # Seed chains into Supabase on every run (idempotent upsert).
    from ..scraper.registry import CHAINS as _CHAINS
    supa.seed_chains(_CHAINS)

    since = datetime.now(timezone.utc) - timedelta(days=days)
    chains = list(SCRAPERS.keys()) if chain == "all" else [chain]
    cap = limit or None
    kind_set = {k.strip() for k in kinds.split(",") if k.strip()} or None

    if skip_recent > 0:
        chains = _filter_overdue(chains, skip_recent)

    STORE_KINDS = {"Stores", "StoresFull"}
    is_stores_only = bool(kind_set and kind_set <= STORE_KINDS)

    # Auto stores-refresh: before price runs, do a stores-only pass for
    # chains whose last Stores file is older than refresh_stores_days.
    if refresh_stores_days > 0 and not is_stores_only:
        stale = _stores_stale_chains(chains, refresh_stores_days)
        if stale:
            console.print(
                f"[dim]stores refresh for {len(stale)} stale chain(s): "
                f"{', '.join(stale)}[/dim]"
            )
            for c in stale:
                console.rule(f"[dim]{c} (stores refresh)")
                try:
                    files_ok, _ = asyncio.run(run_chain(c, since, cap, STORE_KINDS))
                    console.print(f"{c} stores: files_ok={files_ok}")
                except Exception as exc:
                    console.print(f"[yellow]{c} stores refresh failed ({exc}), skipping[/yellow]")

    for c in chains:
        console.rule(f"[bold]{c}")
        try:
            files_ok, rows = asyncio.run(run_chain(c, since, cap, kind_set))
            console.print(f"{c}: files_ok={files_ok} rows={rows}")
        except Exception as exc:
            console.print(f"[red]{c}: unexpected error — {exc}[/red]")

    if not no_prune:
        from .prune import prune
        prune()


if __name__ == "__main__":
    app()
