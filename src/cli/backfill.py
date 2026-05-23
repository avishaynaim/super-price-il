"""Scrape one day of price data per chain (default) and prune anything older than
the retention window. Designed to be invoked daily (cron/systemd timer).

Default policy (2026-04-20): pull yesterday + today, keep last 7 days, drop the rest.
Override with --days / --retain."""
from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from datetime import datetime, timedelta, timezone

import httpx
import typer
from rich.console import Console

from ..db import router as supa  # router auto-falls-back to local SQLite
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
    from ..db.pg import cursor as _cursor
    cutoff = (datetime.now(timezone.utc) - timedelta(days=threshold_days)).isoformat(timespec="seconds")
    with _cursor() as cur:
        # Portable SQL: no DISTINCT ON (Postgres-only), no ::TEXT cast.
        # MAX(finished_at) per chain gives the same result as DISTINCT ON + ORDER BY DESC.
        cur.execute(
            """
            SELECT ch.code, MAX(sr.finished_at) AS finished_at
            FROM scrape_runs sr
            JOIN chains ch ON ch.id = sr.chain_id
            WHERE sr.status = 'ok'
            GROUP BY ch.id, ch.code
            HAVING MAX(sr.finished_at) >= %s
            """,
            (cutoff,),
        )
        last_ok = {r["code"]: r["finished_at"] for r in cur.fetchall() if r["finished_at"]}
    overdue, skipped = [], []
    for code in chains:
        fa = last_ok.get(code)
        if fa and fa >= cutoff:
            skipped.append((code, fa))
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
    PRICE_KINDS = {"PriceFull", "Price"}
    # Stores files are published infrequently; always fetch all of them
    # regardless of the date window when doing a stores-only run.
    effective_since = None if (kinds and kinds <= STORE_KINDS) else since

    # Capture run start BEFORE any inserts. Used at the end to delete rows whose
    # updated_at is older than this — i.e., products not seen in the current run.
    # Doing it this way (delete-AFTER instead of delete-BEFORE) means a crash or
    # timeout during scraping leaves the previous run's prices intact rather than
    # leaving the chain at 0.
    run_start = datetime.now(timezone.utc).isoformat(timespec="seconds")

    files_ok = files_failed = rows_written = 0
    last_heartbeat = 0.0
    HEARTBEAT_EVERY_SEC = 10.0
    # Per-run product barcode→id cache so repeated products across files skip extra round-trips.
    product_cache: dict[str, int] = {}

    try:
        async with client_cm as client:
            scraper = scraper_cls(spec, client)
            # Stream: parse + insert each file as it lands, then drop xml_bytes.
            # Keeps peak memory ~one file (~3 MB) instead of materializing the
            # whole chain (~1+ GB for shufersal/mega) — required on proot.
            async for df in scraper.run(
                since=effective_since, limit=limit, kinds=kinds,
                on_listed=_on_listed, on_downloaded=_on_downloaded,
            ):
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
                finally:
                    df.xml_bytes = b""  # release decompressed buffer ASAP
                    # Delete the gz on disk now that it's been processed; the
                    # raw/ dir would otherwise grow ~200MB per full run.
                    try:
                        if df.path and df.path.exists():
                            df.path.unlink()
                    except Exception:
                        pass

                now_ts = datetime.now(timezone.utc).timestamp()
                if now_ts - last_heartbeat >= HEARTBEAT_EVERY_SEC:
                    try:
                        supa.scrape_run_update(run_id, files_ok=files_ok, files_failed=files_failed, rows_written=rows_written)
                    except Exception:
                        pass
                    last_heartbeat = now_ts
    except Exception as exc:
        msg = f"{type(exc).__name__}: {exc}"
        console.print(f"[red]{code}: scrape failed — {msg}[/red]")
        supa.scrape_run_finish(run_id, "error", files_ok, files_failed, rows_written, error_msg=msg)
        return (files_ok, rows_written)

    # Clean up prices that belong to this chain but were NOT updated in this run
    # (products the chain stopped carrying, or stores that no longer exist).
    # Only fires on full success (files_failed == 0) AND at least one file was
    # processed (files_ok > 0). If the portal returned zero files (transient
    # outage, portal temporarily empty), we keep existing prices rather than
    # wiping them — an empty run must never blank a chain's data.
    if files_failed == 0 and files_ok > 0 and (not kinds or kinds & PRICE_KINDS):
        try:
            supa.delete_stale_chain_prices(chain_id, run_start)
        except Exception as e:
            console.print(f"[dim]stale-price cleanup skipped: {e}[/dim]")

    status = "ok" if files_failed == 0 else "partial"
    supa.scrape_run_finish(run_id, status, files_ok, files_failed, rows_written)
    # Refresh dashboard cache tables for this chain only (fast — <1s per chain).
    try:
        supa.refresh_caches(chain_id)
    except Exception as e:
        console.print(f"[dim]cache refresh skipped: {e}[/dim]")
    return files_ok, rows_written


_console_lock = threading.Lock()


def _run_chain_in_thread(code: str, since: datetime, cap: int | None, kind_set: set[str] | None) -> tuple[int, int]:
    """Run one chain's full scrape in a dedicated thread (each gets its own event loop)."""
    return asyncio.run(run_chain(code, since, cap, kind_set))


def _run_chains_parallel(
    codes: list[str],
    since: datetime,
    cap: int | None,
    kind_set: set[str] | None,
    concurrency: int,
    label: str = "",
    per_chain_timeout: int = 7200,  # 2 hour hard cap per chain
) -> None:
    """Submit all chains to a thread pool; log results as they finish.
    A per-chain timeout prevents one stuck chain (e.g., a hung HTTP request
    on a flaky binaprojects host) from blocking the entire scrape."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(_run_chain_in_thread, c, since, cap, kind_set): c for c in codes}
        # Use a per-future wait so a single hung chain doesn't stall the whole batch.
        pending = set(futures)
        while pending:
            done, pending = concurrent.futures.wait(
                pending, timeout=per_chain_timeout,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            if not done and pending:
                # Nothing finished within the window — skip the still-pending ones.
                with _console_lock:
                    skipped = ", ".join(futures[f] for f in pending)
                    console.print(f"[yellow]timeout {per_chain_timeout}s: abandoning {skipped}{label}[/yellow]")
                for f in pending:
                    f.cancel()
                break
            for fut in done:
                c = futures[fut]
                try:
                    files_ok, rows = fut.result()
                    with _console_lock:
                        console.print(f"[green]{c}{label}[/green]: files_ok={files_ok} rows={rows}")
                except Exception as exc:
                    with _console_lock:
                        console.print(f"[red]{c}{label}: error — {exc}[/red]")


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
    concurrency: int = typer.Option(
        3,
        help="number of chains to scrape in parallel (default 3; raise carefully — each chain opens 6 HTTP connections).",
    ),
) -> None:
    # Seed chains into Supabase on every run (idempotent upsert).
    from ..scraper.registry import CHAINS as _CHAINS
    supa.seed_chains(_CHAINS)

    # days=0 → no date filter: list ALL portal files and let per-store dedup
    # keep only the most recent per (kind, store_code) — even if weeks old.
    since = None if days == 0 else (datetime.now(timezone.utc) - timedelta(days=days))
    if chain == "all":
        chains = list(SCRAPERS.keys())
    else:
        # support comma-separated: --chain shufersal,rami_levi
        chains = [c.strip() for c in chain.split(",") if c.strip() in SCRAPERS]
    cap = limit or None
    kind_set = {k.strip() for k in kinds.split(",") if k.strip()} or None

    if skip_recent > 0:
        chains = _filter_overdue(chains, skip_recent)

    STORE_KINDS = {"Stores", "StoresFull"}
    is_stores_only = bool(kind_set and kind_set <= STORE_KINDS)

    # Auto stores-refresh: parallel pass for chains whose last Stores file is stale.
    if refresh_stores_days > 0 and not is_stores_only:
        stale = _stores_stale_chains(chains, refresh_stores_days)
        if stale:
            console.print(
                f"[dim]stores refresh for {len(stale)} stale chain(s): "
                f"{', '.join(stale)}[/dim]"
            )
            _run_chains_parallel(stale, since, cap, STORE_KINDS, concurrency, label=" (stores)")

    console.print(f"[bold]scraping {len(chains)} chain(s) with concurrency={concurrency}[/bold]")
    _run_chains_parallel(chains, since, cap, kind_set, concurrency)

    if not no_prune:
        from .prune import prune
        effective_retain = retain if retain > 0 else 1
        prune(retain_days=effective_retain)


if __name__ == "__main__":
    app()
