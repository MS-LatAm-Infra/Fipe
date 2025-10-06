#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# fleet.py — unified scrapers, parsers, matcher, and FIPE dump (tuples-only capable)
# Logging & progress standardized with logging + tqdm

import argparse
import asyncio
import aiohttp
import json
import time
import random
import math
import hashlib
import logging
from types import SimpleNamespace
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set

import pandas as pd
import numpy as np
import re
import unicodedata
from bs4 import BeautifulSoup
from difflib import SequenceMatcher
import requests
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm  # horizontal progress bars

# -----------------------------------------------------------------------------
# Logging utilities
# -----------------------------------------------------------------------------

def setup_logging(level: Optional[str], verbose: bool):
    if level is None:
        level = "DEBUG" if verbose else "INFO"
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s | %(levelname)8s | %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

def make_bar(total: Optional[int], desc: str, unit: str, enabled: bool):
    """Create a tqdm bar or a no-op proxy if disabled."""
    if not enabled:
        class _NoBar:
            def update(self, n=1): pass
            def set_postfix(self, *a, **k): pass
            def close(self): pass
            def __enter__(self): return self
            def __exit__(self, exc_type, exc, tb): self.close()
        return _NoBar()
    return tqdm(
        total=total,
        desc=desc,
        unit=unit,
        dynamic_ncols=True,
        leave=False,
    )

# -----------------------------------------------------------------------------
# Opinionated defaults & filesystem layout
# -----------------------------------------------------------------------------

try:
    TZ_LOCAL = ZoneInfo("America/Sao_Paulo")
except Exception:
    TZ_LOCAL = timezone(timedelta(hours=-3))

DATA_DIR     = Path("data")
FIPE_DIR     = DATA_DIR / "fipe"
TUPLES_DIR   = DATA_DIR / "tuples"
TABLES_DIR   = DATA_DIR / "tables"
RAW_DIR      = Path("raw")
RAW_LOCALIZA = RAW_DIR / "localiza"
RAW_MOVIDA   = RAW_DIR / "movida"

# Where the auditable match table lives
MATCH_DIR = DATA_DIR / "localiza"
MATCH_DIR.mkdir(parents=True, exist_ok=True)
MATCH_TABLE = DATA_DIR / "localiza_match_table.csv"
VERSION_MATCH_TABLE = DATA_DIR / "localiza_version_match.csv"  # simplified cache (brand_norm, model_norm, version_norm, model_year) → fipe_code (model_token deprecated)


for d in (DATA_DIR, FIPE_DIR, TUPLES_DIR, TABLES_DIR, RAW_LOCALIZA, RAW_MOVIDA):
    d.mkdir(parents=True, exist_ok=True)

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def today_ymd() -> str:
    return date.today().isoformat()

def ymd_compact() -> str:
    return date.today().strftime("%Y%m%d")

def raw_today_path(vendor: str) -> Path:
    """Return raw CSV path for today's snapshot for a given vendor."""
    folder = RAW_LOCALIZA if vendor == "localiza" else RAW_MOVIDA
    name = f"{vendor}_seminovos_{ymd_compact()}.csv"
    return folder / name

def latest(glob_pattern: str, base: Path) -> Optional[Path]:
    files = sorted(base.glob(glob_pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None

def clean_price_to_int(x: Any):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return pd.NA
    if isinstance(x, (int, np.integer)):
        return int(x)
    if isinstance(x, (float, np.floating)):
        return int(round(float(x)))
    s = re.sub(r"[^\d\.,]", "", str(x))
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return int(round(float(s)))
    except Exception:
        return pd.NA

def snapshot_date_from_meta(meta_path: Path) -> str:
    """Return ISO date (YYYY-MM-DD) in local TZ from fetched_at_utc in meta; fallback to local today."""
    if meta_path.exists():
        try:
            ts = json.loads(meta_path.read_text(encoding="utf-8")).get("fetched_at_utc")
            if ts:
                dt_utc = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return dt_utc.astimezone(TZ_LOCAL).date().isoformat()
        except Exception as e:
            logging.getLogger("parse").debug("meta parse fallback: %s", e)
    return date.today().isoformat()

def match_today_candidates() -> list[Path]:
    """Return potential filenames for today's Localiza match output (case variants)."""
    ymd = ymd_compact()
    return [
        MATCH_DIR / f"localiza_with_fipe_match_{ymd}.csv",
        MATCH_DIR / f"Localiza_with_fipe_match_{ymd}.csv",
    ]

def find_today_match_csv() -> Optional[Path]:
    for p in match_today_candidates():
        if p.exists():
            return p
    return None

def match_table_columns():
    return [
        # Key (must uniquely identify a mapping)
        "brand_norm", "model_norm", "version_norm", "model_year",
        # Human context
        "brand_sample", "model_sample", "version_sample",
        # Result
        "fipe_model", "fipe_code", "fipe_year", "score",
        # Meta
        "status",          # "matched" | "unmatched"
        "match_source",    # "auto" | "manual"
        "first_seen",      # YYYY-MM-DD
        "last_seen",       # YYYY-MM-DD
        "threshold_used",
    ]

PT_MONTHS = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
    "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
}
_MONTH_ALTS = "|".join(sorted(PT_MONTHS.keys(), key=len, reverse=True))

def parse_mes_label(label: str) -> tuple[int, int]:
    """
    Parse FIPE month labels like:
      - 'agosto de 2025'
      - 'agosto/2025'
      - 'agosto 2025'
      - 'Agosto/2025   ' (extra spaces / NBSP)
    Fallback to (1900, 1) if unknown.
    """
    s = (label or "").lower().replace("\xa0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    s_norm = re.sub(r"[/-]", " ", s)

    m = re.search(rf"\b({_MONTH_ALTS})\b(?:\s+de)?\s+(\d{{4}})\b", s_norm)
    if m:
        month_word, year = m.group(1), int(m.group(2))
        return (year, PT_MONTHS.get(month_word, 1))

    m = re.search(r"\b(1[0-2]|0?[1-9])\s+(\d{4})\b", s_norm)
    if m:
        month_num, year = int(m.group(1)), int(m.group(2))
        return (year, month_num)

    return (1900, 1)

# -----------------------------------------------------------------------------
# Localiza scraper (parses __NEXT_DATA__)
# -----------------------------------------------------------------------------

LOCALIZA_HEADERS = {
    "user-agent": "Mozilla/5.0",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "pt-BR,pt;q=0.9,en;q=0.8",
    "referer": "https://seminovos.localiza.com/",
}
log_loc = logging.getLogger("scrape.localiza")

async def _loc_fetch_page(session: aiohttp.ClientSession, page: int) -> Dict[str, Any]:
    """Fetch and parse Localiza listing page with polite retries and jitter.

    Uses exponential backoff, respects Retry-After on 429, and adds a small
    jittered delay between attempts to avoid hammering.
    """
    url = f"https://seminovos.localiza.com/carros?page={page}"
    max_retries = 3
    base_delay = 1.0
    # Allow caller to stash a RateLimiter on the session for global pacing
    limiter: Optional[RateLimiter] = getattr(session, "_rate_limiter", None)

    for attempt in range(max_retries):
        try:
            if limiter:
                await limiter.wait()
            async with session.get(url, headers=LOCALIZA_HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    soup = BeautifulSoup(html, "html.parser")
                    tag = soup.find("script", {"id": "__NEXT_DATA__"})
                    if not tag or not tag.string:
                        raise RuntimeError("Could not find __NEXT_DATA__ on page")
                    data = json.loads(tag.string)
                    return data
                # Too many requests – honor Retry-After when provided
                if resp.status == 429:
                    retry_after = resp.headers.get("Retry-After")
                    ra = 0.0
                    try:
                        ra = float(retry_after) if retry_after else 0.0
                    except Exception:
                        ra = 0.0
                    await asyncio.sleep(max(ra, base_delay) * (0.8 + 0.6 * random.random()))
                    continue
                # Transient server errors – backoff and retry
                if resp.status in (500, 502, 503, 504):
                    await asyncio.sleep(min(30.0, (base_delay * (2 ** attempt))) * (0.8 + 0.6 * random.random()))
                    continue
                # Other HTTP errors are treated as fatal
                txt = await resp.text()
                raise RuntimeError(f"HTTP {resp.status} for Localiza page={page}: {txt[:200]}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if attempt >= max_retries - 1:
                raise
            # Network or parse error – small jittered backoff
            await asyncio.sleep(min(30.0, (base_delay * (2 ** attempt))) * (0.8 + 0.6 * random.random()))
    # Should not reach here
    raise RuntimeError("Exceeded retries fetching Localiza page")

def _loc_pull_products(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    p = data.get("props", {}).get("pageProps", {})
    products = p.get("products") or p.get("Product") or []
    return products if isinstance(products, list) else []

def _loc_total_pages(data: Dict[str, Any]) -> int:
    p = data.get("props", {}).get("pageProps", {})
    meta = p.get("_metadados") or p.get("metadados") or {}
    total = meta.get("_totalPaginas") or meta.get("totalPaginas") or 1
    try:
        return int(total)
    except Exception:
        return 1

async def scrape_localiza(args):
    out = Path(args.out)
    # Configurable pacing knobs with conservative defaults
    concurrency = max(1, int(getattr(args, "concurrency", 3)))
    rps = float(getattr(args, "rps", 0.8))  # requests per second (shared)
    jitter_sleep = float(getattr(args, "delay", 1.0))

    connector = aiohttp.TCPConnector(limit=concurrency, limit_per_host=concurrency, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Attach a RateLimiter to the session to be used inside fetches
        setattr(session, "_rate_limiter", RateLimiter(rps))

        # Initial fetch (page 1) with throttling
        first = await _loc_fetch_page(session, 1)
        total_pages = _loc_total_pages(first)
        products = _loc_pull_products(first)
        log_loc.info("total pages=%s; page1 products=%s", total_pages, len(products))

        localiza_offers = []
        seen = set()
        for pr in products:
            pid = pr.get("id")
            if pid in seen: continue
            seen.add(pid); localiza_offers.append(pr)

        with make_bar(total_pages, "localiza pages", "page", not args.no_progress) as bar:
            bar.update(1)  # page 1 done
            q: asyncio.Queue = asyncio.Queue()
            for pg in range(2, total_pages + 1):
                q.put_nowait(pg)

            async def worker():
                while True:
                    try:
                        pg = q.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    try:
                        data = await _loc_fetch_page(session, pg)
                        prods = _loc_pull_products(data)
                        added = 0
                        for pr in prods:
                            pid = pr.get("id")
                            if pid in seen: continue
                            seen.add(pid); localiza_offers.append(pr); added += 1
                        log_loc.debug("page %s -> %s products (new %s)", pg, len(prods), added)
                    except Exception as e:
                        log_loc.warning("page %s error: %s", pg, e)
                    finally:
                        bar.update(1)
                        q.task_done()
                    # Gentle jitter between page fetches to avoid bursts
                    await asyncio.sleep(max(0.2, jitter_sleep * (0.6 + 0.6 * random.random())))

            workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
            await q.join()
            for w in workers:
                w.cancel()

    out.write_text(json.dumps(localiza_offers, ensure_ascii=False, indent=2), encoding="utf-8")
    meta = {
        "fetched_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "count_saved": len(localiza_offers),
        "pages_fetched": total_pages,
    }
    out.with_suffix(".meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    log_loc.info("saved %s offers -> %s", len(localiza_offers), out)

# -----------------------------------------------------------------------------
# Movida scraper (API walker with retry)
# -----------------------------------------------------------------------------

MOVIDA_URL = "https://be-seminovos.movidacloud.com.br/elasticsearch/veiculos"
MOVIDA_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "content-type": "application/json",
    "origin": "https://www.seminovosmovida.com.br",
    "priority": "u=1, i",
    "referer": "https://www.seminovosmovida.com.br/",
    "sec-ch-ua": '"Not;A=Brand";v="99", "Microsoft Edge";v="139", "Chromium";v="139"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0",
}
log_mov = logging.getLogger("scrape.movida")

def _movida_payload(offset, gad_source=None, gad_campaignid=None, gbraid=None, gclid=None):
    payload = {
        "localizacao": {"latitude": None, "longitude": None, "uf": None},
        "from": (None if offset is None else int(offset)),
    }
    if gad_source is not None: payload["gad_source"] = gad_source
    if gad_campaignid is not None: payload["gad_campaignid"] = gad_campaignid
    if gbraid is not None: payload["gbraid"] = gbraid
    if gclid is not None: payload["gclid"] = gclid
    return payload

def _movida_session(max_retries=5, backoff_factor=1.0):
    sess = requests.Session()
    retry = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter); sess.mount("http://", adapter)
    return sess

def scrape_movida(args):
    out = Path(args.out)
    # More conservative retry/backoff for public API
    sess = _movida_session(max_retries=6, backoff_factor=1.5)
    offset = None
    all_offers = []
    seen = set()
    pages = 0
    first_page_size = None
    total_from_api = None
    # Jittered delay helper
    def _jittered_delay(base: float) -> float:
        return max(0.2, float(base) * (0.7 + 0.6 * random.random()))

    bar = None
    try:
        consecutive_429 = 0
        while True:
            payload = _movida_payload(offset, args.gad_source, args.gad_campaignid, args.gbraid, args.gclid)
            log_mov.debug("requesting offset=%r", payload["from"])
            resp = sess.post(MOVIDA_URL, headers=MOVIDA_HEADERS, json=payload, timeout=30)
            if resp.status_code != 200:
                if resp.status_code == 429:
                    consecutive_429 += 1
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        ra = float(retry_after) if retry_after else 0.0
                    except Exception:
                        ra = 0.0
                    sleep_s = max(ra, _jittered_delay(getattr(args, "delay", 1.2)) * (2 ** min(consecutive_429, 4)))
                    log_mov.warning("HTTP 429 Too Many Requests — sleeping %.1fs (attempt %d)", sleep_s, consecutive_429)
                    time.sleep(min(60.0, sleep_s))
                    # retry same offset
                    continue
                log_mov.warning("HTTP %s — stopping", resp.status_code)
                break

            try:
                data = resp.json()
            except Exception as e:
                log_mov.error("JSON parse error: %s", e); break

            offers = data.get("data", [])
            if first_page_size is None:
                first_page_size = len(offers)
            if total_from_api is None and isinstance(data.get("total"), dict):
                total_from_api = data["total"].get("value")

            if bar is None and first_page_size:
                est_pages = None
                if total_from_api:
                    try:
                        est_pages = max(1, math.ceil(int(total_from_api) / max(1, first_page_size)))
                    except Exception:
                        est_pages = None
                bar = make_bar(est_pages, "movida pages", "page", not args.no_progress)

            new_count = 0
            for offer in offers:
                oid = offer.get("id")
                if oid is None or oid in seen: continue
                seen.add(oid); all_offers.append(offer); new_count += 1

            pages += 1
            if bar: bar.update(1)
            log_mov.debug("page %s: got=%s new=%s acc=%s", pages, len(offers), new_count, len(all_offers))
            consecutive_429 = 0  # reset on successful page

            links = data.get("links") or {}
            next_page = links.get("next_page")

            if not offers:
                log_mov.info("empty data -> done.")
                break
            if next_page is None:
                log_mov.info("no next_page -> done.")
                break
            if args.max_pages is not None and pages >= args.max_pages:
                log_mov.info("reached max_pages=%s -> stop.", args.max_pages)
                break

            try:
                next_offset = int(next_page)
            except Exception:
                inc = first_page_size if first_page_size else 20
                next_offset = (offset or 0) + inc

            if next_offset == offset:
                log_mov.warning("next==current; break.")
                break

            offset = next_offset
            time.sleep(_jittered_delay(getattr(args, "delay", 1.2)))
    finally:
        if bar: bar.close()

    out.write_text(json.dumps(all_offers, ensure_ascii=False, indent=2), encoding="utf-8")
    meta = {
        "fetched_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "count_saved": len(all_offers),
        "total_reported_by_api": total_from_api,
        "pages_fetched": pages,
    }
    out.with_suffix(".meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    log_mov.info("saved %s offers -> %s", len(all_offers), out)

# -----------------------------------------------------------------------------
# Parsers (to dated CSVs)
# -----------------------------------------------------------------------------

log_parse = logging.getLogger("parse")

def parse_movida(in_json: Path, out_dir: Path):
    meta = in_json.with_suffix(".meta.json")
    snap = snapshot_date_from_meta(meta)

    txt = in_json.read_text(encoding="utf-8")
    try:
        raw = json.loads(txt)
    except json.JSONDecodeError:
        raw = [json.loads(line) for line in txt.splitlines() if line.strip()]

    df = pd.json_normalize(raw, sep="_")
    if df.empty:
        log_parse.warning("[movida] no rows to write")
        return

    # Keep only rows with essential fields
    if {"modelo","versao","ano_modelo"}.issubset(df.columns):
        df = df[df["modelo"].notna() & df["versao"].notna() & df["ano_modelo"].notna()].copy()

    # Convert nested structures to JSON strings to preserve data
    for c in df.columns:
        df[c] = df[c].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)

    df["offer_id"] = df.get("id")
    df["fipe_code"] = df.get("codigo_fipe")
    df["type"] = df.get("categoria")
    df["brand"] = df.get("marca")
    df["model"] = df.get("modelo")
    df["version_raw"] = df.get("versao")
    df["manufacture_year"] = df.get("ano_fabricacao")
    df["model_year"] = df.get("ano_modelo")
    df["price"] = df.get("preco")

    df["price"] = df["price"].apply(clean_price_to_int).astype("Int64")
    df["manufacture_year"] = pd.to_numeric(df["manufacture_year"], errors="coerce").astype("Int64")
    df["model_year"] = pd.to_numeric(df["model_year"], errors="coerce").astype("Int64")

    for col in ["type","brand","model","version_raw"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()

    df["version"] = (
        df.get("model", "").astype(str).str.strip() + " " + df.get("version_raw", "").astype(str).str.strip()
    ).str.replace(r"\s+", " ", regex=True).str.strip()

    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"])

    df.insert(0, "snapshot_date", snap)

    ensure_dir(out_dir)
    out = out_dir / f"movida_seminovos_{snap.replace('-', '')}.csv"
    df.to_csv(out, index=False, sep=";")
    log_parse.info("[movida] wrote %s rows -> %s", len(df), out)



def parse_localiza(in_json: Path, out_dir: Path):
    meta = in_json.with_suffix(".meta.json")
    snap = snapshot_date_from_meta(meta)  # use meta date; do NOT overwrite with today
    raw = json.loads(in_json.read_text(encoding="utf-8"))
    df = pd.json_normalize(raw, sep="_")
    if df.empty:
        log_parse.warning("[localiza] no rows to write")
        return
    if {"modeloFamiliaDescricao","modeloDescricao","anoModelo"}.issubset(df.columns):
        df = df[df["modeloFamiliaDescricao"].notna() & df["modeloDescricao"].notna() & df["anoModelo"].notna()].copy()
    for c in df.columns:
        df[c] = df[c].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x,(list,dict)) else x)
    df["type"] = df.get("categoriaDescricao")
    df["brand"] = df.get("marcaDescricao")
    df["model"] = df.get("modeloFamiliaDescricao")
    df["version_raw"] = df.get("modeloDescricao")
    df["manufacture_year"] = df.get("anoFabricacao")
    df["model_year"] = df.get("anoModelo")
    df["price"] = df.get("preco")
    df["fuel"] = df.get("tipoCombustivelDescricao")
    df["transmission"] = df.get("tipoTransmissaoDescricao")
    df["version"] = df["version_raw"].copy()
    df["type"] = np.where((df["version"] == "ONIX LT MT 1.0 4P"), "HATCH", df["type"])
    df["type"] = np.where((df["version"] == "ONIX PLUS TURBO LT AT 1.0 4P"), "SEDAN", df["type"])
    df["type"] = np.where((df["version"] == "HB20S PLATINUM TGDI AT 1.0 4P"), "SEDAN", df["type"])
    df["type"] = df["type"].replace(to_replace=["FURGAO", "PICAPE", "PICAPE CABINE DUPLA", "CABINE SIMPLES", "SPORTBACK", "PARTICULAR", "COUPE", "CARGA"],
                                    value=["UTILITARIO", "PICK-UP", "PICK-UP", "PICK-UP", "PREMIUM", "VAN", "PREMIUM", "VAN"])
    df["transmission"] = df["transmission"].replace(to_replace=["MANUAL", "AUTOMÁTICO", "AUTOMATICO", "AUTOMáTICO", "TIPTRONIC", "ELECTRICO"],
                                    value=["mec.", "aut.", "aut.", "aut.", "tiptr.", "(eletrico)"])
    df["fuel"] = df["fuel"].replace(to_replace=["GASOLINA/ETANOL", "ÁLC/GASOL", "FLEX/GNV", "ETANOL/GNV", "ELÉT/GASOLINA/ETANOL", "ELÉTRICO/GASOLINA", "ELÉTRICO", "GASOLINA"],
                                    value=["flex", "flex", "flex", "flex", "(hibrido)", "(hib.)", "(elétrico)", ""])
    df["version"] = df["version"].astype("string").str.replace(r"\bonix[\s-]+plus\b", "onix sedan plus", regex=True, flags=re.IGNORECASE)
    df["version"] = df["version"].str.replace(r"\bonix\b(?![\s-]*(?:sedan|hatch))","onix hatch",regex=True,flags=re.IGNORECASE)
    df["version"] = df["version"].astype("string").str.replace(" mt ", " mec ", regex=False, flags=re.IGNORECASE)
    df["price"] = df["price"].apply(clean_price_to_int).astype("Int64")
    df["manufacture_year"] = pd.to_numeric(df["manufacture_year"], errors="coerce").astype("Int64")
    df["model_year"] = pd.to_numeric(df["model_year"], errors="coerce").astype("Int64")
    df["version"] = (df["version_raw"].str.replace(r"\bonix[\s-]+plus\b","onix sedan plus", regex=True, flags=re.IGNORECASE)
                                  .str.replace(r"\bonix\b(?![\s-]*(?:sedan|hatch))","onix hatch", regex=True, flags=re.IGNORECASE)
                                  .str.replace(" mt "," mec ", regex=False))
    for c in ["type","brand","model","version_raw","fuel","transmission"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.lower()
    df["version"] = (df["version"].astype(str).str.strip()
                     + " " + df["fuel"].astype(str).str.strip()
                     + " " + df["transmission"].astype(str).str.strip()).str.replace(r"\\s+", " ", regex=True).str.strip()
    df.insert(0, "snapshot_date", snap)
    ensure_dir(out_dir)
    out = out_dir / f"localiza_seminovos_{snap.replace('-', '')}.csv"
    df.to_csv(out, index=False, sep=";")
    log_parse.info("[localiza] wrote %s rows -> %s", len(df), out)
# -----------------------------------------------------------------------------
# Normalization helpers for matching
# -----------------------------------------------------------------------------

def strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", str(s))
        if unicodedata.category(c) != "Mn"
    )

def remove_duplicate_words(text: str) -> str:
    seen = set()
    result = []
    for word in text.split():
        key = word.casefold()
        if key not in seen:
            seen.add(key)
            result.append(word)
    return " ".join(result)

def norm_text(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s0 = strip_accents(str(s).lower())
    s0 = s0.replace(",", ".")
    s0 = s0.replace("c/ar", "")
    s0 = s0.replace("c/ ar", "")
    s0 = re.sub(r"\bhb\s*20\s*x\b", "hb20x", s0)
    s0 = re.sub(r'/', ' ', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:T\.)(?!\w)', 'turbo', s0)
    s0 = re.sub(r"[^a-z0-9\.\s]", " ", s0)
    s0 = re.sub(r"\bautomatic[oa]\b|\bat\b|\baut(?:\.|o)?\b", "aut", s0)
    s0 = re.sub(r"\bman(?:ual)?\b|\bmecanico\b", "mec", s0)
    s0 = re.sub(r"\bt\s?si\b", "tsi", s0)
    s0 = re.sub(r"\b(\d{2,4}(?:i|d))\s*a\b", r"\1 aut", s0)
    s0 = re.sub(r'(?<=[A-Za-z])\.(?=[A-Za-z])', '. ', s0)
    s0 = re.sub(r'(?<=[A-Za-z])\.', '', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:perf|perfor|performa|performance|p)(?!\w)', 'performance', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:long)(?!\w)', 'longitude', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:sportb|SPB|SB)(?!\w)', 'sportback', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:prest)(?!\w)', 'prestige', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:ultim)(?!\w)', 'ultimate', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:insc)(?!\w)', 'inscription', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:xdrive30e)(?!\w)', 'xdrive 30e', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:cp)(?!\w)', 'cs plus', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:7l)(?!\w)', '', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:hurric|hurr)(?!\w)', 'hurricane', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:overl)(?!\w)', 'overland', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:dies|die)(?!\w)', 'diesel', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:tb)(?!\w)', 'turbo', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:sed)(?!\w)', 'sedan', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:step)(?!\w)', 'stepway', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:hig)(?!\w)', 'highline', s0)  # fix typo
    s0 = re.sub(r'(?i)(?<!\w)(?:limit)(?!\w)', 'limited', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:plat)(?!\w)', 'platinum', s0)
    s0 = re.sub(r'(?i)\b\d+[pv]\b', '', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:exclu)(?!\w)', 'exclusive', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:t270)(?!\w)', 'turbo 270', s0)
    s0 = re.sub(r'(?i)(?<!\w)(?:comfort|comfor)(?!\w)', 'comfortline', s0)
    s0 = re.sub(r'(?i)\bONIX\s+HATCH\s+PREM\.\b', 'onix hatch premier', s0)
    s0 = re.sub(r'(?i)\bONIX\s+SEDAN\s+PREM\.\b', 'onix sedan premier', s0)
    s0 = re.sub(r'(?i)\bONIX\s+SEDAN\s+Plus+\s+PREM\.\b', 'onix sedan plus premier', s0)
    s0 = re.sub(r'(?i)\bONIX\s+SD\.+\s+P\.+\s+PR\.\b', 'onix sedan plus premier', s0)
    s0 = re.sub(r'(?i)\bFastback\s+Limited+\s+Ed\.\b', 'fastback limited edition', s0)
    s0 = re.sub(r'(?i)\bAIRCROSS\s+F\.\b', 'aircross feel', s0)
    s0 = re.sub(r'(?<=xc)(\d+)', r' \1', s0)
    s0 = re.sub(r'\bnew\b(?![\s-]*(?:range|beetle)\b)', '', s0, flags=re.IGNORECASE)
    s0 = remove_duplicate_words(s0)
    s0 = re.sub(r"\s+", " ", s0).strip()
    return s0

# Generic lighter normalization for non-version textual fields (brand, model token, etc.)
def generic_norm_text(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s0 = strip_accents(str(s).lower())
    # Keep alphanumerics and spaces; avoid aggressive replacements that are version-specific
    s0 = re.sub(r"[^a-z0-9\s]", " ", s0)
    s0 = re.sub(r'(?<=xc)(\d+)', r' \1', s0)
    s0 = re.sub(r"\s+", " ", s0).strip()
    return s0

def first_token(s: str) -> str:
    # Use generic normalization so model token not distorted by version-specific rules
    s0 = generic_norm_text(s)
    return s0.split(" ")[0] if s0 else ""

def remove_token_whole_word(s: str, tok: str) -> str:
    if not tok:
        return s
    return re.sub(rf"\b{re.escape(tok)}\b", " ", s).replace("  "," ").strip()

def tokset(s: str) -> set:
    # Token sets for non-version comparisons should rely on generic normalization
    return set(generic_norm_text(s).split())

def extract_engine(s: str) -> Optional[str]:
    m = re.search(r"\b(\d\.\d)\b", s)
    return m.group(1) if m else None

def norm_brand(s: str) -> str:
    # Brands should not undergo aggressive version-level normalization
    s0 = generic_norm_text(s)
    aliases = {
    # Volkswagen
    "vw - volkswagen":"volkswagen","vw volkswagen":"volkswagen","volks":"volkswagen","volkswagem":"volkswagen",
    # Chevrolet / GM
    "gm":"chevrolet","gm - chevrolet":"chevrolet","gm chevrolet":"chevrolet","chevy":"chevrolet",
        "mercedes-benz":"mercedes benz","mb":"mercedes benz","caoa chery":"chery",
        "caoa chery/chery": "chery", "great wall": "gwm"
    }
    return aliases.get(s0, s0)

# -----------------------------------------------------------------------------
# fipe_models.csv discovery & robust loader (cars only)
# -----------------------------------------------------------------------------

def find_fipe_models_csv(explicit: Optional[str] = None) -> Optional[Path]:
    if explicit:
        p = Path(explicit)
        return p if p.exists() else None
    for candidate in (DATA_DIR / "fipe_models.csv", Path("fipe_models.csv")):
        if candidate.exists():
            return candidate
    return None

def load_fipe_models_df(path: Path) -> pd.DataFrame:
    """
    Robust loader for fipe_models.csv. Guarantees columns:
      - 'Marca', 'Modelo', 'CodigoFipe', 'AnoModelo'
    Adds helpers: '_brand_norm', '_model_norm', '_toks', '_engine'
    Filters cars (CodigoTipoVeiculo == '1') if column exists.
    """
    try:
        df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
    except Exception:
        df = pd.read_csv(path, sep=";", encoding="utf-8-sig")

    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    idx = {c.casefold(): c for c in df.columns}

    def pick(*names):
        for n in names:
            if n is None:
                continue
            c = idx.get(str(n).casefold())
            if c:
                return c
        return None

    col_marca      = pick("Marca", "marca")
    col_modelo     = pick("Modelo", "modelo")
    col_codfipe    = pick("CodigoFipe", "codigo_fipe", "codigo fipe", "Codigo Fipe")
    col_anomodelo  = pick("AnoModelo", "AnoModeloParam", "ano_modelo", "ano modelo", "anoModelo")
    col_tipo       = pick("CodigoTipoVeiculo", "codigo_tipo_veiculo", "tipo", "tipo_veiculo")

    missing = [name for name, col in {
        "Marca": col_marca, "Modelo": col_modelo, "CodigoFipe": col_codfipe
    }.items() if col is None]
    if missing:
        raise SystemExit(f"fipe_models.csv is missing required columns: {', '.join(missing)}")
    if col_anomodelo is None:
        raise SystemExit("fipe_models.csv does not contain a year column (expected one of: "
                         "'AnoModelo', 'AnoModeloParam', 'ano_modelo').")

    if col_marca != "Marca":
        df.rename(columns={col_marca: "Marca"}, inplace=True)
    if col_modelo != "Modelo":
        df.rename(columns={col_modelo: "Modelo"}, inplace=True)
    if col_codfipe != "CodigoFipe":
        df.rename(columns={col_codfipe: "CodigoFipe"}, inplace=True)
    if col_anomodelo != "AnoModelo":
        df.rename(columns={col_anomodelo: "AnoModelo"}, inplace=True)

    if col_tipo:
        if col_tipo != "CodigoTipoVeiculo":
            df.rename(columns={col_tipo: "CodigoTipoVeiculo"}, inplace=True)
        #df = df[df["CodigoTipoVeiculo"].astype(str) == "1"].copy()

    df["Marca"]      = df["Marca"].astype(str).str.strip().str.lower()
    df["Modelo"]     = df["Modelo"].astype(str).str.strip().str.lower()
    df["CodigoFipe"] = df["CodigoFipe"].astype(str).str.strip()
    df["AnoModelo"]  = pd.to_numeric(df["AnoModelo"], errors="coerce").astype("Int64")


    df = df[df["CodigoFipe"].ne("") & df["Modelo"].ne("") & df["AnoModelo"].notna()].copy()
    df["_brand_norm"] = df["Marca"].map(norm_brand)
    df["_model_norm"] = df["Modelo"].map(norm_text)
    df["_toks"]       = df["_model_norm"].str.split().apply(set)
    df["_engine"]     = df["_model_norm"].map(extract_engine)
    
    if (df["_model_norm"] == "").all():
        logging.getLogger("fipe.load").warning("All _model_norm empty after normalization (check strip_accents / norm_text).")

    df = df.drop_duplicates(subset=["CodigoFipe","Modelo","AnoModelo"]).reset_index(drop=True)

    return df

log_mtbl = logging.getLogger("match.table")

def load_match_table() -> pd.DataFrame:
    if MATCH_TABLE.exists():
        df = pd.read_csv(MATCH_TABLE, sep=";")
        missing = [c for c in match_table_columns() if c not in df.columns]
        for c in missing:
            df[c] = pd.NA
        df["model_year"] = pd.to_numeric(df["model_year"], errors="coerce").astype("Int64")
        return df[match_table_columns()]
    return pd.DataFrame(columns=match_table_columns())

def save_match_table(df: pd.DataFrame):
    df = df[match_table_columns()].copy()
    df.sort_values(["brand_norm","model_norm","version_norm","model_year"], inplace=True)
    df.to_csv(MATCH_TABLE, index=False, sep=";")
    log_mtbl.info("match table saved: %s rows -> %s", len(df), MATCH_TABLE)

def score_best_fipe_for_key(
    brand_norm: str, model_norm: str, version_norm: str, model_year: int,
    fipe_df: pd.DataFrame, threshold: float
) -> tuple[Optional[str], Optional[str], Optional[int], float, str]:
    """
    Returns (fipe_model, fipe_code, fipe_year, score, status)
    status: 'matched' or 'unmatched'
    """
    if not version_norm or not model_norm or pd.isna(model_year):
        return (None, None, None, 0.0, "unmatched")
    # All tokens from model_norm must be present (order-agnostic) in FIPE _model_norm
    m_tokens = [re.escape(t) for t in model_norm.split() if t]
    if not m_tokens:
        return (None, None, None, 0.0, "unmatched")
    token_pattern = "(?=.*" + ")(?=.*".join(m_tokens) + ")"
    cand_mask = (
        (fipe_df["_brand_norm"] == brand_norm) &
        (fipe_df["AnoModelo"] == int(model_year)) &
        fipe_df["_model_norm"].str.contains(token_pattern, regex=True, na=False)
    )
    cand = fipe_df[cand_mask]
    if cand.empty:
        return (None, None, None, 0.0, "unmatched")

    v_toks = tokset(version_norm)
    v_engine = extract_engine(version_norm)
    s_best = -1.0; m_best = c_best = None; y_best: Optional[int] = None

    q_toks_base = v_toks

    for _, fr in cand.iterrows():
        # Use full model_norm – do not strip tokens
        fipe_model_norm = fr["_model_norm"]
        c_toks = fr["_toks"]

        q_toks = q_toks_base
        inter = len(q_toks & c_toks)
        coverage  = inter / len(q_toks) if q_toks else 0.0
        precision = inter / len(c_toks) if c_toks else 0.0
        if coverage < 0.05:
            continue
        f1 = (2*precision*coverage/(precision+coverage)) if (precision+coverage) else 0.0
        jacc = (len(q_toks & c_toks)/len(q_toks | c_toks)) if (q_toks or c_toks) else 0.0

        base = SequenceMatcher(None, version_norm, fipe_model_norm).ratio()
        score = 0.55*f1 + 0.20*jacc + 0.25*base

        c_engine = fr["_engine"]
        if v_engine:
            score += 0.05 if c_engine == v_engine else (-0.10 if c_engine is not None else 0.0)
        if "gp" in v_toks and "gp" in c_toks:
            score += 0.03
        score -= min(0.12, 0.02 * len(c_toks - q_toks))

        if score > s_best:
            s_best = score
            m_best = fr["Modelo"]
            c_best = fr["CodigoFipe"]
            y_best = int(fr["AnoModelo"]) if pd.notna(fr["AnoModelo"]) else None

    if s_best >= threshold and m_best and c_best and y_best is not None:
        return (m_best, c_best, y_best, float(round(s_best, 4)), "matched")
    return (None, None, None, float(max(s_best, 0.0)), "unmatched")

def update_match_table_with_localiza_keys(loc_df: pd.DataFrame, fipe_df: pd.DataFrame, threshold: float):
    tbl = load_match_table()
    today_iso = date.today().isoformat()

    keys = (loc_df[["_brand_norm","_model_norm","_version_norm","model_year","brand","model","version"]]
            .drop_duplicates()
            .rename(columns={
                "_brand_norm":"brand_norm",
                "_model_norm":"model_norm",
                "_version_norm":"version_norm",
                "brand":"brand_sample",
                "model":"model_sample",
                "version":"version_sample",
            }))
    keys["model_year"] = pd.to_numeric(keys["model_year"], errors="coerce").astype("Int64")
    tbl["model_year"]  = pd.to_numeric(tbl.get("model_year"), errors="coerce").astype("Int64")

    merged = keys.merge(
        tbl[["brand_norm","model_norm","version_norm","model_year"]],
        on=["brand_norm","model_norm","version_norm","model_year"],
        how="left",
        indicator=True
    )

    new_keys = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

    if not new_keys.empty:
        logging.getLogger("match.table").info("new localiza versions to match: %d", len(new_keys))
        rows = []
        for _, r in new_keys.iterrows():
            m, c, y, s, status = score_best_fipe_for_key(
                r["brand_norm"], r["model_norm"], r["version_norm"],
                int(r["model_year"]) if pd.notna(r["model_year"]) else None,
                fipe_df, threshold
            )
            rows.append({
                "brand_norm": r["brand_norm"],
                "model_norm": r["model_norm"],
                "version_norm": r["version_norm"],
                "model_year":  r["model_year"],
                "brand_sample": r["brand_sample"],
                "model_sample": r["model_sample"],
                "version_sample": r["version_sample"],
                "fipe_model": m,
                "fipe_code":  c,
                "fipe_year":  y,
                "score":      s,
                "status":     status,
                "match_source": "auto",
                "first_seen":  today_iso,
                "last_seen":   today_iso,
                "threshold_used": threshold,
            })

        if rows:
            add = pd.DataFrame(rows)
            add["model_year"] = pd.to_numeric(add["model_year"], errors="coerce").astype("Int64")
            for col in match_table_columns():
                if col not in add.columns:
                    add[col] = pd.NA
                if col not in tbl.columns:
                    tbl[col] = pd.NA
            # align columns & dtypes before concat (silence FutureWarning)
            add = add.reindex(columns=tbl.columns)
            try:
                add = add.astype(tbl.dtypes.to_dict(), errors="ignore")
            except TypeError:
                add = add.astype({c: tbl[c].dtype for c in add.columns if c in tbl.columns})
            if len(tbl) == 0:
                tbl = add.copy()
            else:
                tbl = pd.concat([tbl, add], ignore_index=True, sort=False)

    kcols = ["brand_norm", "model_norm", "version_norm", "model_year"]
    present = keys[kcols].drop_duplicates()
    present_keys = set(tuple(x) for x in present.itertuples(index=False, name=None))

    if len(tbl):
        mask = tbl[kcols].apply(tuple, axis=1).isin(present_keys)
        if mask.any():
            tbl.loc[mask, "last_seen"] = today_iso

    tbl.drop_duplicates(subset=kcols, keep="last", inplace=True)

    save_match_table(tbl)
    return tbl

# -----------------------------------------------------------------------------
# Simplified version-level match cache (for speed & manual auditing)
# -----------------------------------------------------------------------------

def version_match_table_columns():
    return [
        "brand_norm",      # normalized brand
        "model_norm",      # full normalized model (replaces deprecated model_token)
        "version_norm",    # normalized full version text
        "model_year",      # year dimension
        "fipe_brand",      # raw FIPE brand selected
        "fipe_model",      # matched FIPE model string
        "fipe_code",       # CodigoFipe
        "score",           # similarity score
        "match_source",    # 'auto' | 'manual'
        "first_seen",      # YYYY-MM-DD first time encountered
        "last_seen",       # YYYY-MM-DD last time observed
    ]

def load_version_match_table() -> pd.DataFrame:
    if VERSION_MATCH_TABLE.exists():
        try:
            df = pd.read_csv(VERSION_MATCH_TABLE, sep=";")
        except Exception:
            df = pd.read_csv(VERSION_MATCH_TABLE)
        # Backward compatibility: upgrade model_token -> model_norm
        if "model_norm" not in df.columns and "model_token" in df.columns:
            df["model_norm"] = df["model_token"].astype(str)
        for col in version_match_table_columns():
            if col not in df.columns:
                df[col] = pd.NA
        # Drop deprecated column if present
        if "model_token" in df.columns and "model_norm" in df.columns:
            try:
                df = df.drop(columns=["model_token"])
            except Exception:
                pass
        return df[version_match_table_columns()]
    return pd.DataFrame(columns=version_match_table_columns())

def save_version_match_table(df: pd.DataFrame):
    df = df[version_match_table_columns()].copy()
    # Coerce model_year numeric
    df["model_year"] = pd.to_numeric(df["model_year"], errors="coerce").astype("Int64")
    # keep the most recently updated row per key (assumes caller updated last_seen)
    df.sort_values(["brand_norm","model_norm","version_norm","model_year","last_seen"], inplace=True)
    df = df.drop_duplicates(subset=["brand_norm","model_norm","version_norm","model_year"], keep="last")
    VERSION_MATCH_TABLE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(VERSION_MATCH_TABLE, index=False, sep=";")
    logging.getLogger("match.cache").info("version match table saved: %s rows -> %s", len(df), VERSION_MATCH_TABLE)

def score_best_fipe_any_year(
    brand_norm: str, model_norm: str, version_norm: str, target_year: Optional[int],
    fipe_df: pd.DataFrame, threshold: float
) -> tuple[Optional[str], Optional[str], Optional[int], float, str]:
    if not version_norm or not model_norm:
        return (None, None, None, 0.0, "unmatched")
    m_tokens = [re.escape(t) for t in model_norm.split() if t]
    if not m_tokens:
        return (None, None, None, 0.0, "unmatched")
    token_pattern = "(?=.*" + ")(?=.*".join(m_tokens) + ")"
    pool = fipe_df[(fipe_df["_brand_norm"] == brand_norm) &
                   fipe_df["_model_norm"].str.contains(token_pattern, regex=True, na=False)]
    if pool.empty:
        return (None, None, None, 0.0, "unmatched")

    v_toks = tokset(version_norm)
    v_engine = extract_engine(version_norm)

    best = (-1.0, None, None, None)
    for _, row in pool.iterrows():
        fipe_model_norm = row["_model_norm"]
        c_toks = row["_toks"]

        q_toks = v_toks | c_toks
        inter = len(v_toks & c_toks)
        coverage  = inter / len(v_toks) if v_toks else 0.0
        precision = inter / len(c_toks) if c_toks else 0.0
        if coverage < 0.05:
            pass
        f1   = (2*precision*coverage/(precision+coverage)) if (precision+coverage) else 0.0
        jacc = (len(v_toks & c_toks)/len(v_toks | c_toks)) if (v_toks or c_toks) else 0.0
        base = SequenceMatcher(None, version_norm, fipe_model_norm).ratio()
        score = 0.55*f1 + 0.20*jacc + 0.25*base

        c_engine = row["_engine"]
        if v_engine:
            score += 0.05 if c_engine == v_engine else (-0.10 if c_engine is not None else 0.0)

        fy = int(row["AnoModelo"]) if pd.notna(row["AnoModelo"]) else None
        if target_year is not None and fy is not None:
            score -= 0.01 * min(12.0, abs(target_year - fy))
        score -= min(0.12, 0.02 * len(c_toks - v_toks))

        if score > best[0]:
            best = (score, row["Modelo"], row["CodigoFipe"], fy)

    s, m, c, y = best
    if s >= max(0.0, threshold - 0.08) and m and c:
        return (m, c, y if y is not None else target_year, float(round(s, 4)), "matched_fallback")
    return (None, None, None, float(max(s, 0.0)), "unmatched")

def score_best_fipe_brand_only(
    brand_norm: str, version_norm: str, fipe_df: pd.DataFrame
) -> tuple[Optional[str], Optional[str], Optional[int], float, str]:
    pool = fipe_df[(fipe_df["_brand_norm"] == brand_norm)]
    if pool.empty or not version_norm:
        return (None, None, None, 0.0, "unmatched")

    v_toks = tokset(version_norm)
    best = (-1.0, None, None, None)
    for _, row in pool.iterrows():
        c_toks = row["_toks"]
        base = SequenceMatcher(None, version_norm, row["_model_norm"]).ratio()
        jacc = (len(v_toks & c_toks)/len(v_toks | c_toks)) if (v_toks or c_toks) else 0.0
        score = 0.7*base + 0.3*jacc
        if score > best[0]:
            best = (score, row["Modelo"], row["CodigoFipe"],
                    int(row["AnoModelo"]) if pd.notna(row["AnoModelo"]) else None)
    s, m, c, y = best
    if m and c:
        return (m, c, y, float(round(s, 4)), "matched_brand_only")
    return (None, None, None, float(max(s, 0.0)), "unmatched")

# -----------------------------------------------------------------------------
# Matching Localiza → FIPE (or Movida fallback)
# -----------------------------------------------------------------------------

log_match = logging.getLogger("match")

def match_localiza(localiza_csv: Path,
                   fipe_csv: Optional[Path],
                   out_csv: Path,
                   threshold: float = 0.0,
                   movida_csv_fallback: Optional[Path] = None,
                   fipe_models_csv: Optional[Path] = None):
    """Match Localiza rows to FIPE codes using a cache to skip known versions.

    Versions already stored in ``localiza_version_match.csv`` are not re-scored.
    Newly observed versions are matched and appended to the cache with
    ``first_seen``/``last_seen`` columns maintained. ``match_score`` compared
    against ``threshold`` yields ``match_accepted``.
    """
    log = logging.getLogger("match.simple")
    # ---- Load Localiza dataset (already parsed CSV) ----
    loc = pd.read_csv(localiza_csv, sep=";")
    if "model_year" in loc.columns:
        loc["model_year"] = pd.to_numeric(loc["model_year"], errors="coerce").astype("Int64")
    else:
        loc["model_year"] = pd.NA
    for col in ("brand", "model", "version"):
        if col not in loc.columns:
            loc[col] = ""

    # ---- Load FIPE models (retain year) ----
    fm_path = fipe_models_csv or find_fipe_models_csv()
    if not (fm_path and Path(fm_path).exists()):
        raise SystemExit("match-localiza requires fipe_models.csv (data/fipe/fipe_models.csv).")
    fipe = load_fipe_models_df(Path(fm_path))[ ["Marca","Modelo","CodigoFipe","AnoModelo"] ].copy()
    fipe["AnoModelo"] = pd.to_numeric(fipe["AnoModelo"], errors="coerce").astype("Int64")
    fipe = (fipe.sort_values(["CodigoFipe","AnoModelo"])
                 .drop_duplicates(subset=["CodigoFipe","AnoModelo"])
                 .reset_index(drop=True))

    # ---- Normalize ----
    def _safe_norm_version(s: str) -> str:
        return norm_text(s)
    def _safe_norm_model(s: str) -> str:
        return generic_norm_text(s)

    loc["_brand_norm"] = loc["brand"].map(norm_brand)
    loc["_model_norm"] = loc["model"].map(_safe_norm_model)
    loc["_version_norm"] = loc["version"].map(_safe_norm_version)

    fipe_norm = fipe.copy()
    fipe_norm["_brand_norm"] = fipe_norm["Marca"].map(norm_brand)
    fipe_norm["_model_norm"] = fipe_norm["Modelo"].map(norm_text)

    # ---- Merge existing cache ----
    cache_df = load_version_match_table()
    key_cols = ["_brand_norm","_model_norm","_version_norm","model_year"]
    loc = loc.merge(
        cache_df.rename(columns={"brand_norm":"_brand_norm","model_norm":"_model_norm","version_norm":"_version_norm"}),
        on=["_brand_norm","_model_norm","_version_norm","model_year"],
        how="left",
    )

    today_iso = date.today().isoformat()

    # ---- Match new keys ----
    new_keys = (loc[loc["fipe_code"].isna()][key_cols]
                .drop_duplicates()
                .rename(columns={"_brand_norm":"brand_norm",
                                 "_model_norm":"model_norm",
                                 "_version_norm":"version_norm"}))

    vm_rows: List[Dict[str, Any]] = []
    for _, nk in new_keys.iterrows():
        brand_n = nk["brand_norm"]
        model_n = nk["model_norm"]
        version_n = nk["version_norm"]
        year = nk["model_year"]
        if pd.isna(year) or not model_n:
            vm_rows.append({**nk, "fipe_brand": None, "fipe_model": None, "fipe_code": None,
                            "score": 0.0, "match_source": "contains",
                            "first_seen": today_iso, "last_seen": today_iso})
            continue
        cand = fipe_norm[(fipe_norm["_brand_norm"] == brand_n) & (fipe_norm["AnoModelo"] == int(year))]
        if cand.empty:
            vm_rows.append({**nk, "fipe_brand": None, "fipe_model": None, "fipe_code": None,
                            "score": 0.0, "match_source": "contains",
                            "first_seen": today_iso, "last_seen": today_iso})
            continue
        model_tok_set = set(model_n.split())
        cand = cand[cand["_model_norm"].apply(lambda m: model_n in m or model_tok_set.issubset(set(m.split())))]
        if cand.empty:
            vm_rows.append({**nk, "fipe_brand": None, "fipe_model": None, "fipe_code": None,
                            "score": 0.0, "match_source": "contains",
                            "first_seen": today_iso, "last_seen": today_iso})
            continue
        v_toks = set(version_n.split()) if version_n else set()
        best_score = -1.0; best_code = None; best_model = None; best_brand = None
        for _, fr in cand.iterrows():
            f_model_norm = fr["_model_norm"]
            f_toks = set(f_model_norm.split())
            inter = len(v_toks & f_toks)
            coverage = inter / len(v_toks) if v_toks else 0.0
            precision = inter / len(f_toks) if f_toks else 0.0
            f1 = (2 * precision * coverage / (precision + coverage)) if (precision + coverage) else 0.0
            jacc = (len(v_toks & f_toks) / len(v_toks | f_toks)) if (v_toks or f_toks) else 0.0
            seq = SequenceMatcher(None, version_n, f_model_norm).ratio() if version_n else 0.0
            score = 0.5 * seq + 0.3 * f1 + 0.2 * jacc
            if score > best_score:
                best_score = score; best_code = fr["CodigoFipe"]; best_model = fr["Modelo"]; best_brand = fr["Marca"]
        vm_rows.append({**nk, "fipe_brand": best_brand, "fipe_model": best_model, "fipe_code": best_code,
                        "score": round(float(best_score), 4) if best_code else 0.0,
                        "match_source": "contains", "first_seen": today_iso, "last_seen": today_iso})

    if vm_rows:
        add_df = pd.DataFrame(vm_rows).reindex(columns=version_match_table_columns())
        if cache_df.empty:
            cache_df = add_df.copy()
        else:
            cache_df = pd.concat([cache_df, add_df], ignore_index=True, sort=False)

    if not cache_df.empty:
        present_keys = set(tuple(x) for x in loc[key_cols].drop_duplicates().rename(
            columns={"_brand_norm":"brand_norm","_model_norm":"model_norm","_version_norm":"version_norm"}
        ).itertuples(index=False, name=None))
        mask = cache_df[["brand_norm","model_norm","version_norm","model_year"]].apply(tuple, axis=1).isin(present_keys)
        cache_df.loc[mask, "last_seen"] = today_iso
    save_version_match_table(cache_df)

    loc = loc.drop(columns=[c for c in ["fipe_brand","fipe_model","fipe_code","score","match_source"] if c in loc.columns])
    loc = loc.merge(
        cache_df.rename(columns={"brand_norm":"_brand_norm","model_norm":"_model_norm","version_norm":"_version_norm"}),
        on=["_brand_norm","_model_norm","_version_norm","model_year"],
        how="left",
    )
    loc.rename(columns={"score":"match_score"}, inplace=True)
    loc["match_score"] = pd.to_numeric(loc["match_score"], errors="coerce").fillna(0).astype(float)
    loc["match_accepted"] = (loc["match_score"] >= threshold).astype(int)

    out_csv = Path(out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    loc.to_csv(out_csv, index=False, sep=";")
    log.info("wrote %s rows -> %s (contains strategy)", len(loc), out_csv)

# -----------------------------------------------------------------------------
# Collect tuples (fipe_code, model_year)
# -----------------------------------------------------------------------------

def collect_fipe_tuples(localiza_match_csv: Optional[Path], movida_csv: Optional[Path]) -> Set[Tuple[str, int]]:
    tuples: Set[Tuple[str,int]] = set()
    if localiza_match_csv and Path(localiza_match_csv).exists():
        df = pd.read_csv(localiza_match_csv, sep=";")
        year_col = "fipe_year" if "fipe_year" in df.columns else "model_year"
        df = df[["fipe_code", year_col]].dropna()
        for _, r in df.iterrows():
            code = str(r["fipe_code"]).strip()
            try: yr = int(r[year_col])
            except: continue
            if code: tuples.add((code, yr))
    if movida_csv and Path(movida_csv).exists():
        mv = pd.read_csv(movida_csv, sep=";")
        if {"fipe_code","model_year"}.issubset(mv.columns):
            mv = mv[["fipe_code","model_year"]].dropna()
            for _, r in mv.iterrows():
                code = str(r["fipe_code"]).strip()
                try: yr = int(r["model_year"])
                except: continue
                if code: tuples.add((code, yr))
    return tuples

# -----------------------------------------------------------------------------
# Independent tuples CSV loader (enables standalone fipe-dump)
# -----------------------------------------------------------------------------

def load_tuples_csv(path: Optional[Path]) -> Set[Tuple[str, int]]:
    """Load a CSV (semicolon or comma separated) with columns fipe_code, model_year.

    Returns a set of (fipe_code, model_year). Silently returns empty set on errors.
    """
    out: Set[Tuple[str,int]] = set()
    if not path:
        return out
    p = Path(path)
    if not p.exists():
        return out
    # Try semicolon first (project default), then comma.
    df = None
    for sep in (';', ','):
        try:
            df = pd.read_csv(p, sep=sep)
            # If only one column produced and sep was ';', try comma next
            if df.shape[1] == 1 and sep == ';':
                continue
            break
        except Exception:
            df = None
            continue
    if df is None:
        return out
    lower_map = {c.lower(): c for c in df.columns}
    if 'fipe_code' not in lower_map or 'model_year' not in lower_map:
        return out
    # Optional fuel columns
    fuel_code_col = None
    for cand in ("codigotipocombustivel", "codigo_tipocombustivel", "fuel_code"):
        if cand in lower_map:
            fuel_code_col = lower_map[cand]; break
    sigla_col = None
    if not fuel_code_col:
        for cand in ("siglacombustivel", "sigla_combustivel"):
            if cand in lower_map:
                sigla_col = lower_map[cand]; break
    cols = [lower_map['fipe_code'], lower_map['model_year']]
    if fuel_code_col: cols.append(fuel_code_col)
    if sigla_col and sigla_col not in cols: cols.append(sigla_col)
    df = df[cols].dropna(subset=[lower_map['fipe_code'], lower_map['model_year']])
    for _, r in df.iterrows():
        code = str(r[lower_map['fipe_code']]).strip()
        # Robust year parsing: accept strings like '32000' or '32000.0'
        raw_year = r[lower_map['model_year']]
        try:
            if pd.isna(raw_year):
                continue
        except Exception:
            pass
        try:
            yr = int(float(str(raw_year).replace(',', '.')))
        except Exception:
            continue
        if not code or yr <= 1900:
            continue
        fuel_code_val: Optional[str] = None
        if fuel_code_col:
            raw_fc = str(r[fuel_code_col]).strip()
            if raw_fc:
                fuel_code_val = raw_fc
        elif sigla_col:
            sig = str(r[sigla_col]).strip().upper()
            fuel_code_val = SIGLA_TO_FUEL_CODE.get(sig)
        if fuel_code_val:
            out.add((code, yr, fuel_code_val))
        else:
            out.add((code, yr))
    logging.getLogger("tuples.load").info("Loaded %s tuples from %s", len(out), p)
    return out

def load_all_tuples_history(dir_path: Optional[Path] = None) -> Set[Tuple[str, int]]:
    """Union of tuples observed historically in parsed vendor CSVs.

    Scans, similar to collect_fipe_tuples, across history:
      - Localiza match CSVs under MATCH_DIR: localiza_with_fipe_match_*.csv (and case variant)
      - Movida parsed CSVs under RAW_MOVIDA: movida_seminovos_*.csv

    Returns distinct (fipe_code, model_year) pairs. The dir_path parameter is ignored.
    """
    log = logging.getLogger("tuples.load")
    tuples: Set[Tuple[str, int]] = set()

    # Localiza history (both common case variants)
    try:
        loc_files = (
            sorted(MATCH_DIR.glob("localiza_with_fipe_match_*.csv"))
            + sorted(MATCH_DIR.glob("Localiza_with_fipe_match_*.csv"))
        )
    except Exception:
        loc_files = []

    loc_added = 0
    for f in loc_files:
        try:
            df = pd.read_csv(f, sep=";")
            if "fipe_code" not in df.columns:
                continue
            # Use fipe_year if present for the row; otherwise fall back to model_year (row-wise)
            has_fipe = "fipe_year" in df.columns
            has_model = "model_year" in df.columns
            if not has_fipe and not has_model:
                continue
            cols = ["fipe_code"] + (["fipe_year"] if has_fipe else []) + (["model_year"] if has_model else [])
            sub = df[cols].dropna(subset=["fipe_code"])  # keep row even if one of the year cols is NaN
            for _, r in sub.iterrows():
                code = str(r["fipe_code"]).strip()
                if not code:
                    continue
                raw_year = None
                if has_fipe:
                    raw_year = r.get("fipe_year")
                if (raw_year is None or (isinstance(raw_year, float) and pd.isna(raw_year)) or str(raw_year).strip()=="") and has_model:
                    raw_year = r.get("model_year")
                if raw_year is None or (isinstance(raw_year, float) and pd.isna(raw_year)):
                    continue
                try:
                    yr = int(float(str(raw_year).replace(",", ".")))
                except Exception:
                    continue
                if yr <= 1900:
                    continue
                before = len(tuples)
                tuples.add((code, yr))
                if len(tuples) > before:
                    loc_added += 1
        except Exception:
            continue

    # Movida history
    try:
        mov_files = sorted(RAW_MOVIDA.glob("movida_seminovos_*.csv"))
    except Exception:
        mov_files = []

    mov_added = 0
    for f in mov_files:
        try:
            df = pd.read_csv(f, sep=";")
            if not {"fipe_code", "model_year"}.issubset(df.columns):
                continue
            sub = df[["fipe_code", "model_year"]].dropna()
            for _, r in sub.iterrows():
                code = str(r["fipe_code"]).strip()
                if not code:
                    continue
                try:
                    yr = int(r["model_year"])
                except Exception:
                    continue
                before = len(tuples)
                tuples.add((code, yr))
                if len(tuples) > before:
                    mov_added += 1
        except Exception:
            continue

    log.info("Loaded %s tuples from history (Localiza add=%s, Movida add=%s)", len(tuples), loc_added, mov_added)
    return tuples

def tuples_audit(localiza_match_csv: Optional[Path],
                 movida_csv: Optional[Path],
                 out_path: Optional[Path] = None) -> Path:
    log = logging.getLogger("tuples.audit")

    def _load_localiza_pairs(p: Optional[Path]) -> pd.DataFrame:
        if not p or not Path(p).exists():
            return pd.DataFrame(columns=["fipe_code", "model_year"])
        df = pd.read_csv(p, sep=";")
        year_col = "fipe_year" if "fipe_year" in df.columns else "model_year"
        df = df[["fipe_code", year_col]].dropna()
        df["fipe_code"] = df["fipe_code"].astype(str).str.strip()
        df[year_col] = pd.to_numeric(df[year_col], errors="coerce").astype("Int64")
        df = df[(df["fipe_code"].ne("")) & (df[year_col].notna())].copy()
        df = df.rename(columns={year_col: "model_year"})
        return df

    def _load_movida_pairs(p: Optional[Path]) -> pd.DataFrame:
        if not p or not Path(p).exists():
            return pd.DataFrame(columns=["fipe_code", "model_year"])
        df = pd.read_csv(p, sep=";")
        if not {"fipe_code","model_year"}.issubset(df.columns):
            return pd.DataFrame(columns=["fipe_code", "model_year"])
        df = df[["fipe_code", "model_year"]].dropna()
        df["fipe_code"] = df["fipe_code"].astype(str).str.strip()
        df["model_year"] = pd.to_numeric(df["model_year"], errors="coerce").astype("Int64")
        df = df[(df["fipe_code"].ne("")) & (df["model_year"].notna())].copy()
        return df

    ldf = _load_localiza_pairs(localiza_match_csv)
    mdf = _load_movida_pairs(movida_csv)

    lcnt = (ldf.groupby(["fipe_code","model_year"]).size()
               .rename("localiza_count").reset_index())
    mcnt = (mdf.groupby(["fipe_code","model_year"]).size()
               .rename("movida_count").reset_index())

    audit = pd.merge(lcnt, mcnt, on=["fipe_code","model_year"], how="outer")
    for c in ("localiza_count","movida_count"):
        if c not in audit.columns:
            audit[c] = 0
    audit[["localiza_count","movida_count"]] = audit[["localiza_count","movida_count"]].fillna(0).astype(int)
    audit["total_count"]      = audit["localiza_count"] + audit["movida_count"]
    audit["localiza_present"] = audit["localiza_count"] > 0
    audit["movida_present"]   = audit["movida_count"] > 0

    # Enrich with fuel codes if fipe_models.csv available
    try:
        fm_path = find_fipe_models_csv()
        if fm_path and Path(fm_path).exists():
            fm = pd.read_csv(fm_path, sep=";")
            cols_norm = {c.lower(): c for c in fm.columns}
            if {"codigofipe","anomodelo"}.issubset(cols_norm):
                base_cols = [cols_norm['codigofipe'], cols_norm['anomodelo']]
                sig_col = cols_norm.get('siglacombustivel')
                use_cols = base_cols + ([sig_col] if sig_col else [])
                fsub = fm[use_cols].dropna(subset=base_cols)
                fsub = fsub.rename(columns={
                    cols_norm['codigofipe']: 'fipe_code',
                    cols_norm['anomodelo']: 'model_year'
                })
                fsub['model_year'] = pd.to_numeric(fsub['model_year'], errors='coerce').astype('Int64')
                fsub = fsub.dropna(subset=['model_year'])
                fsub['model_year'] = fsub['model_year'].astype(int)
                fsub = fsub.drop_duplicates(subset=['fipe_code','model_year'])
                if sig_col:
                    fsub = fsub.rename(columns={sig_col: 'SiglaCombustivel'})
                    fsub['codigoTipoCombustivel'] = fsub['SiglaCombustivel'].map(SIGLA_TO_FUEL_CODE)
                audit = audit.merge(fsub, on=['fipe_code','model_year'], how='left')
    except Exception as e:
        logging.getLogger('tuples.audit').warning('fuel enrichment skipped: %s', e)

    audit = audit.sort_values(["total_count","fipe_code","model_year"], ascending=[False, True, True])

    if out_path is None:
        out_path = TUPLES_DIR / f"fipe_tuples_{ymd_compact()}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    audit.to_csv(out_path, index=False, sep=";")
    log.info("wrote %s rows -> %s", len(audit), out_path)
    return out_path

# -----------------------------------------------------------------------------
# Final output table builders
# -----------------------------------------------------------------------------

def normalize_type(raw_type: Any, fuel_sigla: Optional[str]) -> str:
    """Normalize heterogeneous vendor body/segment types and separate electric / hybrid.

    Precedence:
      1. If fuel_sigla indicates Electric (E) or Hybrid (H), return EV.
      2. Otherwise map raw_type (after accent stripping & upper) to a canonical set.

    Canonical types (non-fuel-specific):
      SEDAN, HATCH, SUV, PICKUP/VAN, PREMIUM, OTHER
    EV override: ELETRICO, HIBRIDO
    """
    if fuel_sigla:
        fs = str(fuel_sigla).upper().strip()
        if fs in {'E','H'}:
            return 'ev'
    if raw_type is None or (isinstance(raw_type, float) and pd.isna(raw_type)):
        return ""
    s0 = strip_accents(str(raw_type)).upper().strip()
    mapping = {
        # Sedan
        'SEDAN': 'SEDAN', 'SEDÃ': 'SEDAN', 'SEDA': 'SEDAN',
        # Hatch
        'HATCH': 'HATCH', 'HATCHBACK': 'HATCH',
        # SUV
        'SUV': 'SUV',
        # Pickup / Vans / minivans 
        'PICAPE': 'PICKUP/VANS', 'PICAPE CABINE DUPLA': 'PICKUP/VANS', 'PICK-UP': 'PICKUP/VANS', 'PICKUP': 'PICKUP/VANS', 'CAMINHONETE': 'PICKUP/VANS', 'CABINE SIMPLES': 'PICKUP/VANS',
        # Utilities / cargo boxes
        'UTILITÁRIO': 'PICKUP/VANS', 'FURGAO': 'PICKUP/VANS', 'MINIVAN': 'PICKUP/VANS', 'VAN': 'PICKUP/VANS', 'CARGA': 'PICKUP/VANS',
        # EV
        'ELETRICO': 'EV', 'HIBRIDO': 'EV',
        # Body style specials
        'COUPE': 'PREMIUM', 'SPORTBACK': 'PREMIUM', 'GRAN COUPE': 'PREMIUM', 'FASTBACK': 'PREMIUM',
        # Misc
        'PARTICULAR': 'OTHER', 'OUTROS': 'OTHER', 'OUTRO': 'OTHER'
    }
    return mapping.get(s0, s0).lower()

def build_vendor_table(vendor: str, src_csv: Path, fipe_csv: Path, out_csv: Path) -> Path:
    log = logging.getLogger(f"table.{vendor}")
    df = pd.read_csv(src_csv, sep=";")
    orig_len = len(df)
    df['_orig_row_id'] = range(orig_len)
    fipe = pd.read_csv(fipe_csv)
    fipe = fipe.rename(columns={"CodigoFipe":"fipe_code","AnoModelo":"model_year",
                                 "Modelo":"fipe_version","ValorNum":"fipe_price",
                                 "SiglaCombustivel":"fuel_sigla","Combustivel":"fuel_name"})
    keep_cols = [c for c in ["fipe_code","model_year","fipe_version","fipe_price","fuel_sigla","fuel_name"] if c in fipe.columns]
    fipe = fipe[keep_cols]
    # Retain only essential FIPE columns for vendor table
    fipe = fipe[[c for c in ["fipe_code","model_year","fipe_version","fipe_price"] if c in fipe.columns]]
    df = df.merge(fipe, on=["fipe_code","model_year"], how="left")
    # Collapse duplicate expansions caused by multiple FIPE matches per code/year
    if len(df) != orig_len:
        expanded = len(df) - orig_len
        if expanded > 0:
            log.warning("%s: collapsing %s expanded rows after FIPE merge", vendor, expanded)
        df = (df.sort_values(['_orig_row_id'])
                .groupby('_orig_row_id', as_index=False)
                .first())
    if len(df) != orig_len:
        log.warning("%s: row count mismatch (orig=%s new=%s) after collapse", vendor, orig_len, len(df))
    df["offer_price"] = df.get("price")
    df["premium_vs_fipe_price"] = np.where(df["fipe_price"].gt(0),
                                            (df["offer_price"] - df["fipe_price"]) / df["fipe_price"], pd.NA)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
    df["snapshot_year"] = df["snapshot_date"].dt.year
    df["snapshot_month"] = df["snapshot_date"].dt.month
    df["snapshot_date"] = df["snapshot_date"].dt.date.astype(str)
    # Unified type taxonomy with electric/hybrid separation
    fuel_sigla_col = df.get("fuel_sigla") if "fuel_sigla" in df.columns else None
    df["type"] = df.apply(lambda r: normalize_type(r.get("type"), r.get("fuel_sigla")), axis=1)
    df["fipe_code_model_year"] = df["fipe_code"].astype(str) + "_" + df["model_year"].astype(str)
    df["model_model_year"] = df["model"].astype(str) + "_" + df["model_year"].astype(str)
    out_cols = ["snapshot_date","snapshot_year","snapshot_month","type","brand","model",
                "fipe_version","fipe_code","manufacture_year","model_year",
                "offer_price","fipe_price","premium_vs_fipe_price",
                "fipe_code_model_year","model_model_year"]
    df_out = df[out_cols]
    if len(df_out) != orig_len:
        log.warning("%s: output rows %s != source %s", vendor, len(df_out), orig_len)
    else:
        log.info("%s: preserved row count (%s rows)", vendor, orig_len)
    ensure_dir(out_csv.parent)
    df_out.to_csv(out_csv, index=False, sep=";")
    log.info("wrote %s rows -> %s", len(df_out), out_csv)
    return out_csv

def build_fipe_table(fipe_csv: Path, loc_table: Path, mov_table: Path, out_csv: Path) -> Path:
    log = logging.getLogger("table.fipe")
    fipe = pd.read_csv(fipe_csv)
    fipe["reference_year"], fipe["reference_month"] = zip(*fipe["MesReferencia"].map(parse_mes_label))
    # Shift reference month back by one: prices published at start of month M actually refer to month M-1
    def _shift_back(y: int, m: int) -> Tuple[int,int]:
        if m == 1:
            return (y - 1, 12)
        return (y, m - 1)
    shifted = fipe.apply(lambda r: _shift_back(int(r["reference_year"]), int(r["reference_month"])), axis=1)
    fipe["reference_year"], fipe["reference_month"] = zip(*shifted)
    fipe = fipe.rename(columns={"Marca":"brand","Modelo":"fipe_version",
                                "CodigoFipe":"fipe_code","AnoModelo":"model_year",
                                "ValorNum":"fipe_price"})
    model_map = pd.concat([
        pd.read_csv(loc_table, sep=";")[ ["fipe_code","model_year","model"] ],
        pd.read_csv(mov_table, sep=";")[ ["fipe_code","model_year","model"] ]
    ], ignore_index=True)
    model_map = model_map.dropna().drop_duplicates(subset=["fipe_code","model_year"])
    fipe = fipe.merge(model_map, on=["fipe_code","model_year"], how="left")
    # Keep only models present in at least one vendor fleet
    before_presence = len(fipe)
    fipe = fipe[fipe["model"].notna()].copy()
    removed_presence = before_presence - len(fipe)
    if removed_presence > 0:
        log.info("dropped %s FIPE rows without vendor presence", removed_presence)
    # Merge normalized type from vendor tables (prefer Localiza, then Movida; fallback first available)
    try:
        loc_types = pd.read_csv(loc_table, sep=";")[ ["fipe_code","model_year","type"] ]
        mov_types = pd.read_csv(mov_table, sep=";")[ ["fipe_code","model_year","type"] ]
        all_types = pd.concat([loc_types.assign(_src='loc'), mov_types.assign(_src='mov')], ignore_index=True)
        all_types = all_types.dropna(subset=["type"])
        # Preference order loc > mov: sort accordingly before dropping duplicates
        all_types['_pref'] = all_types['_src'].map({'loc':0,'mov':1}).fillna(2)
        all_types = all_types.sort_values(['fipe_code','model_year','_pref'])
        type_map = all_types.drop_duplicates(subset=["fipe_code","model_year"])[["fipe_code","model_year","type"]]
        fipe = fipe.merge(type_map, on=["fipe_code","model_year"], how="left")
    except Exception as e:
        log.warning("type merge skipped: %s", e)
    fipe = fipe.sort_values(["fipe_code","model_year","reference_year","reference_month"]).reset_index(drop=True)
    before = len(fipe)
    fipe = fipe.drop_duplicates(subset=["fipe_code","model_year","reference_year","reference_month"], keep="last")
    removed = before - len(fipe)
    if removed > 0:
        log.info("removed %s duplicate FIPE rows (same code/year/month)", removed)
    fipe["m_m_price_change"] = fipe.groupby(["fipe_code","model_year"])["fipe_price"].pct_change()
    out_cols = ["reference_year","reference_month","brand","model","type","fipe_version",
                "fipe_code","model_year","fipe_price","m_m_price_change"]
    df_out = fipe[out_cols]
    ensure_dir(out_csv.parent)
    df_out.to_csv(out_csv, index=False, sep=";")
    log.info("wrote %s rows -> %s", len(df_out), out_csv)
    return out_csv

def export_tables(args):
    loc_csv = Path(args.localiza_csv) if args.localiza_csv else latest("localiza_with_fipe_match_*.csv", MATCH_DIR)
    mov_csv = Path(args.movida_csv) if args.movida_csv else latest("movida_seminovos_*.csv", RAW_MOVIDA)
    fipe_csv = Path(args.fipe_csv) if args.fipe_csv else latest("fipe_dump_*.csv", FIPE_DIR)
    if not (loc_csv and mov_csv and fipe_csv and loc_csv.exists() and mov_csv.exists() and fipe_csv.exists()):
        raise SystemExit("build-tables: required CSVs not found")
    out_dir = Path(args.out_dir)
    ensure_dir(out_dir)
    stamp = ymd_compact()
    loc_out = out_dir / f"localiza_table_{stamp}.csv"
    mov_out = out_dir / f"movida_table_{stamp}.csv"
    fipe_out = out_dir / f"fipe_table_{stamp}.csv"
    build_vendor_table("localiza", loc_csv, fipe_csv, loc_out)
    build_vendor_table("movida", mov_csv, fipe_csv, mov_out)
    build_fipe_table(fipe_csv, loc_out, mov_out, fipe_out)
    log = logging.getLogger("table")
    log.info("generated tables -> %s", out_dir)
    return out_dir

# -----------------------------------------------------------------------------
# FIPE client & dump (supports tuples-only mode)
# -----------------------------------------------------------------------------

BASE_URL = "https://veiculos.fipe.org.br"
ENDPOINTS = {
    "tabelas": "/api/veiculos/ConsultarTabelaDeReferencia",
    "marcas": "/api/veiculos/ConsultarMarcas",
    "modelos": "/api/veiculos/ConsultarModelos",
    "anos_modelo": "/api/veiculos/ConsultarAnoModelo",
    "valor_todos_params": "/api/veiculos/ConsultarValorComTodosParametros",
}
DEFAULT_HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": BASE_URL,
    "referer": BASE_URL + "/",
    "x-requested-with": "XMLHttpRequest",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}
TIPO_STR = {"1":"carro","2":"moto","3":"caminhao"}

# Some FIPE codes require a specific combustivel (fuel) code to return a price.
# For those we force trying that code first; otherwise we try a list of common fuel codes.
# Code 6 (Flex) is often required for recent Brazilian passenger cars, so it is first.
FUEL_CODE_CANDIDATES = ["6","1","2","3","4","5"]  # ordered attempt list
FORCE_FUEL_CODE: Dict[str,str] = {
    # Known codes reported failing unless fuel code 6 is used
    "093010-5": "6",
    "035120-2": "6",
    "008302-0": "6",
}

# Static fuel mapping table (combustivel, codigoTipoCombustivel, SiglaCombustivel)
FUEL_STATIC_TABLE = [
    {"combustivel": "Flex",     "codigoTipoCombustivel": "5", "SiglaCombustivel": "F"},
    {"combustivel": "Híbrido",  "codigoTipoCombustivel": "6", "SiglaCombustivel": "H"},
    {"combustivel": "Gasolina", "codigoTipoCombustivel": "1", "SiglaCombustivel": "G"},
    {"combustivel": "Diesel",   "codigoTipoCombustivel": "3", "SiglaCombustivel": "D"},
    {"combustivel": "Elétrico", "codigoTipoCombustivel": "4", "SiglaCombustivel": "E"},
]
SIGLA_TO_FUEL_CODE = {r["SiglaCombustivel"]: r["codigoTipoCombustivel"] for r in FUEL_STATIC_TABLE}
FUEL_CODE_TO_SIGLA = {r["codigoTipoCombustivel"]: r["SiglaCombustivel"] for r in FUEL_STATIC_TABLE}

def since_filter(tabelas: List[Dict[str, Any]], since_yyyymm: Optional[str]) -> List[Dict[str, Any]]:
    if not since_yyyymm: return tabelas
    try:
        y, m = map(int, since_yyyymm.split("-"))
    except Exception:
        return tabelas
    out=[]
    for t in tabelas:
        y2,m2 = parse_mes_label(t.get("Mes", t.get("Label","")))
        if (y2,m2) >= (y,m): out.append(t)
    return out

def parse_valor_brl(valor: Any) -> Optional[float]:
    if valor is None: return None
    if isinstance(valor,(int,float)): return float(valor)
    if not isinstance(valor,str): return None
    v = (valor.replace("R$","").replace(".","").replace(" ","").replace("\u00A0","").strip().replace(",","."))
    try: return float(v)
    except Exception: return None

def unique_row_key(row: Dict[str, Any]) -> Tuple:
    return (row.get("MesReferencia"), row.get("CodigoFipe"), row.get("AnoModelo"), row.get("SiglaCombustivel"))

def param_key_for_resume(tref, tipo, cod_marca, cod_modelo, ano_param) -> Tuple:
    return (str(tref), str(tipo), str(cod_marca), str(cod_modelo), str(ano_param), "")

class RateLimiter:
    def __init__(self, rate_per_sec: float):
        self.rate = max(0.1, rate_per_sec)
        self._lock = asyncio.Lock()
        self._next_free = 0.0
    async def wait(self):
        async with self._lock:
            now = time.perf_counter()
            if now < self._next_free: await asyncio.sleep(self._next_free - now)
            self._next_free = max(self._next_free, now) + 1.0 / self.rate

class DiskCache:
    def __init__(self, root: Path):
        self.root = root; root.mkdir(parents=True, exist_ok=True)
    def _key_to_path(self, endpoint: str, data: Dict[str, Any]) -> Path:
        payload = json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        h = hashlib.sha1((endpoint + "|" + payload).encode("utf-8")).hexdigest()
        sub = self.root / endpoint.strip("/").replace("/", "_")
        sub.mkdir(parents=True, exist_ok=True)
        return sub / f"{h}.json"
    def get(self, endpoint: str, data: Dict[str, Any]) -> Optional[Any]:
        p = self._key_to_path(endpoint, data)
        if p.exists():
            try: return json.loads(p.read_text(encoding="utf-8"))
            except Exception: return None
        return None
    def set(self, endpoint: str, data: Dict[str, Any], value: Any):
        p = self._key_to_path(endpoint, data)
        try: p.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
        except Exception: pass

class FipeClient:
    def __init__(self, session: aiohttp.ClientSession, cache: DiskCache,
                 rate_limiter: RateLimiter, sem: asyncio.Semaphore, progress: bool):
        self.session = session
        self.cache = cache
        self.rate = rate_limiter
        self.sem = sem
        self.progress = progress
        self.base_delay = {
            "ConsultarAnoModelo": 0.2,
            "ConsultarModelos": 0.1,
            "ConsultarMarcas": 0.1,
            "ConsultarValorComTodosParametros": 0.1,
            "ConsultarTabelaDeReferencia": 0.0,
        }
        self.pow2_level = {k: 0 for k in self.base_delay}
        self.pow2_seed = 1.0
        self.pow2_cap  = 8.0

    async def _bootstrap_cookies(self):
        try:
            async with self.session.get(BASE_URL, timeout=20) as resp:
                await resp.text()
        except Exception:
            pass

    def _match_key(self, endpoint: str) -> Optional[str]:
        for k in self.base_delay.keys():
            if k in endpoint: return k
        return None

    async def _post_json(self, endpoint: str, data: Dict[str, Any]) -> Any:
        # Bypass cache for the reference table endpoint to always fetch fresh values
        bypass_cache = "ConsultarTabelaDeReferencia" in str(endpoint)
        if not bypass_cache:
            cached = self.cache.get(endpoint, data)
            if cached is not None:
                return cached

        url = BASE_URL + endpoint
        await self.rate.wait()

        key = self._match_key(endpoint)
        base = self.base_delay.get(key, 0.0) if key else 0.0
        penalty = 0.0
        if key: penalty = min(self.pow2_seed * (2 ** self.pow2_level[key]), self.pow2_cap)
        ed = base + penalty
        if ed > 0: await asyncio.sleep(ed * (0.7 + 0.6 * random.random()))

        async with self.sem:
            delay = 1.0
            for attempt in range(7):
                try:
                    async with self.session.post(url, data=data, headers=DEFAULT_HEADERS, timeout=40) as resp:
                        if resp.status in (200, 201):
                            if key and self.pow2_level[key] > 0: self.pow2_level[key] -= 1
                            try: js = await resp.json(content_type=None)
                            except Exception:
                                txt = await resp.text()
                                js = json.loads(txt)
                            # Do not write to cache for reference table endpoint
                            if not bypass_cache:
                                self.cache.set(endpoint, data, js)
                            return js
                        elif resp.status == 429:
                            retry_after = resp.headers.get("Retry-After")
                            if key:
                                max_level = int(math.log2(max(self.pow2_cap / max(self.pow2_seed, 1e-9), 1)))
                                if self.pow2_level[key] < max_level: self.pow2_level[key] += 1
                            ra = 0.0
                            try: ra = float(retry_after) if retry_after else 0.0
                            except Exception: ra = 0.0
                            await asyncio.sleep(max(delay, ra))
                            delay = min(delay * 2, 30)
                        elif resp.status in (500, 502, 503, 504):
                            retry_after = resp.headers.get("Retry-After")
                            ra = 0.0
                            try: ra = float(retry_after) if retry_after else 0.0
                            except Exception: ra = 0.0
                            await asyncio.sleep(max(delay, ra))
                            delay = min(delay * 2, 30)
                        else:
                            txt = await resp.text()
                            raise RuntimeError(f"HTTP {resp.status} for {endpoint}: {txt[:200]}")
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if attempt >= 6: raise
                    await asyncio.sleep(delay); delay = min(delay * 2, 30)
        raise RuntimeError("Unexpected fallthrough in _post_json")

class CsvWriter:
    def __init__(self, out_path: Optional[Path], resume: bool, progress: bool):
        self.out = Path(out_path) if out_path else None
        self.resume = resume; self.progress = progress
        self.fieldnames = [
            "TabelaReferenciaCodigo","CodigoTipoVeiculo","CodigoMarca","CodigoModelo",
            "AnoModeloParam","CodigoTipoCombustivelParam",
            "Valor","Marca","Modelo","AnoModelo","CodigoFipe","Combustivel",
            "TipoVeiculo","SiglaCombustivel","MesReferencia","Autenticacao","DataConsulta","ValorNum",
        ]
        self._existing_keys = set(); self._existing_param_keys = set()
        if self.out and not self.out.exists():
            with self.out.open("w", newline="", encoding="utf-8") as f:
                pd.DataFrame(columns=self.fieldnames).to_csv(f, index=False)
        if self.resume and self.out and self.out.exists():
            self._load_existing_keys()

    def _load_existing_keys(self):
        try:
            df = pd.read_csv(self.out)
            df = df[df["CodigoFipe"].notna()]
            for _, row in df.iterrows():
                self._existing_keys.add(unique_row_key(row))
                self._existing_param_keys.add(param_key_for_resume(
                    row.get("TabelaReferenciaCodigo"), row.get("CodigoTipoVeiculo"),
                    row.get("CodigoMarca"), row.get("CodigoModelo"), row.get("AnoModeloParam")
                ))
        except Exception:
            pass

    def has_row(self, row: Dict[str, Any]) -> bool:
        return self.resume and unique_row_key(row) in self._existing_keys

    def has_param_key(self, tref: str, tipo: str, cod_marca: str, cod_modelo: str, ano_param: str) -> bool:
        return self.resume and param_key_for_resume(tref, tipo, cod_marca, cod_modelo, ano_param) in self._existing_param_keys

    def write_row(self, row: Dict[str, Any]):
        if not self.out: return
        with self.out.open("a", newline="", encoding="utf-8") as f:
            pd.DataFrame([row], columns=self.fieldnames).to_csv(f, header=False, index=False)

async def build_model_catalog_for_type(client: FipeClient, cod_tab_seed: str, tipo: str, progress: bool) -> List[Dict[str, Any]]:
    models: List[Dict[str, Any]] = []
    marcas = await client._post_json(ENDPOINTS["marcas"], {"codigoTabelaReferencia": str(cod_tab_seed), "codigoTipoVeiculo": str(tipo)}) or []
    for mk in marcas:
        cod_marca = str(mk.get("Value", mk.get("Codigo","")))
        modelos_obj = await client._post_json(ENDPOINTS["modelos"], {"codigoTabelaReferencia": str(cod_tab_seed), "codigoTipoVeiculo": str(tipo), "codigoMarca": cod_marca}) or {}
        modelos = modelos_obj.get("Modelos", []) or []
        for model_item in modelos:
            cod_modelo = str(model_item.get("Value", model_item.get("Codigo","")))
            anos_raw = await client._post_json(ENDPOINTS["anos_modelo"], {"codigoTabelaReferencia": str(cod_tab_seed), "codigoTipoVeiculo": str(tipo), "codigoMarca": cod_marca, "codigoModelo": cod_modelo}) or []
            anos_distintos, anos_set = [], set()
            for a in anos_raw:
                v = str(a.get("Value",""))
                ano = v.split("-",1)[0] if "-" in v else v
                if ano not in anos_set:
                    anos_set.add(ano); anos_distintos.append(ano)
            rep_ano_val = None
            for a in anos_raw:
                v = str(a.get("Value",""))
                if v.startswith("32000-"):
                    rep_ano_val = v; break
            if rep_ano_val is None and anos_raw:
                rep_ano_val = sorted((str(a.get("Value","")) for a in anos_raw), reverse=True)[0]
            models.append({"tipo":str(tipo),"cod_marca":cod_marca,"cod_modelo":cod_modelo,"anos":anos_distintos,"rep_ano_val":rep_ano_val})
    return models

async def process_tasks_by_code(client: FipeClient, writer: CsvWriter, tasks: List[Dict[str, Any]],
                                progress_label: str, progress_enabled: bool = True):
    if not tasks:
        return 0
    bar = make_bar(len(tasks), progress_label, "req", progress_enabled)
    try:
        queue: asyncio.Queue = asyncio.Queue()
        for t in tasks: queue.put_nowait(t)

        async def worker():
            while True:
                try: item = queue.get_nowait()
                except asyncio.QueueEmpty: return
                try:
                    # Attempt querying with (year) first if not forced, then with specific fuel codes.
                    forced = FORCE_FUEL_CODE.get(str(item["fipe_code"]))
                    explicit = item.get("fuel_code")  # explicit codigoTipoCombustivel from tuples
                    # Candidate ordering:
                    if explicit:
                        candidates: List[Optional[str]] = [explicit]  # single fast attempt
                    elif forced:
                        candidates = [forced] + [c for c in FUEL_CODE_CANDIDATES if c != forced]
                    else:
                        candidates = [None] + FUEL_CODE_CANDIDATES  # first no fuel code

                    js = {}
                    chosen_fuel: Optional[str] = None
                    for fuel_code in candidates:
                        payload = {
                            "codigoTabelaReferencia": str(item["cod_tab"]),
                            "codigoTipoVeiculo": str(item["tipo"]),
                            "tipoVeiculo": TIPO_STR.get(str(item["tipo"]), "carro"),
                            "modeloCodigoExterno": str(item["fipe_code"]),
                            "tipoConsulta": "codigo",
                            # IMPORTANT: FIPE expects plain year or 32000; do not append -fuel
                            "anoModelo": str(item["ano_param"]),
                        }
                        if fuel_code is not None:
                            payload["codigoTipoCombustivel"] = fuel_code
                        try:
                            js = await client._post_json(ENDPOINTS["valor_todos_params"], payload) or {}
                        except Exception:
                            js = {}
                        if js.get("Valor") and js.get("CodigoFipe"):
                            chosen_fuel = fuel_code
                            break
                        if explicit:
                            # Do not fallback if explicit fuel code failed
                            break
                    row = {
                        "TabelaReferenciaCodigo": item["cod_tab"],
                        "CodigoTipoVeiculo": item["tipo"],
                        "CodigoMarca": "",
                        "CodigoModelo": "",
                        "AnoModeloParam": item["ano_param"],
                        "CodigoTipoCombustivelParam": chosen_fuel or "",
                        "Valor": js.get("Valor"),
                        "Marca": js.get("Marca"),
                        "Modelo": js.get("Modelo"),
                        "AnoModelo": js.get("AnoModelo"),
                        "CodigoFipe": js.get("CodigoFipe"),
                        "Combustivel": js.get("Combustivel"),
                        "TipoVeiculo": js.get("TipoVeiculo"),
                        "SiglaCombustivel": js.get("SiglaCombustivel"),
                        "MesReferencia": js.get("MesReferencia", item["mes_ref"]),
                        "Autenticacao": js.get("Autenticacao"),
                        "DataConsulta": js.get("DataConsulta"),
                        "ValorNum": parse_valor_brl(js.get("Valor")),
                    }
                    valid = js and js.get("CodigoFipe") and parse_valor_brl(js.get("Valor")) is not None
                    if valid and not writer.has_row(row):
                        writer.write_row(row)
                finally:
                    bar.update(1); queue.task_done()

        n_workers = min(getattr(client.sem, "_value", 8), 16)
        workers=[asyncio.create_task(worker()) for _ in range(n_workers)]
        await queue.join()
        for w in workers: w.cancel()
    finally:
        bar.close()
    return len(tasks)

def plan_tasks_from_tuples(code_years: Set[Tuple[str,int]], cod_tab: str, mes_ref: str, tipo: str = "1") -> List[Dict[str, Any]]:
    tasks=[]
    for tpl in sorted(code_years):
        if len(tpl) == 3:
            code, yr, fuel_code = tpl
        else:
            code, yr = tpl[0], tpl[1]; fuel_code = None
        task = {
            "cod_tab": cod_tab,
            "mes_ref": mes_ref,
            "tipo": str(tipo),
            "fipe_code": code,
            "ano_param": str(int(yr)),
        }
        if fuel_code:
            task["fuel_code"] = str(fuel_code)
        tasks.append(task)
    return tasks

async def fipe_list(args):
    log = logging.getLogger("fipe.list")
    conn = aiohttp.TCPConnector(limit=args.max_concurrency, ttl_dns_cache=300)
    cookie_jar = aiohttp.CookieJar(unsafe=True)
    if args.routeid: cookie_jar.update_cookies({"ROUTEID": str(args.routeid)})
    cache = DiskCache(Path(args.cache_dir))
    limiter = RateLimiter(args.rps)
    sem = asyncio.Semaphore(args.max_concurrency)
    async with aiohttp.ClientSession(connector=conn, cookie_jar=cookie_jar) as session:
        client = FipeClient(session, cache, limiter, sem, True)
        await client._bootstrap_cookies()
        tabs = await client._post_json(ENDPOINTS["tabelas"], {}) or []
        log.info("Meses (tabelas) disponíveis:")
        for t in tabs:
            cod=t.get("Value") or t.get("Codigo"); mes=t.get("Label") or t.get("Mes")
            y,m = parse_mes_label(mes or "")
            log.info("- %s: %s  (ISO %04d-%02d)", cod, mes, y, m)
        log.info("Total: %s", len(tabs))

async def fipe_dump(args):
    log = logging.getLogger("fipe.dump")
    if not args.out:
        args.out = str(FIPE_DIR / f"fipe_dump_{ymd_compact()}.csv")
    # Auto-load tuples if not already present (standalone mode)
    # Behavior: if --tuples-csv is provided, use ONLY that file; else use union of historical tuples
    if not getattr(args, 'tuples', None):
        tuples_csv_path = getattr(args, 'tuples_csv', None)
        if tuples_csv_path:
            logging.getLogger("fipe.dump").info("Using exclusive tuples from --tuples-csv: %s", tuples_csv_path)
            args.tuples = load_tuples_csv(Path(tuples_csv_path))
        else:
            tuples_set: Set[Tuple[str,int]] = set()
            # 1) History from Localiza/Movida
            hist = load_all_tuples_history(TUPLES_DIR)
            if hist:
                tuples_set |= hist
            # 2) Auto-include 0km tuples if present
            zero_km_csv = DATA_DIR / "0km_tuples.csv"
            if zero_km_csv.exists():
                z = load_tuples_csv(zero_km_csv)
                if z:
                    before = len(tuples_set)
                    tuples_set |= z
                    logging.getLogger("fipe.dump").info("Added 0km tuples: +%d (now %d)", len(tuples_set)-before, len(tuples_set))
            args.tuples = tuples_set
    cache_dir = Path(args.cache_dir)
    cache = DiskCache(cache_dir)
    limiter = RateLimiter(args.rps)
    sem = asyncio.Semaphore(args.max_concurrency)
    conn = aiohttp.TCPConnector(limit=args.max_concurrency, ttl_dns_cache=300)
    cookie_jar = aiohttp.CookieJar(unsafe=True)
    if getattr(args, "routeid", None): cookie_jar.update_cookies({"ROUTEID": str(args.routeid)})
    async with aiohttp.ClientSession(connector=conn, cookie_jar=cookie_jar) as session:
        client = FipeClient(session, cache, limiter, sem, args.progress)
        client.pow2_cap = float(args.throttle_cap)
        await client._bootstrap_cookies()

        tabelas = await client._post_json(ENDPOINTS["tabelas"], {}) or []
        for t in tabelas:
            t.setdefault("Codigo", t.get("Value"))
            t.setdefault("Mes", t.get("Label"))

        try:
            tabelas.sort(key=lambda x: int(x.get("Codigo", 0)), reverse=True)
        except Exception:
            pass

        use_auto = (args.since is None) or (str(args.since).strip().lower() in {"", "auto", "latest", "newest"})
        if args.tabelas:
            keep = {str(x) for x in args.tabelas}
            target_tabs = [t for t in tabelas if str(t.get("Codigo")) in keep]
        else:
            target_tabs = tabelas if use_auto else since_filter(tabelas, args.since)

        if not target_tabs:
            latest_tab = tabelas[0] if tabelas else None
            if latest_tab is None:
                log.error("Nenhuma tabela retornada pela FIPE.")
                return
            log.warning("Nenhuma tabela >= %s; usando a mais recente disponível: %s  (cod %s).",
                        args.since, latest_tab.get("Mes"), latest_tab.get("Codigo"))
            target_tabs = [latest_tab]
            #target_tabs = [{"Codigo": 325, "Mes": "setembro/2025"}]

        tuples: Optional[Set[Tuple[str,int]]] = getattr(args, "tuples", None)
        if not tuples:
            raise SystemExit(
                "fipe-dump: no (fipe_code, model_year) tuples available. Provide --tuples-csv or run run-all first."
            )

        writer = CsvWriter(Path(args.out), resume=args.resume, progress=args.progress)
        total_tasks = 0
        for tab in target_tabs:   # FIXED (honor since/tabelas)
            cod_tab = str(tab.get("Codigo"))
            mes_ref = tab.get("Mes")
            for tipo in [str(t) for t in (args.tipos or ["1"])]:
                tasks = plan_tasks_from_tuples(tuples, cod_tab, mes_ref, tipo)
                total_tasks += len(tasks)
                await process_tasks_by_code(
                    client, writer, tasks,
                    progress_label=f"{mes_ref} | tipo {tipo} (by-code tuples)",
                    progress_enabled=not args.no_progress
                )
        log.info("Concluído (tuples). Tarefas: %s | arquivo: %s", total_tasks, args.out)
        return

# -----------------------------------------------------------------------------
# Cleaning helper
# -----------------------------------------------------------------------------

def clean_keep_last_k(folder: Path, prefix: str, k: int):
    files = sorted(folder.glob(f"{prefix}_*.csv"))
    if len(files) <= k:
        logging.getLogger("clean").info("%s: nothing to delete (%s <= %s)", prefix, len(files), k); return
    to_del = files[:-k]
    for f in to_del:
        try:
            f.unlink(); logging.getLogger("clean").info("removed %s", f.name)
        except Exception as e:
            logging.getLogger("clean").warning("could not remove %s: %s", f.name, e)

# -----------------------------------------------------------------------------
# One-shot pipeline: scrape → parse → match → tuples-only FIPE
# -----------------------------------------------------------------------------

def run_all(args):
    log = logging.getLogger("run-all")

    localiza_today_csv = raw_today_path("localiza")
    movida_today_csv   = raw_today_path("movida")

    lcz_csv = None
    mvd_csv = None

    # LOCALIZA
    if not getattr(args, "force_scrape", False) and localiza_today_csv.exists():
        log.info("Localiza raw for today already exists: %s — skipping scrape/parse", localiza_today_csv.name)
        lcz_csv = localiza_today_csv
    else:
        asyncio.run(scrape_localiza(SimpleNamespace(
            out="localiza_offers.json",
            concurrency=args.concurrency,
            rps=getattr(args, "localiza_rps", 0.8),
            delay=getattr(args, "localiza_delay", 1.0),
            no_progress=args.no_progress
        )))
        parse_localiza(Path("localiza_offers.json"), RAW_LOCALIZA)
        lcz_csv = latest("localiza_seminovos_*.csv", RAW_LOCALIZA)

    # MOVIDA
    if not getattr(args, "force_scrape", False) and movida_today_csv.exists():
        log.info("Movida raw for today already exists: %s — skipping scrape/parse", movida_today_csv.name)
        mvd_csv = movida_today_csv
    else:
        scrape_movida(SimpleNamespace(
            out="movida_offers.json",
            delay=args.movida_delay,
            max_pages=args.movida_max_pages,
            gad_source=None, gad_campaignid=None, gbraid=None, gclid=None,
            no_progress=args.no_progress
        ))
        parse_movida(Path("movida_offers.json"), RAW_MOVIDA)
        mvd_csv = latest("movida_seminovos_*.csv", RAW_MOVIDA)

    if not lcz_csv or not mvd_csv:
        raise SystemExit("run-all: parsed CSVs not found.")

    # Match (prefer fipe_models.csv), skip if today's exists
    existing_match = None if getattr(args, "force_match", False) else find_today_match_csv()
    if existing_match:
        logging.getLogger("run-all").info(
            "Localiza_with_fipe_match for today already exists: %s — skipping match",
            existing_match.name,
        )
        match_out = existing_match
    else:
        fm_path = find_fipe_models_csv()
        match_out = MATCH_DIR / f"localiza_with_fipe_match_{ymd_compact()}.csv"
        match_localiza(
            lcz_csv,
            None,
            match_out,
            threshold=args.threshold,
            movida_csv_fallback=mvd_csv,
            fipe_models_csv=fm_path,
        )

    # Tuples & audit
    tuples = collect_fipe_tuples(match_out, mvd_csv)
    audit_csv = tuples_audit(match_out, mvd_csv)
    # Expand with historical tuples so we cover previously seen pairs as well
    try:
        hist_union = load_all_tuples_history(TUPLES_DIR)
        if hist_union:
            before = len(tuples)
            tuples |= hist_union
            logging.getLogger("run-all").info("Expanded tuples with history: %d -> %d distinct tuples", before, len(tuples))
    except Exception as e:
        logging.getLogger("run-all").warning("Could not load historical tuples; proceeding with today's only: %s", e)
    if not tuples:
        raise SystemExit("run-all: no (fipe_code, model_year) tuples found to fetch from FIPE.")
    fipe_out = FIPE_DIR / f"fipe_dump_{ymd_compact()}.csv"

    fargs = SimpleNamespace(
        out=str(fipe_out),
        since=args.since,
        tabelas=None,
        tipos=args.tipos,
        max_concurrency=args.max_concurrency,
        rps=args.rps,
        cache_dir=args.cache_dir,
        resume=args.resume,
        progress=args.progress,
        progress_interval=args.progress_interval,
        seed_tabela=args.seed_tabela,
        discover_rps=args.discover_rps,
        discover_max_concurrency=args.discover_max_concurrency,
        routeid=args.routeid,
        throttle_cap=args.throttle_cap,
        tuples=tuples,
        no_progress=args.no_progress,
    )
    asyncio.run(fipe_dump(fargs))

    logging.getLogger("run-all").info(
        "Completed.\n  Localiza CSV  : %s\n  Movida  CSV   : %s\n  Match output  : %s\n  Tuples audit  : %s\n  FIPE dump     : %s (tuples-only)",
        lcz_csv.name, mvd_csv.name, match_out.name, Path(audit_csv).name, fipe_out.name
    )

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(prog="fleet.py", description="Movida/Localiza scrapers, parsing, matching & FIPE dump (tuples-only)")
    # Global logging/progress controls
    p.add_argument("--log-level", choices=["DEBUG","INFO","WARNING","ERROR","CRITICAL"], default=None,
                   help="Logging level (default INFO; DEBUG if --verbose)")
    p.add_argument("--no-progress", action="store_true", help="Disable progress bars")
    sub = p.add_subparsers(dest="cmd", required=True)

    # scrape-localiza
    sl = sub.add_parser("scrape-localiza", help="Scrape Localiza seminovos listings to JSON (+meta)")
    sl.add_argument("--out", default="localiza_offers.json")
    sl.add_argument("--concurrency", type=int, default=3, help="Concurrent page fetches (default 3)")
    sl.add_argument("--rps", type=float, default=0.8, help="Shared requests per second cap (default 0.8)")
    sl.add_argument("--delay", type=float, default=1.0, help="Base jittered delay between page fetches (s)")

    # scrape-movida
    sm = sub.add_parser("scrape-movida", help="Scrape Movida seminovos API to JSON (+meta)")
    sm.add_argument("--out", default="movida_offers.json")
    sm.add_argument("--delay", type=float, default=1.2, help="Base delay between Movida API page requests (s)")
    sm.add_argument("--max-pages", type=int, default=None)
    sm.add_argument("--gad_source", default=None)
    sm.add_argument("--gad_campaignid", default=None)
    sm.add_argument("--gbraid", default=None)
    sm.add_argument("--gclid", default=None)

    # parse-movida
    mvd = sub.add_parser("parse-movida", help="Convert movida_offers.json -> dated CSV")
    mvd.add_argument("--in", dest="in_json", default="movida_offers.json")
    mvd.add_argument("--out-dir", default=str(RAW_MOVIDA))

    # parse-localiza
    lcz = sub.add_parser("parse-localiza", help="Convert Localiza localiza_offers.json -> dated CSV")
    lcz.add_argument("--in", dest="in_json", default="localiza_offers.json")
    lcz.add_argument("--out-dir", default=str(RAW_LOCALIZA))

    # match-localiza
    mtc = sub.add_parser("match-localiza", help="Match Localiza CSV to FIPE (fipe_models.csv preferred)")
    mtc.add_argument("--localiza-csv", default=None, help="Defaults to newest raw/localiza/localiza_seminovos_*.csv")
    mtc.add_argument("--fipe-csv", default=None, help="Optional older path (fallback if models CSV missing)")
    mtc.add_argument("--movida-fallback-csv", default=None, help="Defaults to newest raw/movida/movida_seminovos_*.csv")
    mtc.add_argument("--fipe-models-csv", default=None, help="Path to fipe_models.csv (default: auto-discover)")
    mtc.add_argument("--out", default=str(DATA_DIR / f"localiza_with_fipe_match_{ymd_compact()}.csv"))
    mtc.add_argument("--threshold", type=float, default=0.0)

    # fipe-list
    fls = sub.add_parser("fipe-list", help="List FIPE reference tables (months)")
    fls.add_argument("--max-concurrency", type=int, default=1)
    fls.add_argument("--rps", type=float, default=2.0)
    fls.add_argument("--cache-dir", default=".fipe_cache")
    fls.add_argument("--routeid", help="Cookie ROUTEID (ex.: .13)")

    # fipe-dump
    fdp = sub.add_parser("fipe-dump", help="Dump FIPE prices; tuples-only if run via run-all")
    fdp.add_argument("--out", default=None, help="Defaults to data/fipe/fipe_dump_YYYYMMDD.csv")
    fdp.add_argument("--since", help="YYYY-MM filter for tables, e.g., 2024-07")
    fdp.add_argument("--tabelas", nargs="+")
    fdp.add_argument("--tipos", nargs="+", default=["1"])
    fdp.add_argument("--max-concurrency", type=int, default=8)
    fdp.add_argument("--rps", type=float, default=2.0)
    fdp.add_argument("--cache-dir", default=".fipe_cache")
    fdp.add_argument("--resume", action="store_true")
    fdp.add_argument("--progress", action="store_true")
    fdp.add_argument("--progress-interval", type=float, default=5.0)
    fdp.add_argument("--seed-tabela")
    fdp.add_argument("--fallback-param", action="store_true")
    fdp.add_argument("--discover-rps", type=float, default=0.6)
    fdp.add_argument("--discover-max-concurrency", type=int, default=2)
    fdp.add_argument("--routeid")
    fdp.add_argument("--throttle-cap", type=float, default=8.0)
    fdp.add_argument("--tuples-csv", help="Independent CSV with fipe_code,model_year (optionally with fuel_code)")

    # build-tables
    tbl = sub.add_parser("build-tables", help="Generate Localiza, Movida and FIPE output tables")
    tbl.add_argument("--localiza-csv", default=None, help="Localiza matched CSV")
    tbl.add_argument("--movida-csv", default=None, help="Movida CSV")
    tbl.add_argument("--fipe-csv", default=None, help="FIPE dump CSV")
    tbl.add_argument("--out-dir", default=str(TABLES_DIR))

    # clean
    cln = sub.add_parser("clean", help="Keep only last K dated CSVs per vendor")
    cln.add_argument("--folder", default="raw")
    cln.add_argument("--keep", type=int, default=3)

    # run-all
    allp = sub.add_parser("run-all", help="Scrape → Parse → Match → FIPE (tuples-only)")
    allp.add_argument("--concurrency", type=int, default=3, help="Localiza scraper concurrency")
    allp.add_argument("--localiza-rps", type=float, default=0.8, help="Localiza shared requests per second cap")
    allp.add_argument("--localiza-delay", type=float, default=1.0, help="Localiza base jittered delay (s)")
    allp.add_argument("--verbose", action="store_true", help="Alias for --log-level DEBUG")
    allp.add_argument("--movida-delay", type=float, default=1.2, help="Movida delay between page requests (s)")
    allp.add_argument("--movida-max-pages", type=int, default=None, help="Movida max pages (debug)")
    # FIPE controls
    allp.add_argument("--since", default="auto", help="FIPE since: YYYY-MM or 'auto' (use latest available)")
    allp.add_argument("--tipos", nargs="+", default=["1"], help="FIPE tipos: 1=Carros, 2=Motos, 3=Caminhões (default: 1)")
    allp.add_argument("--max-concurrency", type=int, default=8)
    allp.add_argument("--rps", type=float, default=2.0)
    allp.add_argument("--cache-dir", default=".fipe_cache")
    allp.add_argument("--resume", action="store_true")
    allp.add_argument("--progress", action="store_true")
    allp.add_argument("--progress-interval", type=float, default=5.0)
    allp.add_argument("--seed-tabela")
    allp.add_argument("--discover-rps", type=float, default=0.6)
    allp.add_argument("--discover-max-concurrency", type=int, default=2)
    allp.add_argument("--routeid")
    allp.add_argument("--throttle-cap", type=float, default=8.0)
    allp.add_argument("--threshold", type=float, default=0.62, help="Matcher acceptance threshold")
    allp.add_argument("--force-scrape", action="store_true", help="Ignore today's raw CSVs check and always scrape/parse again")
    allp.add_argument("--force-match", action="store_true", help="Ignore today's Localiza_with_fipe_match file and run matching anyway")

    args = p.parse_args()
    setup_logging(args.log_level, getattr(args, "verbose", False))

    if args.cmd == "scrape-localiza":
        asyncio.run(scrape_localiza(args))
    elif args.cmd == "scrape-movida":
        scrape_movida(args)
    elif args.cmd == "parse-movida":
        parse_movida(Path(args.in_json), Path(args.out_dir))
    elif args.cmd == "parse-localiza":
        parse_localiza(Path(args.in_json), Path(args.out_dir))
    elif args.cmd == "match-localiza":
        lcz_path = Path(args.localiza_csv) if args.localiza_csv else latest("localiza_seminovos_*.csv", RAW_LOCALIZA)
        if not lcz_path or not lcz_path.exists():
            raise SystemExit("No Localiza CSV found (raw/localiza/localiza_seminovos_*.csv).")
        fipe_path = Path(args.fipe_csv) if args.fipe_csv else (latest("fipe_dump_*.csv", FIPE_DIR) or latest("fipe_dump*.csv", Path(".")))
        mv_fallback = Path(args.movida_fallback_csv) if args.movida_fallback_csv else latest("movida_seminovos_*.csv", RAW_MOVIDA)
        fm_path = Path(args.fipe_models_csv) if args.fipe_models_csv else find_fipe_models_csv()
        out_path = Path(args.out) if args.out else MATCH_DIR / f"localiza_with_fipe_match_{ymd_compact()}.csv"
        match_localiza(lcz_path, fipe_path, out_path, args.threshold, movida_csv_fallback=mv_fallback, fipe_models_csv=fm_path)
    elif args.cmd == "fipe-list":
        asyncio.run(fipe_list(args))
    elif args.cmd == "fipe-dump":
        # Build tuple set for dump.
        if getattr(args, 'tuples_csv', None):
            logging.getLogger("fipe.dump").info("Using exclusive tuples from --tuples-csv: %s", args.tuples_csv)
            tuple_set = load_tuples_csv(Path(args.tuples_csv))
        else:
            tuple_set: Set[Tuple[str,int]] = set()
            # 1) History union from Localiza/Movida
            hist_union = load_all_tuples_history(TUPLES_DIR)
            if hist_union:
                tuple_set |= hist_union
            # 2) Auto-include 0km tuples if present
            zero_km_csv = DATA_DIR / "0km_tuples.csv"
            if zero_km_csv.exists():
                z = load_tuples_csv(zero_km_csv)
                if z:
                    before = len(tuple_set)
                    tuple_set |= z
                    logging.getLogger("fipe.dump").info("Added 0km tuples: +%d (now %d)", len(tuple_set)-before, len(tuple_set))
        if not tuple_set:
            raise SystemExit("fipe-dump CLI: no tuples found. Provide --tuples-csv or run run-all to generate tuples first.")
        args.tuples = tuple_set
        logging.getLogger("fipe.dump").info("CLI tuples selected: %s distinct tuples", len(args.tuples))
        asyncio.run(fipe_dump(args))
    elif args.cmd == "build-tables":
        export_tables(args)
    elif args.cmd == "clean":
        base = Path(args.folder)
        clean_keep_last_k(base / "movida",   "movida_seminovos", args.keep)
        clean_keep_last_k(base / "localiza", "localiza_seminovos", args.keep)
    elif args.cmd == "run-all":
        run_all(args)

if __name__ == "__main__":
    main()
