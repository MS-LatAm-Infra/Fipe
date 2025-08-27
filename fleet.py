#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# fleet.py — unified scrapers, parsers, matcher, and FIPE dump (tuples-only capable)
# Logging & progress standardized with logging + tqdm

import argparse
import asyncio
import aiohttp
import json
import os
import sys
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
RAW_DIR      = Path("raw")
RAW_LOCALIZA = RAW_DIR / "localiza"
RAW_MOVIDA   = RAW_DIR / "movida"

# Where the auditable match table lives
MATCH_DIR = DATA_DIR / "localiza"
MATCH_DIR.mkdir(parents=True, exist_ok=True)
MATCH_TABLE = DATA_DIR / "localiza_match_table.csv"
VERSION_MATCH_TABLE = DATA_DIR / "localiza_version_match.csv"  # simplified cache (brand_norm, model_norm, version_norm, model_year) → fipe_code (model_token deprecated)


for d in (DATA_DIR, FIPE_DIR, TUPLES_DIR, RAW_LOCALIZA, RAW_MOVIDA):
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
    url = f"https://seminovos.localiza.com/carros?page={page}"
    async with session.get(url, headers=LOCALIZA_HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
        resp.raise_for_status()
        html = await resp.text()
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if not tag or not tag.string:
        raise RuntimeError("Could not find __NEXT_DATA__ on page")
    data = json.loads(tag.string)
    return data

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
    connector = aiohttp.TCPConnector(limit=args.concurrency, limit_per_host=args.concurrency, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
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
                    await asyncio.sleep(0.05 + random.random() * 0.1)

            workers = [asyncio.create_task(worker()) for _ in range(args.concurrency)]
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
    sess = _movida_session()
    offset = None
    all_offers = []
    seen = set()
    pages = 0
    first_page_size = None
    total_from_api = None

    bar = None
    try:
        while True:
            payload = _movida_payload(offset, args.gad_source, args.gad_campaignid, args.gbraid, args.gclid)
            log_mov.debug("requesting offset=%r", payload["from"])
            resp = sess.post(MOVIDA_URL, headers=MOVIDA_HEADERS, json=payload, timeout=30)
            if resp.status_code != 200:
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
            time.sleep(max(0.0, args.delay))
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

    rows = []
    for o in raw:
        if not (o.get("modelo") and o.get("versao") and o.get("ano_modelo")):
            continue
        rows.append({
            "offer_id": o.get("id"),
            "fipe_code": o.get("codigo_fipe"),
            "type": o.get("categoria"),
            "brand": o.get("marca"),
            "model": o.get("modelo"),
            "version_raw": o.get("versao"),
            "manufacture_year": o.get("ano_fabricacao"),
            "model_year": o.get("ano_modelo"),
            "price": o.get("preco"),
            "city": o.get("cidade"),
            "state": o.get("uf") or o.get("estado"),
            "store_code": o.get("codigo_filial") or o.get("filial_codigo"),
            "store_name": o.get("filial") or o.get("loja"),
            "source": "movida",
        })

    df = pd.DataFrame.from_records(rows)
    if df.empty:
        log_parse.warning("[movida] no rows to write")
        return

    df["price"] = df["price"].apply(clean_price_to_int).astype("Int64")
    df["manufacture_year"] = pd.to_numeric(df["manufacture_year"], errors="coerce").astype("Int64")
    df["model_year"] = pd.to_numeric(df["model_year"], errors="coerce").astype("Int64")

    for col in ["type","brand","model","version_raw"]:
        df[col] = df[col].astype(str).str.strip().str.lower()

    df["version"] = (
        df["model"].astype(str).str.strip() + " " + df["version_raw"].astype(str).str.strip()
    ).str.replace(r"\s+", " ", regex=True).str.strip()

    if "offer_id" in df.columns:
        df = df.drop_duplicates(subset=["offer_id"])

    df.insert(0, "snapshot_date", snap)

    ensure_dir(out_dir)
    out = out_dir / f"movida_seminovos_{snap.replace('-', '')}.csv"
    df.to_csv(out, index=False, sep=";")
    log_parse.info("[movida] wrote %s rows -> %s", len(df), out)

def parse_localiza(in_json: Path, out_dir: Path):
    meta = in_json.with_suffix(".meta.json")
    snap = snapshot_date_from_meta(meta)  # use meta date; do NOT overwrite with today
    raw = json.loads(in_json.read_text(encoding="utf-8"))
    df = pd.DataFrame([{
        "type": o.get("categoriaDescricao"),
        "brand": o.get("marcaDescricao"),
        "model": o.get("modeloFamiliaDescricao"),
        "version_raw": o.get("modeloDescricao"),
        "manufacture_year": o.get("anoFabricacao"),
        "model_year": o.get("anoModelo"),
        "price": o.get("preco"),
        "fuel": o.get("tipoCombustivelDescricao"),
        "transmission": o.get("tipoTransmissaoDescricao"),
    } for o in raw if o.get("modeloFamiliaDescricao") and o.get("modeloDescricao") and o.get("anoModelo")])

    if df.empty:
        log_parse.warning("[localiza] no rows to write")
        return
    
    df["version"] = df["version_raw"].copy()
    
    df["type"] = np.where((df["version"] == "ONIX LT MT 1.0 4P"), "HATCH", df["type"])
    df["type"] = np.where((df["version"] == "ONIX PLUS TURBO LT AT 1.0 4P"), "SEDAN", df["type"])
    df["type"] = np.where((df["version"] == "HB20S PLATINUM TGDI AT 1.0 4P"), "SEDAN", df["type"])
    df["type"] = df["type"].replace(to_replace=["FURGAO", "PICAPE", "PICAPE CABINE DUPLA", "CABINE SIMPLES", "SPORTBACK", "PARTICULAR", "COUPE", "CARGA"],
                                            value=["UTILITARIO", "PICK-UP", "PICK-UP", "PICK-UP", "PREMIUM", "OTHER", "PREMIUM", "VAN"])
    df["transmission"] = df["transmission"].replace(to_replace=["MANUAL", "AUTOMÁTICO", "AUTOMATICO", "AUTOMáTICO", "TIPTRONIC", "ELECTRICO"],
                                            value=["mec.", "aut.", "aut.", "aut.", "tiptr.", "(eletrico)"])
    df["fuel"] = df["fuel"].replace(to_replace=["GASOLINA/ETANOL", "ÁLC/GASOL", "FLEX/GNV", "ETANOL/GNV", "ELÉT/GASOLINA/ETANOL", "ELÉTRICO/GASOLINA", "ELÉTRICO", "GASOLINA"],
                                            value=["flex", "flex", "flex", "flex", "(hibrido)", "(hib.)", "(elétrico)", ""])
    df["version"] = (df["version"].astype("string").str.replace(r"\bonix[\s-]+plus\b", "onix sedan plus", regex=True, flags=re.IGNORECASE))
    df["version"] = df["version"].str.replace(r"\bonix\b(?![\s-]*(?:sedan|hatch))","onix hatch",regex=True,flags=re.IGNORECASE)
    df["version"] = (df["version"].astype("string").str.replace(" mt ", " mec ", regex=False, flags=re.IGNORECASE))

    df["price"] = df["price"].apply(clean_price_to_int).astype("Int64")
    df["manufacture_year"] = pd.to_numeric(df["manufacture_year"], errors="coerce").astype("Int64")
    df["model_year"] = pd.to_numeric(df["model_year"], errors="coerce").astype("Int64")


    df["version"] = (
        df["version_raw"].str.replace(r"\bonix[\s-]+plus\b","onix sedan plus", regex=True, flags=re.IGNORECASE)
                          .str.replace(r"\bonix\b(?![\s-]*(?:sedan|hatch))","onix hatch", regex=True, flags=re.IGNORECASE)
                          .str.replace(" mt "," mec ", regex=False)
    )

    for c in ["type","brand","model","version_raw","fuel","transmission"]:
        df[c] = df[c].astype(str).str.strip().str.lower()

    df["version"] = (df["version"].astype(str).str.strip()
                     + " " + df["fuel"].astype(str).str.strip()
                     + " " + df["transmission"].astype(str).str.strip()) \
                     .str.replace(r"\s+"," ", regex=True).str.strip()

    df = df.drop(columns=["fuel","transmission"])

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
                   fipe_models_csv: Optional[Path] = None,
                   strategy: str = "contains"):
    """Simplified Localiza → FIPE matcher (algorithm from localiza_match.py).

    Replaces the previous auditable match-table pipeline with a direct fuzzy
    scorer per row. Keeps the same public signature so CLI commands continue
    to work. Unmatched rows are kept (fipe_code empty) instead of aborting.
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
    # Normalize AnoModelo: keep sentinel 32000 (zero-km) separate; ensure Int64
    fipe["AnoModelo"] = pd.to_numeric(fipe["AnoModelo"], errors="coerce").astype("Int64")
    # Deduplicate by (CodigoFipe, AnoModelo) keeping first (fuel/reference month differences not needed for matching)
    fipe = fipe.sort_values(["CodigoFipe","AnoModelo"]).drop_duplicates(subset=["CodigoFipe","AnoModelo"]).reset_index(drop=True)

    # If the user requested the new simple brand/year + model containment strategy, run it and return early.
    if strategy == "contains":
        # Prepare normalization helpers from existing functions
        # Reuse norm_text (version-level) and norm_brand defined above.
        def _safe_norm_version(s: str) -> str:
            return norm_text(s)
        def _safe_norm_model(s: str) -> str:
            # Model normalization should be lighter; reuse generic_norm_text if model tokens are simpler
            return generic_norm_text(s)

        # Normalize Localiza rows
        loc["_brand_norm"] = loc["brand"].map(norm_brand)
        loc["_model_norm_lcz"] = loc["model"].map(_safe_norm_model)
        loc["_version_norm"] = loc["version"].map(_safe_norm_version)

        # Normalize FIPE
        fipe_norm = fipe.copy()
        fipe_norm["_brand_norm"] = fipe_norm["Marca"].map(norm_brand)
        fipe_norm["_model_norm"] = fipe_norm["Modelo"].map(norm_text)  # full normalization for robust contains

        results: List[Dict[str, Any]] = []
        for idx, row in loc.iterrows():
            brand_n = row["_brand_norm"]
            year = row.get("model_year")
            model_n = row["_model_norm_lcz"] or ""
            version_n = row["_version_norm"] or ""

            if pd.isna(year) or not model_n:
                results.append({"fipe_code": None, "fipe_model": None, "match_score": 0.0})
                continue

            # Step 1: brand + same model year filter
            cand = fipe_norm[(fipe_norm["_brand_norm"] == brand_n) & (fipe_norm["AnoModelo"] == int(year))]
            if cand.empty:
                results.append({"fipe_code": None, "fipe_model": None, "match_score": 0.0})
                continue

            # Step 2: ensure each remaining FIPE model contains the Localiza model (substring containment over normalized text)
            # Use simple containment OR all model tokens subset of candidate tokens.
            model_tok_set = set(model_n.split())
            def _model_contains(fipe_model_norm: str) -> bool:
                f_toks = set(fipe_model_norm.split())
                return model_n in fipe_model_norm or model_tok_set.issubset(f_toks)
            cand = cand[cand["_model_norm"].apply(_model_contains)]
            if cand.empty:
                results.append({"fipe_code": None, "fipe_model": None, "match_score": 0.0})
                continue

            # Step 3: score each remaining FIPE model comparing to Localiza normalized version
            v_toks = set(version_n.split()) if version_n else set()
            best_score = -1.0
            best_code = None
            best_model = None
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
                    best_score = score
                    best_code = fr["CodigoFipe"]
                    best_model = fr["Modelo"]

            if best_code:
                results.append({
                    "fipe_code": best_code,
                    "fipe_model": best_model,
                    "match_score": round(float(best_score), 4),
                    "match_accepted": 1 if best_score >= threshold else 0
                })
            else:
                results.append({
                    "fipe_code": None,
                    "fipe_model": None,
                    "match_score": 0.0,
                    "match_accepted": 0
                })
        # Attach results to DataFrame
        res_df = pd.DataFrame(results)
        loc = pd.concat([loc.reset_index(drop=True), res_df], axis=1)
        # Build / update version match cache (localiza_version_match.csv) for auditing consistency
        try:
            today_iso = date.today().isoformat()
            cache_df = load_version_match_table()
            vm_rows = []
            for _, r in loc.iterrows():
                vm_rows.append({
                    "brand_norm": r.get("_brand_norm"),
                    "model_norm": r.get("_model_norm_lcz"),
                    "version_norm": r.get("_version_norm"),
                    "model_year": r.get("model_year"),
                    "fipe_brand": None,  # brand from FIPE (we can look it up if matched)
                    "fipe_model": r.get("fipe_model"),
                    "fipe_code": r.get("fipe_code"),
                    "score": r.get("match_score"),
                    "match_source": "contains",
                    "first_seen": today_iso,
                    "last_seen": today_iso,
                })
            add_df = pd.DataFrame(vm_rows)
            # If we want the FIPE brand for matched rows, map via fipe table
            if not add_df.empty:
                matched_codes = add_df[add_df["fipe_code"].notna()]["fipe_code"].unique().tolist()
                if matched_codes:
                    fipe_brand_map = fipe.set_index("CodigoFipe")["Marca"].to_dict()
                    add_df.loc[add_df["fipe_code"].notna(), "fipe_brand"] = add_df.loc[add_df["fipe_code"].notna(), "fipe_code"].map(fipe_brand_map)
            # Merge with existing cache: keep oldest first_seen, update last_seen
            if not cache_df.empty:
                key_cols = ["brand_norm","model_norm","version_norm","model_year"]
                cache_df = cache_df.copy()
                add_df = add_df.copy()
                # Align dtypes
                add_df["model_year"] = pd.to_numeric(add_df["model_year"], errors="coerce").astype("Int64")
                cache_df["model_year"] = pd.to_numeric(cache_df["model_year"], errors="coerce").astype("Int64")
                merged = pd.merge(add_df, cache_df, on=key_cols, how="left", suffixes=("","_old"))
                # If existed, keep original first_seen and update last_seen to today
                existed_mask = merged["fipe_code_old"].notna() | merged["score_old"].notna()
                merged.loc[existed_mask, "first_seen"] = merged.loc[existed_mask, "first_seen_old"].fillna(today_iso)
                merged.loc[existed_mask, "last_seen"] = today_iso
                # Prefer new match data where score improved or previously empty
                def pick(col):
                    return np.where(merged[f"{col}_old"].notna(), merged[col].fillna(merged[f"{col}_old"]), merged[col])
                for c in ["fipe_brand","fipe_model","fipe_code","score","match_source"]:
                    if f"{c}_old" in merged.columns:
                        merged[c] = pick(c)
                keep_cols = version_match_table_columns()
                add_df_final = merged[keep_cols]
                # Combine with cache (drop keys from cache that are being replaced)
                existing_keys = set(tuple(x) for x in add_df_final[key_cols].itertuples(index=False, name=None))
                cache_filtered = cache_df[~cache_df[key_cols].apply(tuple, axis=1).isin(existing_keys)]
                cache_df = pd.concat([cache_filtered, add_df_final], ignore_index=True, sort=False)
            else:
                cache_df = add_df[version_match_table_columns()]
            save_version_match_table(cache_df)
        except Exception as e:
            log.warning("could not update version match cache (contains strategy): %s", e)
        # Write output
        out_csv = Path(out_csv)
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        loc.to_csv(out_csv, index=False, sep=";")
        log.info("wrote %s rows -> %s (contains strategy)", len(loc), out_csv)
        return

    # ---- Normalization helpers (legacy strategy below) ----
    def _strip_accents(s: str) -> str:
        return "".join(c for c in unicodedata.normalize("NFD", str(s)) if unicodedata.category(c) != "Mn")

    def _remove_duplicate_words(text: str) -> str:
        seen=set(); out=[]
        for w in str(text).split():
            k = w.casefold()
            if k not in seen:
                seen.add(k); out.append(w)
        return " ".join(out)

    def _norm_text(s: str) -> str:
        if s is None or (isinstance(s,float) and pd.isna(s)): return ""
        s0 = _strip_accents(str(s).lower())
        s0 = s0.replace(",", ".")
        s0 = s0.replace("c/ar", "")
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
        s0 = re.sub(r'(?i)(?<!\w)(?:cs)(?!\w)', 'cabine simples', s0)
        s0 = re.sub(r'(?i)(?<!\w)(?:cd)(?!\w)', 'cabine dupla', s0)
        s0 = re.sub(r'(?i)(?<!\w)(?:7l)(?!\w)', '', s0)
        s0 = re.sub(r'(?i)(?<!\w)(?:hurric|hurr)(?!\w)', 'hurricane', s0)
        s0 = re.sub(r'(?i)(?<!\w)(?:overl)(?!\w)', 'overland', s0)
        s0 = re.sub(r'(?i)(?<!\w)(?:dies|die)(?!\w)', 'diesel', s0)
        s0 = re.sub(r'(?i)(?<!\w)(?:tb)(?!\w)', 'turbo', s0)
        s0 = re.sub(r'(?i)(?<!\w)(?:sed)(?!\w)', 'sedan', s0)
        s0 = re.sub(r'(?i)(?<!\w)(?:step)(?!\w)', 'stepway', s0)
        s0 = re.sub(r'(?i)(?<!\w)(?:hig)(?!\w)', 'highline', s0)
        s0 = re.sub(r'(?i)(?<!\w)(?:limit)(?!\w)', 'limited', s0)
        s0 = re.sub(r'(?i)(?<!\w)(?:plat)(?!\w)', 'platinum', s0)
        s0 = re.sub(r'(?i)\b\d+[pv]\b', '', s0)
        s0 = re.sub(r'(?i)(?<!\w)(?:exclu)(?!\w)', 'exclusive', s0)
        s0 = re.sub(r'(?i)(?<!\w)(?:t270)(?!\w)', 'turbo 270', s0)
        s0 = re.sub(r'(?i)(?<!\w)(?:comfort|comfor)(?!\w)', 'comfortline', s0)
        s0 = re.sub(r'(?i)\bONIX\s+HATCH\s+PREM\.\b', 'onix hatch premier', s0)
        s0 = re.sub(r'(?i)\bONIX\s+SEDAN\s+PREM\.\b', 'onix sedan premier', s0)
        s0 = re.sub(r'(?i)\bONIX\s+SEDAN\s+Plus+\s+PREM\.\b', 'onix sedan plus premier', s0)
        s0 = re.sub(r'(?i)\bONIX\s+SD\. +\s+P\. +\s+PR\.\b', 'onix sedan plus premier', s0)
        s0 = re.sub(r'(?i)\bFastback\s+Limited+\s+Ed\.\b', 'fastback limited edition', s0)
        s0 = re.sub(r'(?i)\bAIRCROSS\s+F\.\b', 'aircross feel', s0)
        s0 = re.sub(r'\bnew\b(?![\s-]*(?:range|beetle)\b)', '', s0, flags=re.IGNORECASE)
        s0 = re.sub(r'(?i)(?<!\w)(?:mec)(?!\w)', '', s0)
        s0 = re.sub(r"\s+", " ", s0).strip()
        s0 = _remove_duplicate_words(s0)
        return s0

    def _first_token(s: str) -> str:
        s0 = _norm_text(s)
        return s0.split(" ")[0] if s0 else ""

    def _remove_token_whole_word(s: str, tok: str) -> str:
        if not tok: return s
        return re.sub(rf"\b{re.escape(tok)}\b", " ", s).replace("  ", " ").strip()

    def _tokset(s: str) -> set:
        return set(_norm_text(s).split())

    def _extract_engine(s: str):
        m = re.search(r"\b(\d\.\d)\b", s)
        return m.group(1) if m else None

    # ---- Normalize Localiza columns ----
    loc["_brand_norm"]   = loc["brand"].map(norm_brand)
    loc["_model_norm"]   = loc["model"].map(_norm_text)
    # _model_token deprecated as key – keep for potential diagnostics but not used for matching
    loc["_version_norm"] = loc["version"].map(_norm_text)

    # ---- Normalize FIPE ----
    fipe["_brand_norm"] = fipe["Marca"].map(norm_brand)
    fipe["_model_norm"] = fipe["Modelo"].map(_norm_text)

    # ---- Build brand/year indices ----
    from collections import defaultdict
    index_brand_year = defaultdict(list)      # (brand_norm, year) -> row indices
    index_brand = defaultdict(list)           # brand_norm -> row indices (fallback)
    index_brand_year_32000 = defaultdict(list)  # (brand_norm, 32000) sentinel rows
    for idx, row in fipe.iterrows():
        brand_key = row["_brand_norm"]
        year_val = row["AnoModelo"]
        if pd.notna(year_val):
            index_brand_year[(brand_key, int(year_val))].append(idx)
            if int(year_val) == 32000:
                index_brand_year_32000[(brand_key, 32000)].append(idx)
        index_brand[brand_key].append(idx)

    # ---- Load / merge existing version match cache ----
    cache_df = load_version_match_table()
    today_iso = date.today().isoformat()
    key_cols = ["_brand_norm","_model_norm","_version_norm","model_year"]
    cache_key_cols = ["brand_norm","model_norm","version_norm","model_year"]
    loc = loc.merge(
        cache_df.rename(columns={"brand_norm":"_brand_norm","model_norm":"_model_norm","version_norm":"_version_norm"})[
            ["_brand_norm","_model_norm","_version_norm","model_year","fipe_brand","fipe_model","fipe_code","score","match_source"]
        ],
        on=["_brand_norm","_model_norm","_version_norm","model_year"],
        how="left",
        suffixes=("","_cache")
    )

    # Identify new keys not in cache
    loc["_is_new_key"] = loc["fipe_code"].isna()
    new_keys = (loc[loc["_is_new_key"]][key_cols]
                .drop_duplicates()
                .rename(columns={"_brand_norm":"brand_norm","_model_norm":"model_norm","_version_norm":"version_norm"}))

    if not new_keys.empty:
        log.info("Detected %d new Localiza version(s) to match.", len(new_keys))
        # Perform matching only for new keys
        new_rows = []
        # Cache for version tokenization to avoid recomputing sets repeatedly
        _version_token_cache: dict[str, set] = {}
        _version_engine_cache: dict[str, Optional[str]] = {}
        for _, nk in new_keys.iterrows():
            brand = nk["brand_norm"]; v_norm = nk["version_norm"]; myear = nk.get("model_year", pd.NA)
            if not v_norm:
                new_rows.append({**nk, "model_year": myear, "fipe_model": None, "fipe_code": None, "score": 0.0, "match_source":"empty_version", "first_seen": today_iso, "last_seen": today_iso}); continue
            # ---- Primary candidate pool: same brand & same year ----
            cand_idx: List[int] = []
            match_source_tag = "brand_year"
            if pd.notna(myear):
                cand_idx = index_brand_year.get((brand, int(myear)), [])
                if not cand_idx and (brand, 32000) in index_brand_year_32000:
                    cand_idx = index_brand_year_32000[(brand, 32000)]
                    match_source_tag = "brand_year_32000"
            if not cand_idx:
                cand_idx = index_brand.get(brand, [])
                match_source_tag = "brand_only"
            cand = fipe.loc[cand_idx]
            # Precompute candidate token sets once (store on DataFrame if missing)
            if "_cand_toks" not in cand.columns:
                # Assign on underlying fipe slice via loc to persist for future keys
                fipe.loc[cand.index, "_cand_toks"] = cand["_model_norm"].str.split().map(set)
            # Version token/engine caches
            if v_norm not in _version_token_cache:
                _version_token_cache[v_norm] = _tokset(v_norm)
                _version_engine_cache[v_norm] = _extract_engine(v_norm)
            v_toks = _version_token_cache[v_norm]
            v_engine = _version_engine_cache[v_norm]
            if not len(cand_idx):
                logging.getLogger("match.simple").debug(
                    "No FIPE candidates after brand/year (brand=%s year=%s) -> unmatched", brand, myear)
                new_rows.append({**nk, "model_year": myear, "fipe_model": None, "fipe_code": None, "score": 0.0, "match_source": match_source_tag, "first_seen": today_iso, "last_seen": today_iso}); continue
            # Stage 1: fast token-based preliminary score (no SequenceMatcher yet)
            prelim_scores = []  # (score_prelim, idx)
            v_toks_len = len(v_toks) if v_toks else 0
            for c_idx in cand_idx:
                c_toks = fipe.at[c_idx, "_cand_toks"] if pd.notna(fipe.at[c_idx, "_cand_toks"]) else set()
                if v_toks_len:
                    inter = len(v_toks & c_toks)
                    coverage = inter / v_toks_len
                    precision = inter / len(c_toks) if c_toks else 0.0
                    f1 = (2*precision*coverage/(precision+coverage)) if (precision+coverage) else 0.0
                else:
                    coverage = precision = f1 = 0.0
                jacc = (len(v_toks & c_toks)/len(v_toks | c_toks)) if (v_toks or c_toks) else 0.0
                prelim = 0.55*f1 + 0.20*jacc  # omit SequenceMatcher for now
                prelim_scores.append((prelim, c_idx, f1, jacc))
            # Keep top K for expensive SequenceMatcher refinement
            K = 15 if len(prelim_scores) > 15 else len(prelim_scores)
            prelim_scores.sort(reverse=True, key=lambda x: x[0])
            top_candidates = prelim_scores[:K]
            s_best = -1.0; m_best = c_best = None
            for prelim, c_idx, f1, jacc in top_candidates:
                c_model_norm = fipe.at[c_idx, "_model_norm"]
                base = SequenceMatcher(None, v_norm, c_model_norm).ratio()
                score = prelim + 0.25*base  # add base component
                if v_engine:
                    c_engine = _extract_engine(c_model_norm)
                    if c_engine == v_engine: score += 0.05
                    elif c_engine is not None: score -= 0.10
                if score > s_best:
                    s_best = score; m_best = fipe.at[c_idx, "Modelo"]; c_best = fipe.at[c_idx, "CodigoFipe"]
            # determine fipe_brand for selected code
            fipe_brand = None
            if c_best is not None:
                # Slice candidate pool to find matching code
                pool = fipe.loc[cand_idx]
                hit = pool[pool["CodigoFipe"] == c_best]
                if not hit.empty:
                    fipe_brand = hit.iloc[0]["Marca"]
            new_rows.append({**nk,
                             "model_year": myear,
                             "fipe_brand": fipe_brand,
                             "fipe_model": m_best,
                             "fipe_code": c_best,
                             "score": round(float(max(s_best,0.0)),4),
                             "match_source": match_source_tag,
                             "first_seen": today_iso,
                             "last_seen": today_iso})

        # Append new rows to cache and save
        if new_rows:
            add_df = pd.DataFrame(new_rows)
            cache_df = load_version_match_table()
            # Ensure column order & presence
            add_df = add_df.reindex(columns=version_match_table_columns())
            if cache_df.empty:
                cache_df = add_df.copy()
            else:
                cache_df = pd.concat([cache_df, add_df], ignore_index=True, sort=False)
            # Update last_seen for existing keys present today
            seen_keys = set(tuple(x) for x in add_df[["brand_norm","model_norm","version_norm","model_year"]].itertuples(index=False, name=None))
            mask = cache_df[["brand_norm","model_norm","version_norm","model_year"]].apply(tuple, axis=1).isin(seen_keys)
            cache_df.loc[mask, "last_seen"] = today_iso
            save_version_match_table(cache_df)
            # Notify user about new versions (list a few examples)
            sample = add_df.head(10)[["brand_norm","model_norm","version_norm","model_year","fipe_brand","fipe_code","score"]]
            log.info("New version mappings added (%d). Sample:\n%s", len(add_df), sample.to_string(index=False))
    else:
        # Update last_seen for all present keys
        if not cache_df.empty:
            present_keys = set(tuple(x) for x in loc[key_cols].drop_duplicates().rename(columns={"_brand_norm":"brand_norm","_model_norm":"model_norm","_version_norm":"version_norm"}).itertuples(index=False, name=None))
            mask = cache_df[["brand_norm","model_norm","version_norm","model_year"]].apply(tuple, axis=1).isin(present_keys)
            if mask.any():
                cache_df.loc[mask, "last_seen"] = today_iso
                save_version_match_table(cache_df)

    # After ensuring cache filled for current versions, re-merge to populate columns
    cache_df = load_version_match_table()
    # Drop any stale columns before fresh merge (keep fipe_brand if present)
    loc = loc.drop(columns=[c for c in ["fipe_model","fipe_code","score","match_source","fipe_brand"] if c in loc.columns])
    loc = loc.merge(
        cache_df.rename(columns={"brand_norm":"_brand_norm","model_norm":"_model_norm","version_norm":"_version_norm"}),
        on=["_brand_norm","_model_norm","_version_norm","model_year"],
        how="left"
    )
    loc.rename(columns={"score":"match_score"}, inplace=True)

    out_csv = Path(out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    loc.to_csv(out_csv, index=False, sep=";")
    log.info("wrote %s rows -> %s (simple matcher)", len(loc), out_csv)

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

    audit = audit.sort_values(["total_count","fipe_code","model_year"], ascending=[False, True, True])

    if out_path is None:
        out_path = TUPLES_DIR / f"fipe_tuples_{ymd_compact()}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    audit.to_csv(out_path, index=False, sep=";")
    log.info("wrote %s rows -> %s", len(audit), out_path)
    return out_path

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
        cached = self.cache.get(endpoint, data)
        if cached is not None: return cached

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
                    candidates: List[Optional[str]]
                    if forced:
                        # try forced fuel first, then remaining excluding duplicate
                        candidates = [forced] + [c for c in FUEL_CODE_CANDIDATES if c != forced]
                    else:
                        # Start with bare year (no fuel) then candidate list
                        candidates = [None] + FUEL_CODE_CANDIDATES
                    js = {}
                    chosen_fuel: Optional[str] = None
                    for fuel_code in candidates:
                        payload = {
                            "codigoTabelaReferencia": str(item["cod_tab"]),
                            "codigoTipoVeiculo": str(item["tipo"]),
                            "tipoVeiculo": TIPO_STR.get(str(item["tipo"]), "carro"),
                            "modeloCodigoExterno": str(item["fipe_code"]),
                            "tipoConsulta": "codigo",
                        }
                        if fuel_code is None:
                            payload["anoModelo"] = str(item["ano_param"])  # original behavior
                        else:
                            payload["anoModelo"] = f"{item['ano_param']}-{fuel_code}"
                            payload["codigoTipoCombustivel"] = fuel_code
                        try:
                            js = await client._post_json(ENDPOINTS["valor_todos_params"], payload) or {}
                        except Exception:
                            js = {}
                        if js.get("Valor") and js.get("CodigoFipe"):
                            chosen_fuel = fuel_code
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
    for code, yr in sorted(code_years):
        tasks.append({
            "cod_tab": cod_tab,
            "mes_ref": mes_ref,
            "tipo": str(tipo),
            "fipe_code": code,
            "ano_param": str(int(yr)),
        })
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

        tuples: Optional[Set[Tuple[str,int]]] = getattr(args, "tuples", None)
        if not tuples:
            raise SystemExit(
                "fipe-dump is tuples-only in this project. Use `run-all` (it builds the (fipe_code, model_year) set) "
                "or wire your own tuples before calling fipe-dump."
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
    sl.add_argument("--concurrency", type=int, default=10)

    # scrape-movida
    sm = sub.add_parser("scrape-movida", help="Scrape Movida seminovos API to JSON (+meta)")
    sm.add_argument("--out", default="movida_offers.json")
    sm.add_argument("--delay", type=float, default=0.35)
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
    mtc.add_argument("--strategy", choices=["contains","legacy"], default="contains", help="Matching strategy: contains (brand/year + model containment) or legacy (previous fuzzy matcher)")

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

    # clean
    cln = sub.add_parser("clean", help="Keep only last K dated CSVs per vendor")
    cln.add_argument("--folder", default="raw")
    cln.add_argument("--keep", type=int, default=3)

    # run-all
    allp = sub.add_parser("run-all", help="Scrape → Parse → Match → FIPE (tuples-only)")
    allp.add_argument("--concurrency", type=int, default=10, help="Localiza scraper concurrency")
    allp.add_argument("--verbose", action="store_true", help="Alias for --log-level DEBUG")
    allp.add_argument("--movida-delay", type=float, default=0.35, help="Movida delay between page requests (s)")
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
        match_localiza(lcz_path, fipe_path, out_path, args.threshold, movida_csv_fallback=mv_fallback, fipe_models_csv=fm_path, strategy=args.strategy)
    elif args.cmd == "fipe-list":
        asyncio.run(fipe_list(args))
    elif args.cmd == "fipe-dump":
        asyncio.run(fipe_dump(args))
    elif args.cmd == "clean":
        base = Path(args.folder)
        clean_keep_last_k(base / "movida",   "movida_seminovos", args.keep)
        clean_keep_last_k(base / "localiza", "localiza_seminovos", args.keep)
    elif args.cmd == "run-all":
        run_all(args)

if __name__ == "__main__":
    main()
