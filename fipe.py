"""
FIPE API Data Fetcher
=====================
Monolithic script to fetch all car prices from the FIPE table API.
This script recursively fetches brands, models, years, and prices for all vehicles.
"""

import argparse
import hashlib
import os
import random
from typing import Any, Dict, Iterable, Optional, Set, List
import requests
import pandas as pd
import time
import json
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# API Configuration
FIPE_BASE = "https://veiculos.fipe.org.br"
ENDPOINTS = {
    "tabelas": "/api/veiculos/ConsultarTabelaDeReferencia",
    "marcas": "/api/veiculos/ConsultarMarcas",
    "modelos": "/api/veiculos/ConsultarModelos",
    "modelos_por_ano": "/api/veiculos/ConsultarModelosAtravesDoAno",
    "anos_modelo": "/api/veiculos/ConsultarAnoModelo",
    "valor_todos_params": "/api/veiculos/ConsultarValorComTodosParametros",
}

# Request configuration
HEADERS = {
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# Rate limiting
REQUEST_DELAY = 1.0  # Delay between requests in seconds
MAX_RETRIES = 5
RETRY_DELAY_BASE = 2  # Base delay for exponential backoff


def get_retry_delay(attempt: int, base: float = RETRY_DELAY_BASE, max_delay: float = 60.0) -> float:
    """
    Calculate retry delay with exponential backoff and jitter.
    
    Args:
        attempt: Current attempt number (0-indexed)
        base: Base delay in seconds
        max_delay: Maximum delay cap in seconds
        
    Returns:
        Delay in seconds with exponential backoff and jitter
    """
    # Exponential backoff: base * 2^attempt
    exponential_delay = base * (2 ** attempt)
    # Cap the delay
    capped_delay = min(exponential_delay, max_delay)
    # Add jitter: random value between 0 and capped_delay
    jitter = random.uniform(0, capped_delay * 0.5)
    return capped_delay + jitter


# Vehicle types
VEHICLE_TYPES = {
    1: 'carros',      # Cars
    2: 'motos',       # Motorcycles
    3: 'caminhoes'    # Trucks
}


class FipeAPIClient:
    """Client for interacting with the FIPE API."""
    
    def __init__(self, vehicle_type: int = 1):
        """
        Initialize the FIPE API client.
        
        Args:
            vehicle_type: Type of vehicle (1=cars, 2=motorcycles, 3=trucks)
        """
        self.vehicle_type = vehicle_type
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.base_payload = {"codigoTabelaReferencia": None}
        
    def _make_request(self, endpoint: str, payload: Dict[str, Any], 
                     retries: int = MAX_RETRIES) -> Optional[Any]:
        """
        Make a POST request to the FIPE API with retry logic.
        
        Args:
            endpoint: API endpoint key from ENDPOINTS dict
            payload: Request payload
            retries: Number of retry attempts
            
        Returns:
            JSON response or None if all retries fail
        """
        url = FIPE_BASE + ENDPOINTS[endpoint]
        
        for attempt in range(retries):
            try:
                time.sleep(REQUEST_DELAY)
                response = self.session.post(url, json=payload, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    delay = get_retry_delay(attempt)
                    logger.info(f"Retrying in {delay:.2f} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"All retries failed for endpoint {endpoint}")
                    return None
        return None
    
    def get_reference_table(self) -> Optional[Dict[str, Any]]:
        """
        Get the current reference table (month/year).
        
        Returns:
            Most recent reference table entry
        """
        payload = {}
        result = self._make_request("tabelas", payload)
        if result and len(result) > 0:
            return result[0]  # Return most recent table
        return None
    
    def get_brands(self, table_code: int) -> List[Dict[str, Any]]:
        """
        Get all brands for the specified vehicle type.
        
        Args:
            table_code: Reference table code
            
        Returns:
            List of brands
        """
        payload = {
            "codigoTabelaReferencia": table_code,
            "codigoTipoVeiculo": self.vehicle_type
        }
        result = self._make_request("marcas", payload)
        return result if result else []
    
    def get_models(self, table_code: int, brand_code: str) -> List[Dict[str, Any]]:
        """
        Get all models for a specific brand.
        
        Args:
            table_code: Reference table code
            brand_code: Brand code
            
        Returns:
            List of models
        """
        payload = {
            "codigoTabelaReferencia": table_code,
            "codigoTipoVeiculo": self.vehicle_type,
            "codigoMarca": brand_code
        }
        result = self._make_request("modelos", payload)
        if result and isinstance(result, dict) and 'Modelos' in result:
            return result['Modelos']
        return []
    
    def get_years(self, table_code: int, brand_code: str, 
                 model_code: str) -> List[Dict[str, Any]]:
        """
        Get all available years for a specific model.
        
        Args:
            table_code: Reference table code
            brand_code: Brand code
            model_code: Model code
            
        Returns:
            List of year variants
        """
        payload = {
            "codigoTabelaReferencia": table_code,
            "codigoTipoVeiculo": self.vehicle_type,
            "codigoMarca": brand_code,
            "codigoModelo": model_code
        }
        result = self._make_request("anos_modelo", payload)
        return result if result else []
    
    def get_price(self, table_code: int, brand_code: str, 
                 model_code: str, year_code: str) -> Optional[Dict[str, Any]]:
        """
        Get the price for a specific vehicle configuration.
        
        Args:
            table_code: Reference table code
            brand_code: Brand code
            model_code: Model code
            year_code: Year code (format: "YYYY-F" where F is fuel type)
            
        Returns:
            Price information dict
        """
        # Split year_code into year and fuel type
        year_parts = year_code.split('-')
        year = year_parts[0] if len(year_parts) > 0 else year_code
        fuel_type = year_parts[1] if len(year_parts) > 1 else "1"
        
        payload = {
            "codigoTabelaReferencia": table_code,
            "codigoTipoVeiculo": self.vehicle_type,
            "codigoMarca": brand_code,
            "codigoModelo": model_code,
            "anoModelo": int(year),
            "codigoTipoCombustivel": int(fuel_type),
            "tipoConsulta": "tradicional"
        }
        return self._make_request("valor_todos_params", payload)


def _stable_mod_hash(s: str, mod: int) -> int:
    """Stable shard assignment."""
    h = hashlib.md5(s.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % mod


class FipeDataFetcher:
    """Main class to orchestrate fetching all FIPE data."""

    def __init__(
        self,
        vehicle_type: int = 1,
        output_dir: str = "data/fipe/full",
        shard_id: int = 0,
        shard_count: int = 1,
        request_delay: Optional[float] = None,
    ):
        self.client = FipeAPIClient(vehicle_type)
        self.vehicle_type = vehicle_type
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.shard_id = shard_id
        self.shard_count = shard_count

        # Optional override of delay without editing globals.
        if request_delay is not None:
            global REQUEST_DELAY
            REQUEST_DELAY = float(request_delay)

        # One file per shard, append-only (JSONL).
        self.out_jsonl = self.output_dir / f"fipe_part_v{vehicle_type}_s{shard_id}of{shard_count}.jsonl"
        self.done_keys_path = self.output_dir / f"fipe_done_v{vehicle_type}_s{shard_id}of{shard_count}.txt"
        self.failed_keys_path = self.output_dir / f"fipe_failed_v{vehicle_type}_s{shard_id}of{shard_count}.txt"

        self._done: Set[str] = self._load_done_keys()
        self._failed: Set[str] = set()  # Track failures in current run

    def _load_done_keys(self) -> Set[str]:
        done: Set[str] = set()
        if self.done_keys_path.exists():
            done.update(x.strip() for x in self.done_keys_path.read_text(encoding="utf-8").splitlines() if x.strip())
        return done

    def _mark_done(self, key: str) -> None:
        if key in self._done:
            return
        self._done.add(key)
        with open(self.done_keys_path, "a", encoding="utf-8") as f:
            f.write(key + "\n")

    def _mark_failed(self, key: str, reason: str) -> None:
        """Track items that failed to fetch for later retry."""
        if key in self._failed:
            return
        self._failed.add(key)
        logger.warning(f"Failed to fetch {key}: {reason}")
        with open(self.failed_keys_path, "a", encoding="utf-8") as f:
            f.write(f"{key}|{reason}|{datetime.now().isoformat()}\n")

    def _load_failed_keys(self) -> List[Dict[str, str]]:
        """Load failed keys from file for retry."""
        failed_items = []
        if self.failed_keys_path.exists():
            for line in self.failed_keys_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                parts = line.strip().split("|")
                if len(parts) >= 5:  # key format: table|vehicle|brand|model|year
                    key = "|".join(parts[:5])
                    reason = parts[5] if len(parts) > 5 else "unknown"
                    failed_items.append({"key": key, "reason": reason})
        return failed_items

    def _clear_failed_key(self, key: str) -> None:
        """Remove a key from the failed list after successful retry."""
        if key in self._failed:
            self._failed.discard(key)

    def _rewrite_failed_file(self, remaining_failures: Set[str]) -> None:
        """Rewrite the failed file with only remaining failures."""
        if not remaining_failures:
            # All retries succeeded, remove the file
            if self.failed_keys_path.exists():
                self.failed_keys_path.unlink()
            return
        
        # Read existing entries to preserve reasons
        existing_entries = {}
        if self.failed_keys_path.exists():
            for line in self.failed_keys_path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    parts = line.strip().split("|")
                    if len(parts) >= 5:
                        key = "|".join(parts[:5])
                        existing_entries[key] = line.strip()
        
        # Write only remaining failures
        with open(self.failed_keys_path, "w", encoding="utf-8") as f:
            for key in remaining_failures:
                if key in existing_entries:
                    f.write(existing_entries[key] + "\n")
                else:
                    f.write(f"{key}|unknown|{datetime.now().isoformat()}\n")

    def _append_jsonl(self, obj: Dict[str, Any]) -> None:
        with open(self.out_jsonl, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    def _owns_model(self, table_code: int, brand_code: str, model_code: str) -> bool:
        # Balance across shards using a stable hash of (table, vehicle, brand, model)
        key = f"{table_code}|{self.vehicle_type}|{brand_code}|{model_code}"
        return _stable_mod_hash(key, self.shard_count) == self.shard_id

    def fetch_all_data_streaming(self) -> None:
        """
        Fetch FIPE data for the shard and write each record to JSONL immediately.
        """
        logger.info(
            f"Starting FIPE data fetch (vehicle_type={self.vehicle_type}, shard={self.shard_id}/{self.shard_count})"
        )

        table = self.client.get_reference_table()
        if not table:
            logger.error("Failed to get reference table")
            return

        table_code = table["Codigo"]
        table_month = table["Mes"]
        logger.info(f"Using reference table: {table_month} (Code: {table_code})")

        brands = self.client.get_brands(table_code)
        logger.info(f"Found {len(brands)} brands")

        # Note: we still iterate brands, but we skip most models quickly,
        # and we do NOT fetch years/prices for models not owned by this shard.
        for brand in tqdm(brands, desc=f"Shard {self.shard_id}: brands"):
            brand_code = brand["Value"]
            brand_name = brand["Label"]

            models = self.client.get_models(table_code, brand_code)

            for model in tqdm(models, desc=f"  {brand_name}: models", leave=False):
                model_code = str(model["Value"])
                model_name = model["Label"]

                if not self._owns_model(table_code, brand_code, model_code):
                    continue

                years = self.client.get_years(table_code, brand_code, model_code)
                for year in years:
                    year_code = year["Value"]
                    year_label = year["Label"]

                    # Unique key for resumability (per (table, vehicle, brand, model, year_code))
                    item_key = f"{table_code}|{self.vehicle_type}|{brand_code}|{model_code}|{year_code}"
                    if item_key in self._done:
                        continue

                    price_data = self.client.get_price(table_code, brand_code, model_code, year_code)
                    if not price_data:
                        self._mark_failed(item_key, "no_response_after_retries")
                        continue

                    price_data["BrandCode"] = brand_code
                    price_data["BrandName"] = brand_name
                    price_data["ModelCode"] = model_code
                    price_data["ModelName"] = model_name
                    price_data["YearCode"] = year_code
                    price_data["YearLabel"] = year_label
                    price_data["TableCode"] = table_code
                    price_data["TableMonth"] = table_month
                    price_data["VehicleType"] = self.vehicle_type
                    price_data["VehicleTypeName"] = VEHICLE_TYPES.get(self.vehicle_type, "unknown")
                    price_data["FetchDate"] = datetime.now().isoformat()

                    self._append_jsonl(price_data)
                    self._mark_done(item_key)

        logger.info(f"Shard complete. Output: {self.out_jsonl}")
        
        # Report failures summary
        if self._failed:
            logger.warning(f"⚠️  {len(self._failed)} items failed. See: {self.failed_keys_path}")
        else:
            logger.info("✓ All items fetched successfully.")

    def retry_failed_items(self) -> int:
        """
        Retry fetching only the items that previously failed.
        Updates the main output file and clears successful retries from failed list.
        
        Returns:
            Number of successfully recovered items
        """
        failed_items = self._load_failed_keys()
        if not failed_items:
            logger.info("No failed items to retry.")
            return 0

        logger.info(f"Retrying {len(failed_items)} failed items...")

        # Get current reference table
        table = self.client.get_reference_table()
        if not table:
            logger.error("Failed to get reference table")
            return 0

        table_code = table["Codigo"]
        table_month = table["Mes"]

        recovered = 0
        still_failed: Set[str] = set()

        for item in tqdm(failed_items, desc="Retrying failed items"):
            key = item["key"]
            parts = key.split("|")
            if len(parts) < 5:
                continue

            orig_table_code, vehicle_type, brand_code, model_code, year_code = parts[:5]

            # Check if already recovered in a previous retry
            if key in self._done:
                continue

            price_data = self.client.get_price(table_code, brand_code, model_code, year_code)
            if not price_data:
                still_failed.add(key)
                continue

            # Get brand and model names (we need to fetch them)
            brand_name = price_data.get("Marca", "Unknown")
            model_name = price_data.get("Modelo", "Unknown")
            year_label = price_data.get("AnoModelo", year_code)

            price_data["BrandCode"] = brand_code
            price_data["BrandName"] = brand_name
            price_data["ModelCode"] = model_code
            price_data["ModelName"] = model_name
            price_data["YearCode"] = year_code
            price_data["YearLabel"] = str(year_label)
            price_data["TableCode"] = table_code
            price_data["TableMonth"] = table_month
            price_data["VehicleType"] = int(vehicle_type)
            price_data["VehicleTypeName"] = VEHICLE_TYPES.get(int(vehicle_type), "unknown")
            price_data["FetchDate"] = datetime.now().isoformat()
            price_data["RetryRecovered"] = True  # Mark as recovered from retry

            self._append_jsonl(price_data)
            self._mark_done(key)
            recovered += 1

        # Update the failed file
        self._rewrite_failed_file(still_failed)

        if recovered > 0:
            logger.info(f"✓ Recovered {recovered} items successfully.")
        if still_failed:
            logger.warning(f"⚠️  {len(still_failed)} items still failed. See: {self.failed_keys_path}")

        return recovered

    def jsonl_to_parquet_or_csv(self, out_path: Optional[str] = None) -> Optional[Path]:
        """Convert shard JSONL to parquet (preferred) or csv."""
        if not self.out_jsonl.exists():
            logger.warning("No JSONL output found to convert.")
            return None

        df = pd.read_json(self.out_jsonl, lines=True)

        if out_path is None:
            out_path = str(self.out_jsonl).replace(".jsonl", ".parquet")

        outp = Path(out_path)
        if outp.suffix.lower() == ".csv":
            df.to_csv(outp, index=False, encoding="utf-8-sig")
        else:
            # parquet by default
            df.to_parquet(outp, index=False)

        logger.info(f"Converted shard output to: {outp}")
        return outp


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--vehicle-type", type=int, default=1, choices=[1, 2, 3])
    parser.add_argument("--output-dir", type=str, default="data/fipe/full")
    parser.add_argument("--shard-id", type=int, default=int(os.getenv("SHARD_ID", "0")))
    parser.add_argument("--shard-count", type=int, default=int(os.getenv("SHARD_COUNT", "1")))
    parser.add_argument("--request-delay", type=float, default=None, help="Override REQUEST_DELAY seconds")

    # New: CSV output path (default computed)
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output CSV path. If omitted: "
             "single shard -> data/fipe/full/fipe_models.csv; "
             "multi-shard -> data/fipe/full/parts/part-<shard>.csv"
    )

    # Optional: keep the intermediate JSONL (useful for debugging)
    parser.add_argument("--keep-jsonl", action="store_true", help="Do not delete intermediate JSONL")
    
    # Retry mode: only retry previously failed items
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Only retry items that previously failed (reads from failed keys file)"
    )

    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Default output naming
    if args.output is None:
        if args.shard_count == 1:
            out_csv = output_dir / "fipe_models.csv"
        else:
            parts_dir = output_dir / "parts"
            parts_dir.mkdir(parents=True, exist_ok=True)
            out_csv = parts_dir / f"part-{args.shard_id}.csv"
    else:
        out_csv = Path(args.output)
        out_csv.parent.mkdir(parents=True, exist_ok=True)

    fetcher = FipeDataFetcher(
        vehicle_type=args.vehicle_type,
        output_dir=str(output_dir),
        shard_id=args.shard_id,
        shard_count=args.shard_count,
        request_delay=args.request_delay,
    )

    # Either retry failed items or run full fetch
    if args.retry_failed:
        recovered = fetcher.retry_failed_items()
        if recovered > 0:
            # Re-export CSV with recovered items
            fetcher.jsonl_to_parquet_or_csv(str(out_csv))
            logger.info(f"Updated output with {recovered} recovered items: {out_csv}")
    else:
        # Runs the shard and writes JSONL incrementally (safe/resumable)
        fetcher.fetch_all_data_streaming()
        # Always produce CSV (default behavior)
        fetcher.jsonl_to_parquet_or_csv(str(out_csv))

    # Optional cleanup
    if not args.keep_jsonl:
        try:
            fetcher.out_jsonl.unlink(missing_ok=True)
        except Exception:
            pass


if __name__ == "__main__":
    main()
