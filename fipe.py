import argparse
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Set

import pandas as pd
from tqdm import tqdm

# ... keep your existing imports, constants, FipeAPIClient, VEHICLE_TYPES, etc.


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

        self._done: Set[str] = self._load_done_keys()

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
