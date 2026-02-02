import sys
from pathlib import Path
import pandas as pd

def main():
    if len(sys.argv) != 3:
        print("Usage: merge_parts.py <artifacts_dir> <out_csv>")
        raise SystemExit(1)

    artifacts_dir = Path(sys.argv[1])
    out_csv = Path(sys.argv[2])

    parts = sorted(artifacts_dir.rglob("part-*.csv"))
    if not parts:
        raise SystemExit(f"No part CSVs found under {artifacts_dir}")

    df = pd.concat((pd.read_csv(p) for p in parts), ignore_index=True)

    # If you have stable ID columns, dedupe here:
    # Example keys (adjust to your columns):
    # keys = ["VehicleType", "BrandCode", "ModelCode", "YearCode", "TableCode"]
    # df = df.drop_duplicates(subset=keys, keep="last")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"Merged {len(parts)} parts -> {out_csv} ({len(df):,} rows)")

if __name__ == "__main__":
    main()
