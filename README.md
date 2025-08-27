# Seminovos Fleet Data Pipeline

Unified toolkit to:

1. Scrape daily retail used-car (seminovos) listings from Localiza and Movida.
2. Parse raw JSON offers into dated, normalized CSV snapshots (one file per vendor per day) under `raw/`.
3. Fuzzy‑match Localiza vehicle versions against official FIPE model catalog (via `fipe_models.csv`) producing enriched match CSVs in `data/`.
4. Consolidate all (fipe_code, model_year) tuples seen in Localiza + Movida data and audit their frequency.
5. Query official FIPE API for price/metadata ONLY for the discovered tuples ("tuples‑only" dump) and store results in `data/fipe/`.
6. Repeat end‑to‑end with a single command (`run-all`).

All core logic lives in `fleet.py` (≈2k lines) – other older scripts are kept for reference/backups.

---
## Repository Layout
```
raw/                      # Parsed vendor daily CSV snapshots (source of truth) 
  localiza/localiza_seminovos_YYYYMMDD.csv
  movida/movida_seminovos_YYYYMMDD.csv
.data/fipe/               # FIPE dumps & model catalog (input: fipe_models.csv)
data/match/               # Matching caches (auditable)
localiza_offers.json(.meta.json)  # Last scraped Localiza raw JSON + metadata
movida_offers.json(.meta.json)    # Last scraped Movida raw JSON + metadata
fleet.py                  # Unified CLI & pipeline
fipe_models.csv           # (Provide) FIPE model catalog with Marca/Modelo/CodigoFipe/AnoModelo
```

---
## Quick Start

### 1. Install dependencies
Python 3.11+ recommended.

```
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt  # (create this file – sample below)
```
If you don't yet have `requirements.txt`, create it (minimal):
```
aiohttp
pandas
numpy
beautifulsoup4
requests
tqdm
```

### 2. Provide `fipe_models.csv`
Place the FIPE model catalog at either:
- `data/fipe/fipe_models.csv` (preferred), or
- repository root (`fipe_models.csv`).

Must include columns (case/sep flexible, auto‑detected):
`Marca`, `Modelo`, `CodigoFipe`, `AnoModelo` (others ignored). Only passenger cars (TipoVeiculo==1) are needed for default usage.

### 3. Run full daily pipeline
```
python fleet.py run-all --threshold 0.62 --since auto
```
Outputs (example for 2025‑08‑25):
- `raw/localiza/localiza_seminovos_20250825.csv`
- `raw/movida/movida_seminovos_20250825.csv`
- `data/localiza_with_fipe_match_20250825.csv`
- `data/fipe/fipe_tuples_20250825.csv` (audit counts)
- `data/fipe/fipe_dump_20250825.csv` (tuples‑only FIPE pricing)

Re‑run the same day: existing raw CSVs & match file are reused unless you force.

### Common flags
- `--force-scrape` re‑scrapes vendors even if today's CSV present.
- `--force-match` recomputes matching even if today's match file exists.
- `--since YYYY-MM` restricts FIPE reference months (tables) considered. `auto` == latest only.
- `--tipos 1 2` include multiple FIPE vehicle types (1=carros, 2=motos, 3=caminhoes).
- `--max-concurrency` & `--rps` tune FIPE API request rate (polite defaults: 8, 2.0).
- `--resume` appends new FIPE prices if re‑running the same dump file (idempotent by row key).

---
## Individual Commands

Each sub-command can be executed individually. Typical invocations and prerequisites:

```bash
# 1. Scrape raw offers (internet required)
python fleet.py scrape-localiza --out localiza_offers.json --concurrency 10
python fleet.py scrape-movida   --out movida_offers.json --delay 0.35

# 2. Parse JSON into dated CSV snapshots
python fleet.py parse-localiza --in localiza_offers.json --out-dir raw/localiza
python fleet.py parse-movida   --in movida_offers.json   --out-dir raw/movida

# 3. Match Localiza rows to FIPE codes (requires fipe_models.csv)
python fleet.py match-localiza --localiza-csv raw/localiza/localiza_seminovos_YYYYMMDD.csv \
    --fipe-models-csv data/fipe/fipe_models.csv --threshold 0.62

# 4. Inspect FIPE reference months or dump prices
python fleet.py fipe-list
python fleet.py fipe-dump --out data/fipe/fipe_dump_YYYYMMDD.csv --since 2025-07

# 5. Build normalized output tables (requires previous steps)
python fleet.py build-tables --localiza-csv data/localiza/localiza_with_fipe_match_YYYYMMDD.csv \
    --movida-csv raw/movida/movida_seminovos_YYYYMMDD.csv \
    --fipe-csv data/fipe/fipe_dump_YYYYMMDD.csv --out-dir data/tables

# 6. Housekeeping
python fleet.py clean --folder raw --keep 3

# 7. Full pipeline
python fleet.py run-all --threshold 0.62 --since auto
```

### Matching
`match-localiza` applies a cached brand/year + model containment strategy. Versions
seen in previous runs are loaded from `data/localiza_version_match.csv`, and only
new combinations are scored and appended to the cache (tracking `first_seen` and
`last_seen`).

`match_score` >= `--threshold` sets `match_accepted=1`.

### FIPE tuples mode
The project intentionally queries ONLY the (CodigoFipe, AnoModelo) pairs actually observed in Localiza/Movida data (reduces traffic & time). `fipe-dump` requires a prepared tuple set; `run-all` builds it automatically.

---
## Data Columns (Key Outputs)

### raw vendor CSVs (`raw/localiza/*`, `raw/movida/*`)
Common standardized subset:
`snapshot_date;type;brand;model;version_raw;manufacture_year;model_year;price;version` (+ vendor specific extras may be dropped after normalization).

### Match CSV (`data/localiza_with_fipe_match_YYYYMMDD.csv`)
Extends Localiza rows with:
`fipe_code;fipe_model;match_score;match_accepted` plus internal normalization columns (`_brand_norm`, etc.) used during matching.

### FIPE dump (`data/fipe/fipe_dump_YYYYMMDD.csv`)
Raw rows returned directly from FIPE API with added numeric `ValorNum` (float) and reference month metadata.

### Tuples audit (`data/fipe/fipe_tuples_YYYYMMDD.csv`)
For each (fipe_code, model_year): counts of presence across Localiza / Movida snapshots.

### Output tables (`data/tables/*.csv`)
Final normalized datasets joining vendor offers with FIPE prices.

- `localiza_table_YYYYMMDD.csv` and `movida_table_YYYYMMDD.csv` contain:
  `snapshot_date,snapshot_year,snapshot_month,type,brand,model,fipe_version,fipe_code,manufacture_year,model_year,offer_price,fipe_price,premium_vs_fipe_price,fipe_code_model_year,model_model_year`.
- `fipe_table_YYYYMMDD.csv` contains:
  `reference_year,reference_month,brand,model,fipe_version,fipe_code,model_year,fipe_price,m_m_price_change`.

---
## Matching Heuristics (Contains Strategy)
1. Normalize brand/model/version (accent stripping, casing, canonical replacements).
2. Filter FIPE candidates by brand and identical model year.
3. Keep candidates whose normalized model text contains (substring or token superset) the Localiza model.
4. Score candidates vs full Localiza version using weighted combination of SequenceMatcher ratio + token F1 + Jaccard.
5. Keep best scoring candidate; accept if score >= threshold.


---
## Rate Limiting & Caching
- FIPE API calls are throttled by requests/second plus exponential backoff on 429 / 5xx.
- All POST payloads are cached under `.fipe_cache/` (hash keyed) to minimize duplicate hits across runs.
- Resume mode prevents duplicate FIPE rows by composite key (MesReferencia, CodigoFipe, AnoModelo, SiglaCombustivel).

---
## Cleaning
```
python fleet.py clean --folder raw --keep 5
```
Retains only last K dated CSVs for each vendor subfolder.

---
## Minimal requirements.txt (example)
```
aiohttp>=3.9
pandas>=2.0
numpy>=1.24
beautifulsoup4>=4.12
requests>=2.31
tqdm>=4.66
```
Optional extras: `python-dotenv`, `pyarrow` (faster Parquet exports you may add).

---
## Development Notes
- All temporary & output paths auto‑created.
- Timezone localized to America/Sao_Paulo when deriving snapshot dates from metadata.
- Matching caches (`data/match/`) are safe to delete; they'll be rebuilt incrementally.
- To experiment with new normalization rules, adjust helpers in `fleet.py` (search for `norm_text` and `generic_norm_text`).

---
## Typical Daily Cron (Windows PowerShell)
```
$env:PYTHONIOENCODING="utf-8"; python fleet.py run-all --threshold 0.62 --since auto >> logs\run-all-$(Get-Date -Format yyyyMMdd).log 2>&1
```
Ensure the venv is activated or use its full python path.

---
## Troubleshooting
- Empty vendor CSV: site layout/API likely changed – inspect raw JSON (`*_offers.json`).
- Low match rate: lower `--threshold`, inspect unmatched rows (fipe_code empty) to refine normalization.
- FIPE 429 responses: reduce `--max-concurrency` or `--rps`; cache will help subsequent runs.
- Tuples empty: ensure both vendor parse steps produced rows before matching.

---
## License
Internal / proprietary dataset workflow. Add a license file if distributing.

---
## Disclaimer
This code interacts with public web endpoints. Respect each provider's Terms of Use and robots directives.
