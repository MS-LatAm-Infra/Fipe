# Seminovos Pipeline – Technical Details

This document complements the top-level `README.md`, focusing on internal architecture, data contracts and extension points.

## 1. High-Level Flow
```
(scrape localiza)  -> localiza_offers.json -> parse-localiza -> raw/localiza/localiza_seminovos_YYYYMMDD.csv
(scrape movida)    -> movida_offers.json   -> parse-movida   -> raw/movida/movida_seminovos_YYYYMMDD.csv
(localiza + FIPE models) -> match-localiza -> data/localiza_with_fipe_match_YYYYMMDD.csv
(Localiza+Movida) -> (fipe_code, model_year) tuple audit -> data/fipe/fipe_tuples_YYYYMMDD.csv
(tuples + FIPE tables) -> fipe-dump -> data/fipe/fipe_dump_YYYYMMDD.csv
```
`run-all` orchestrates the above with idempotent daily guards.

## 2. Key Normalization Functions (fleet.py)
- `norm_text`: aggressive normalization for version strings (removes accents, canonicalizes abbreviations, deduplicates tokens, handles model-specific patterns like Onix Premier, FIPE edge cases, engine token extraction).
- `generic_norm_text`: lighter form used for brand/model tokens (avoid over-normalizing core model identifiers).
- `norm_brand`: brand alias mapping (e.g., `gm` -> `chevrolet`, `vw volkswagen` -> `volkswagen`). Extend here when brand variants appear.

## 3. Matching Strategies
### Contains (default)
Simplified, fast approach:
1. Normalize brand & model.
2. Filter FIPE rows by identical brand + year.
3. Keep FIPE models that "contain" the Localiza model (substring or token superset test).
4. Score candidate using weighted blend: 0.5*SequenceMatcher + 0.3*token F1 + 0.2*Jaccard.
5. Accept if score >= threshold.
6. Persist to `localiza_version_match.csv` with provenance (`match_source=contains`).

### Legacy
Legacy multi-stage search (retained for parity / regression diffing). More complex token heuristics plus fallback brand-only matching and engine bias scoring.

## 4. Version Match Cache (`data/match/localiza_version_match.csv`)
Columns:
`brand_norm, model_norm, version_norm, model_year, fipe_brand, fipe_model, fipe_code, score, match_source, first_seen, last_seen`.
- Primary key: (brand_norm, model_norm, version_norm, model_year)
- `first_seen` remains earliest detection; `last_seen` updated each day the key appears.
- Deleting the file resets historical context (fresh matching on next run).

## 5. FIPE Tuples Logic
- `collect_fipe_tuples` inspects Localiza match output (prefers `fipe_year` else `model_year`) plus Movida CSV.
- `tuples_audit` produces a frequency table: counts where each tuple appears.
- Only these tuples are expanded into full FIPE price rows, massively reducing API volume.

## 6. FIPE Dump Mechanics
- Rate limiting: adaptive exponential backoff per logical endpoint family tracked by `pow2_level` (per endpoint) with cap.
- Fuel code probing: Some FIPE codes require a particular combustivel; the logic tries (forced) -> other candidates, starting with Flex (6).
- Row uniqueness guard when `--resume` is active via `(MesReferencia, CodigoFipe, AnoModelo, SiglaCombustivel)`.
- Disk cache: request payload hashing (SHA1 of endpoint + canonical JSON) under `.fipe_cache/<endpoint>/<hash>.json`.

## 7. Snapshot Dating
A vendor's JSON meta file stores UTC fetch timestamp (`fetched_at_utc`). Snapshot date = that timestamp localized to `America/Sao_Paulo`. If missing/unparseable, fallback to local system date (`date.today()`).

## 8. Extending / Customization
| Area | How |
|------|-----|
| Add vendor | Implement `scrape_<vendor>` + `parse_<vendor>` producing same canonical columns; integrate into `run_all` and tuple collection. |
| New normalization rule | Modify `norm_text` (version-level) or `generic_norm_text` (for model) – keep changes idempotent and test match deltas. |
| Brand alias | Add to `norm_brand` mapping. |
| Engine detection pattern | Adjust `extract_engine` regex. |
| Alternative scoring | Change weights in contains strategy (search for `score = 0.5 * seq`). |
| Data export format | After each to_csv call, optionally add a Parquet export (`df.to_parquet(..., engine="pyarrow")`) behind a flag. |

## 9. Error / Edge Handling
- Network retries: Movida uses `requests` Retry adapter; Localiza uses manual async tasks with per-page retry logging.
- FIPE 429 / 5xx: exponential backoff + delayed next attempt; per-endpoint penalty scaled by power-of-two growth.
- Empty candidate pools: rows left unmatched (blank `fipe_code`) but retained for transparency.
- Duplicate offers: filtered by offer id before DataFrame creation.

## 10. Performance Notes
- Localiza scraper concurrency configurable; default 10 with tiny random jitter to reduce burstiness.
- Matching caches avoid recomputing for previously seen versions (daily incremental cost << full recompute).
- FIPE tuples drastically shrink remote calls—only observed pairs cause requests.

## 11. Tests (Suggested Future Work)
Add lightweight regression tests: 
- Normalize sample versions to ensure stability.
- Confirm contains strategy picks expected FIPE codes for a curated golden set.
- Validate tuple audit aggregator counts.

## 12. Logging Conventions
`logger name` prefixes:
- `scrape.localiza`, `scrape.movida`
- `parse`, `match`, `match.simple`, `match.cache`, `match.table`
- `fipe.dump`, `fipe.list`
- `tuples.audit`
- `run-all`

Adjust global level via `--log-level DEBUG` or `--verbose` (alias for run-all).

## 13. Data Quality Checklist (Daily)
1. Vendor CSV row counts within expected range (detect anomalies).
2. % matched Localiza versions (monitor drift; threshold tune if falls sharply).
3. New unmatched versions list (investigate normalization gaps).
4. FIPE dump row count equals tuple set size times number of reference months targeted.
5. No excessive 429 warnings in logs (else slow parameters).

## 14. Known Limitations
- Movida parser currently ignores some optional attributes (extend as needed).
- Only deterministic heuristic scoring—no ML ranking.
- FIPE API schema changes could break parse; watch for missing `Valor` / `CodigoFipe` fields.

## 15. Security / Compliance
- No credentials stored; all endpoints public.
- Do not distribute scraped raw data without verifying terms of service.

## 16. Housekeeping
Use `python fleet.py clean --folder raw --keep 7` weekly to manage storage footprint.

## 17. Changelog Guidance
Maintain a `CHANGELOG.md` (not yet created) when altering matching heuristics so historical score shifts can be understood.

---
Maintainer tip: search for `# ----` section headers inside `fleet.py` for a quick outline of subsystems.
