#!/usr/bin/env python3
"""
Convert Power BI DSR response (saved as JSON in input.txt) to CSV.

It specifically targets the "dsr" -> "DS" -> "PH" -> "DM*" structure where rows are
compressed using an "R" bitmask and partial "C" value arrays. The script
decompresses these rows and writes them to a CSV file.

Usage (default paths assume this script lives next to input.txt):
  python convert_to_csv_enhanced.py -i input.txt -o output.csv

Notes:
- We preserve the original JSON types: numbers stay numbers, strings stay strings.
- CSV is written with quotes around non-numeric fields.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple


def read_json(path: str) -> Any:
	with open(path, "r", encoding="utf-8") as f:
		text = f.read().strip()
		# Some captures contain trailing null bytes or BOM; strip BOM if present
		if text.startswith("\ufeff"):
			text = text.lstrip("\ufeff")
		return json.loads(text)


def iter_dsr_dm_segments(dsr: Dict[str, Any]) -> Iterable[List[Dict[str, Any]]]:
	"""
	Yield each DM* segment list found under dsr["DS"][i]["PH"][j].
	Each yielded value is the list assigned to a key like "DM0".
	"""
	if not isinstance(dsr, dict):
		return
	datasets = dsr.get("DS") or []
	for ds in datasets:
		ph_list = (ds or {}).get("PH") or []
		for ph in ph_list:
			if not isinstance(ph, dict):
				continue
			for key, value in ph.items():
				# We only care about keys like DM0, DM1, etc., whose value is a list of row chunks
				if key.startswith("DM") and isinstance(value, list):
					yield value


def decompress_rows(dm_list: List[Dict[str, Any]]) -> List[List[Any]]:
	"""
	Given a DM* array (list of chunk dictionaries), return a list of full rows (lists).
	Implements the R-bitmask reuse per Power BI DSR encoding:
	- The first chunk typically includes schema "S" and a complete row in "C".
	- Subsequent chunks include partial "C" and an integer bitmask "R" indicating
	  which columns to REUSE from the previous row (1=reuse, 0=consume next value from C).
	"""
	rows: List[List[Any]] = []
	schema = None
	col_count = None
	prev: Optional[List[Any]] = None

	for chunk in dm_list:
		if not isinstance(chunk, dict):
			continue

		if schema is None and "S" in chunk:
			schema = chunk.get("S") or []
			if not isinstance(schema, list) or not schema:
				# If schema is malformed, we can't reliably process
				raise ValueError("Malformed or missing schema 'S' in DM segment")
			col_count = len(schema)

		if "C" not in chunk:
			continue

		if col_count is None:
			# No schema seen yet; infer column count from C the first time
			col_count = len(chunk["C"]) if isinstance(chunk["C"], list) else 0
			if col_count == 0:
				continue

		values = chunk.get("C") or []
		if not isinstance(values, list):
			# Unexpected type; skip
			continue

		if prev is None:
			# First row: fill left-to-right; pad if necessary
			row = list(values[:col_count])
			if len(row) < col_count:
				row.extend([None] * (col_count - len(row)))
			rows.append(row)
			prev = row
			continue

		# Subsequent rows: apply reuse mask R
		reuse_mask = chunk.get("R", 0)
		if not isinstance(reuse_mask, int):
			reuse_mask = 0

		row = list(prev)
		vi = 0
		for ci in range(col_count):
			# If bit is 1, reuse from prev; otherwise consume next value from C
			if (reuse_mask >> ci) & 1:
				continue
			if vi < len(values):
				row[ci] = values[vi]
				vi += 1
			else:
				row[ci] = None

		rows.append(row)
		prev = row

	return rows


def _get_first_schema_codes_from_dsr(dsr: Dict[str, Any]) -> Optional[List[str]]:
	for dm_list in iter_dsr_dm_segments(dsr):
		for chunk in dm_list:
			if isinstance(chunk, dict) and isinstance(chunk.get("S"), list):
				S = chunk["S"]
				codes_list: List[str] = []
				for c in S:
					if isinstance(c, dict):
						n = c.get("N")
						if isinstance(n, str):
							codes_list.append(n)
				if codes_list:
					return codes_list
	return None


def _find_all_dsr_objects(obj: Any) -> List[Dict[str, Any]]:
	"""Recursively find all dicts that look like a DSR object (have 'DS' array)."""
	found: List[Dict[str, Any]] = []
	if isinstance(obj, dict):
		if "DS" in obj and isinstance(obj.get("DS"), list):
			found.append(obj)
		for v in obj.values():
			found.extend(_find_all_dsr_objects(v))
	elif isinstance(obj, list):
		for v in obj:
			found.extend(_find_all_dsr_objects(v))
	return found


def extract_all_rows(doc: Dict[str, Any]) -> List[List[Any]]:
	"""
	Traverse the full response structure to collect all rows from all DM* segments.
	"""
	all_rows: List[List[Any]] = []

	# Typical structure: { "results": [ { "result": { "dsr": { ... } } } ] }
	results = (doc or {}).get("results") or []
	if results:
		for r in results:
			result_dict = (r or {}).get("result") or {}
			# DSR can be under result.dsr or result.data.dsr
			candidates: List[Dict[str, Any]] = []
			if isinstance(result_dict, dict):
				if isinstance(result_dict.get("dsr"), dict):
					candidates.append(result_dict["dsr"])
				data = result_dict.get("data")
				if isinstance(data, dict) and isinstance(data.get("dsr"), dict):
					candidates.append(data["dsr"])

			for dsr in candidates:
				for dm_list in iter_dsr_dm_segments(dsr):
					rows = decompress_rows(dm_list)
					if rows:
						all_rows.extend(rows)

	# Fallback: sometimes the top-level may already be a dsr-like object
	if not all_rows:
		if isinstance(doc, dict) and isinstance(doc.get("dsr"), dict):
			for dm_list in iter_dsr_dm_segments(doc["dsr"]):
				rows = decompress_rows(dm_list)
				if rows:
					all_rows.extend(rows)

	# Last-resort: recursively search for DSR-like dicts anywhere
	if not all_rows:
		for dsr in _find_all_dsr_objects(doc):
			for dm_list in iter_dsr_dm_segments(dsr):
				rows = decompress_rows(dm_list)
				if rows:
					all_rows.extend(rows)

	return all_rows


def build_headers(doc: Dict[str, Any]) -> Optional[List[str]]:
	"""Build header names using descriptor.Select mapping and schema order.

	We map schema codes (e.g., G0, G1, M0) to descriptor names (e.g., "SNV.latitude").
	If mapping or schema is missing, return None.
	"""
	# Build code->name mapping from descriptor.Select
	code_to_name: Dict[str, str] = {}
	results = (doc or {}).get("results") or []
	for r in results:
		result_dict = (r or {}).get("result") or {}
		data = result_dict.get("data") if isinstance(result_dict, dict) else None
		descriptor = (data or {}).get("descriptor") if isinstance(data, dict) else None
		if isinstance(descriptor, dict):
			selects = descriptor.get("Select")
			if isinstance(selects, list):
				for item in selects:
					if not isinstance(item, dict):
						continue
					code = item.get("Value")
					name = item.get("Name")
					if isinstance(code, str) and isinstance(name, str):
						code_to_name[code] = name

	# Find the first schema codes order
	schema_codes: Optional[List[str]] = None

	# Look under standard dsr locations
	for r in results:
		result_dict = (r or {}).get("result") or {}
		for dsr_candidate in (
			result_dict.get("dsr") if isinstance(result_dict, dict) else None,
			((result_dict.get("data") or {}).get("dsr") if isinstance(result_dict, dict) else None),
		):
			if isinstance(dsr_candidate, dict):
				schema_codes = _get_first_schema_codes_from_dsr(dsr_candidate)
				if schema_codes:
					break
		if schema_codes:
			break

	# Fallback: search anywhere
	if not schema_codes:
		for dsr in _find_all_dsr_objects(doc):
			schema_codes = _get_first_schema_codes_from_dsr(dsr)
			if schema_codes:
				break

	if not schema_codes:
		return None

	# Map schema codes to display names; fallback to code itself
	headers = [code_to_name.get(code, code) for code in schema_codes]
	return headers


def write_csv(rows: List[List[Any]], out_path: str, header: Optional[List[str]] = None) -> None:
	# Ensure directory exists
	os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
	with open(out_path, "w", newline="", encoding="utf-8") as f:
		writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
		if header:
			writer.writerow(header)
		for row in rows:
			writer.writerow(row)


def main(argv: Optional[List[str]] = None) -> int:
	parser = argparse.ArgumentParser(description="Convert Power BI DSR JSON to CSV")
	parser.add_argument("-i", "--input", default="input.txt", help="Path to JSON input file (default: input.txt)")
	parser.add_argument("-o", "--output", default="output.csv", help="Path to CSV output file (default: output.csv)")
	args = parser.parse_args(argv)

	try:
		doc = read_json(args.input)
	except FileNotFoundError:
		print(f"Input file not found: {args.input}", file=sys.stderr)
		return 1
	except json.JSONDecodeError as e:
		print(f"Failed to parse JSON in {args.input}: {e}", file=sys.stderr)
		return 2

	try:
		rows = extract_all_rows(doc)
	except Exception as e:
		print(f"Error extracting rows: {e}", file=sys.stderr)
		return 3

	if not rows:
		print("No rows found in DSR structure.", file=sys.stderr)
		return 4

	try:
		header = build_headers(doc)
		write_csv(rows, args.output, header=header)
	except Exception as e:
		print(f"Failed to write CSV: {e}", file=sys.stderr)
		return 5

	print(f"Wrote {len(rows)} rows to {args.output}")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())

