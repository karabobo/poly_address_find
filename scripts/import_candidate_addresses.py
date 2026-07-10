#!/usr/bin/env python3
"""Import manually curated Polymarket wallet candidates from text and Excel files."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import OrderedDict
from pathlib import Path
from typing import Any

import pandas as pd


ADDRESS_RE = re.compile(r"0x[a-fA-F0-9]{40}")


def split_notes(value: str) -> list[str]:
    return [part.strip() for part in re.split(r"[;,|，；、\n]+", value) if part.strip()]


def add_record(
    records: OrderedDict[str, dict[str, Any]],
    address: str,
    source: str,
    note: str = "",
    link: str = "",
    label: str = "",
    status: str = "",
) -> None:
    normalized = address.lower()
    row = records.setdefault(
        normalized,
        {
            "address": normalized,
            "sources": [],
            "labels": [],
            "notes": [],
            "links": [],
            "status": [],
        },
    )
    if source and source not in row["sources"]:
        row["sources"].append(source)
    for field, value in (("labels", label), ("notes", note), ("links", link), ("status", status)):
        if value:
            for part in split_notes(str(value)):
                if part not in row[field]:
                    row[field].append(part)


def import_text(path: Path, records: OrderedDict[str, dict[str, Any]]) -> None:
    pending_notes: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        addresses = ADDRESS_RE.findall(line)
        if addresses:
            note_text = ADDRESS_RE.sub("", line).strip()
            note = "；".join([*pending_notes, note_text] if note_text else pending_notes)
            for address in addresses:
                add_record(records, address, path.name, note=note)
            pending_notes = []
        else:
            pending_notes.append(line)


def import_excel(path: Path, records: OrderedDict[str, dict[str, Any]]) -> None:
    workbook = pd.ExcelFile(path)
    for sheet in workbook.sheet_names:
        df = workbook.parse(sheet, dtype=str).fillna("")
        for idx, row in df.iterrows():
            row_values = {str(k).strip(): str(v).strip() for k, v in row.items()}
            combined = " ".join(row_values.values())
            addresses = ADDRESS_RE.findall(combined)
            if not addresses:
                continue
            source = f"{path.name}:{sheet}:row{idx + 2}"
            raw_link = row_values.get("链接", "")
            link = raw_link if raw_link.lower().startswith(("http://", "https://")) else ""
            label = row_values.get("标签", "")
            status = row_values.get("状态", "")
            note_parts = []
            if raw_link and not link and not ADDRESS_RE.search(raw_link):
                note_parts.append(f"链接={raw_link}")
            for key, value in row_values.items():
                if value and key not in {"地址", "链接", "标签", "状态"} and not ADDRESS_RE.search(value):
                    note_parts.append(f"{key}={value}")
            note = "；".join(note_parts)
            for address in addresses:
                add_record(records, address, source, note=note, link=link, label=label, status=status)


def flatten(row: dict[str, Any]) -> dict[str, str]:
    return {
        "address": row["address"],
        "sources": " | ".join(row["sources"]),
        "labels": " | ".join(row["labels"]),
        "notes": " | ".join(row["notes"]),
        "links": " | ".join(row["links"]),
        "status": " | ".join(row["status"]),
    }


def write_outputs(records: OrderedDict[str, dict[str, Any]], csv_path: Path, json_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [flatten(row) for row in records.values()]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["address", "sources", "labels", "notes", "links", "status"])
        writer.writeheader()
        writer.writerows(rows)
    json_path.write_text(json.dumps(list(records.values()), ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build candidate address library from curated files")
    parser.add_argument("--excel", action="append", default=[], help="xlsx file to import")
    parser.add_argument("--text", action="append", default=[], help="txt file to import")
    parser.add_argument("--csv-out", default="data/candidate_addresses.csv")
    parser.add_argument("--json-out", default="data/candidate_addresses.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    records: OrderedDict[str, dict[str, Any]] = OrderedDict()
    for path in args.text:
        import_text(Path(path), records)
    for path in args.excel:
        import_excel(Path(path), records)
    write_outputs(records, Path(args.csv_out), Path(args.json_out))
    print(f"wrote {len(records)} unique candidate addresses")
    print(args.csv_out)
    print(args.json_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
