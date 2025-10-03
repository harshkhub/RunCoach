#!/usr/bin/env python3
"""
Salvage activities from a malformed Garmin summarizedActivities export.
- Works even if the top-level array is truncated or has junk.
- Extracts objects inside "summarizedActivitiesExport":[ ... ] by brace balancing.
- Falls back to scanning the entire file for activity-like objects.
- Writes one clean JSON object per line (JSONL).

Usage:
  python scripts/salvage_garmin_activities.py data/raw/<summarized>.json --out data/raw/garmin_activities.jsonl
"""

from __future__ import annotations
import json, gzip, sys, argparse
from pathlib import Path
from typing import Iterable

def read_text_auto(p: Path) -> str:
    raw = p.read_bytes()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8", errors="ignore").lstrip("\ufeff")

def find_all(hay: str, needle: str) -> list[int]:
    i = 0; out=[]
    while True:
        j = hay.find(needle, i)
        if j < 0: break
        out.append(j); i = j + 1
    return out

def scan_objects(s: str, start: int, end: int | None = None) -> Iterable[str]:
    """Yield JSON object strings by brace balancing between [start, end). String/escape aware."""
    if end is None: end = len(s)
    i = start
    in_str = False; esc = False; depth = 0; obj_start = -1
    while i < end:
        ch = s[i]
        if in_str:
            if esc: esc = False
            elif ch == "\\": esc = True
            elif ch == '"': in_str = False
            i += 1; continue
        if ch == '"':
            in_str = True
        elif ch == '{':
            if depth == 0: obj_start = i
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and obj_start >= 0:
                yield s[obj_start:i+1]
                obj_start = -1
        i += 1

def looks_like_activity(txt: str) -> bool:
    # Quick heuristic so we don’t keep wrapper objects
    return ("\"activityType\"" in txt or "\"activityId\"" in txt) and "\"splitSummaries\"" in txt

def light_cleanup(txt: str) -> str:
    # Remove obvious stray control chars (keep newline/tab), fix CELCIUS spelling
    cleaned = []
    for c in txt:
        if c >= " " or c in "\n\t":
            cleaned.append(c)
    txt = "".join(cleaned)
    txt = txt.replace('"CELCIUS"', '"CELSIUS"')
    # strip stray '%' around commas/braces (best-effort)
    txt = txt.replace(",%", ",").replace("%,", ",").replace("%}", "}").replace("{%", "{")
    return txt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input", type=Path)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    s = read_text_auto(args.input)
    if not s:
        print("Empty input.", file=sys.stderr); sys.exit(1)
    if s.lstrip().startswith("<!DOCTYPE html"):
        print("Looks like an HTML error page; re-download from Garmin.", file=sys.stderr); sys.exit(2)

    total_candidates = 0; written = 0; skipped = 0
    with args.out.open("w", encoding="utf-8") as out:
        # 1) Preferred: carve inside each "summarizedActivitiesExport":[ ... (even if ']' missing)
        hits = find_all(s, '"summarizedActivitiesExport"')
        for h in hits:
            arr_l = s.find("[", h)
            if arr_l < 0: continue
            # scan from arr_l+1 to EOF for objects; stop when we likely leave the array (heuristic):
            # If we hit ']"' or '}]' followed by comma/new wrapper key, we probably passed the array.
            # But even if we don't, scanning objects is safe.
            for obj in scan_objects(s, arr_l+1, None):
                total_candidates += 1
                if not looks_like_activity(obj):
                    continue
                try:
                    o = json.loads(obj)
                except Exception:
                    try:
                        o = json.loads(light_cleanup(obj))
                    except Exception:
                        skipped += 1
                        continue
                out.write(json.dumps(o, ensure_ascii=False) + "\n")
                written += 1

        # 2) Fallback: if nothing written, scan the whole file for activity-shaped objects
        if written == 0:
            for obj in scan_objects(s, 0, None):
                total_candidates += 1
                if not looks_like_activity(obj):
                    continue
                try:
                    o = json.loads(obj)
                except Exception:
                    try:
                        o = json.loads(light_cleanup(obj))
                    except Exception:
                        skipped += 1
                        continue
                out.write(json.dumps(o, ensure_ascii=False) + "\n")
                written += 1

    print(f"Candidates seen: {total_candidates}; activities written: {written}; skipped: {skipped} -> {args.out}")
    if written == 0:
        print("No activity objects recovered. Double-check the file path and confirm it contains 'splitSummaries' blocks.", file=sys.stderr)
        sys.exit(3)

if __name__ == "__main__":
    main()
