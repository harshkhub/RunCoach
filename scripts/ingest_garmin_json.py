#!/usr/bin/env python3
"""
Garmin JSON / JSONL -> Parquet (running & treadmill_running only)

Inputs:
  - A .jsonl file where each line is one activity object (recommended)
  - OR a .json / .jsonl file containing arrays/objects
  - OR a directory of .json/.jsonl files

Outputs:
  data/bronze/activities.parquet
  data/bronze/splits.parquet
"""

from __future__ import annotations
import json
import sys
import typing as T
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import typer

app = typer.Typer(help="Ingest Garmin activities into Parquet (running-only).")

# -------------------- Config & unit helpers --------------------

RUN_TYPES = {"running", "treadmill_running"}

CM_PER_M = 100.0
CM_PER_MS_TO_MPS = 10.0  # 1 cm/ms == 10 m/s

def ms_to_iso(ms: T.Optional[float]) -> T.Optional[str]:
    if ms is None:
        return None
    return datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc).isoformat()

def ms_to_s(ms: T.Optional[float]) -> T.Optional[float]:
    return None if ms is None else float(ms) / 1000.0

def cm_to_m(cm: T.Optional[float]) -> T.Optional[float]:
    return None if cm is None else float(cm) / CM_PER_M

def cm_per_ms_to_mps(v: T.Optional[float]) -> T.Optional[float]:
    return None if v is None else float(v) * CM_PER_MS_TO_MPS

# Common Garmin unit map for split measurements
UNIT_MAP = {
    "CENTIMETERS_PER_MILLISECOND": ("speed_mps", CM_PER_MS_TO_MPS),
    "CENTIMETER": ("distance_m", 1.0 / CM_PER_M),
    "BPM": ("bpm", 1.0),
    "WATT": ("watt", 1.0),
    "CELSIUS": ("celsius", 1.0),
    "CELCIUS": ("celsius", 1.0),  # misspelling appears in your data
    "MILLISECOND": ("seconds", 1.0 / 1000.0),
    "DIMENSIONLESS": ("value", 1.0),
}

# -------------------- Normalizers --------------------

def is_running_activity(activity: dict) -> bool:
    atype = (activity.get("activityType") or "").lower()
    return atype in RUN_TYPES

def norm_activity(a: dict) -> dict:
    min_t = a.get("minTemperature")
    max_t = a.get("maxTemperature")
    avg_t = None
    if min_t is not None and max_t is not None:
        avg_t = (float(min_t) + float(max_t)) / 2.0

    return {
        "activity_id": a.get("activityId"),
        "activity_type": (a.get("activityType") or "").lower(),
        "sport": a.get("sportType"),
        "start_time_gmt": ms_to_iso(a.get("startTimeGmt")),
        "start_time_local": ms_to_iso(a.get("startTimeLocal")),
        "duration_s": ms_to_s(a.get("duration")),
        "elapsed_s": ms_to_s(a.get("elapsedDuration", a.get("duration"))),
        "moving_s": ms_to_s(a.get("movingDuration", a.get("duration"))),
        "distance_m": cm_to_m(a.get("distance")),
        "avg_speed_mps": cm_per_ms_to_mps(a.get("avgSpeed")),
        "max_speed_mps": cm_per_ms_to_mps(a.get("maxSpeed")),
        "avg_hr": a.get("avgHr"),
        "max_hr": a.get("maxHr"),
        "min_hr": a.get("minHr"),
        "avg_power_w": a.get("avgPower"),
        "max_power_w": a.get("maxPower"),
        "np_w": a.get("normPower"),
        "avg_double_cad_spm": a.get("avgDoubleCadence"),
        "avg_run_cad_single": a.get("avgRunCadence"),
        "avg_stride_cm": a.get("avgStrideLength"),
        "avg_gct_ms": a.get("avgGroundContactTime"),
        "avg_vo_cm": a.get("avgVerticalOscillation"),
        "avg_vertical_ratio": a.get("avgVerticalRatio"),
        "min_temp_c": min_t,
        "avg_temp_c": avg_t,
        "max_temp_c": max_t,
        "rpe": a.get("workoutRpe"),
        "feel": a.get("workoutFeel"),
        "device_id": a.get("deviceId"),
        "manufacturer": a.get("manufacturer"),
        "lap_count": a.get("lapCount"),
        # Optional geo (may not exist in treadmill)
        "start_lat": a.get("startLatitude"),
        "start_lon": a.get("startLongitude"),
        "end_lat": a.get("endLatitude"),
        "end_lon": a.get("endLongitude"),
        "location_name": a.get("locationName"),
    }

def flatten_split(activity_id: T.Any, split: dict) -> dict:
    base = {
        "activity_id": activity_id,
        "index": split.get("messageIndex"),
        "type": split.get("type"),
        "start_gmt": ms_to_iso(split.get("startTimeGMT")),
        "end_gmt": ms_to_iso(split.get("endTimeGMT")),
        "duration_s": ms_to_s((split.get("endTimeGMT") or 0) - (split.get("startTimeGMT") or 0)),
        "start_source": split.get("startTimeSource"),
        "end_source": split.get("endTimeSource"),
        "start_lat": split.get("startLatitude"),
        "start_lon": split.get("startLongitude"),
        "end_lat": split.get("endLatitude"),
        "end_lon": split.get("endLongitude"),
    }
    metrics: dict[str, T.Any] = {}
    for m in split.get("measurements", []):
        unit = (m.get("unitEnum") or "").upper()
        field = (m.get("fieldEnum") or "").upper()
        val = m.get("value")
        if val is None:
            continue
        if unit in UNIT_MAP:
            suffix, factor = UNIT_MAP[unit]
            key = f"{field.lower()}__{suffix}"  # e.g., weighted_mean_speed__speed_mps
            try:
                metrics[key] = float(val) * factor
            except Exception:
                metrics[key] = val
        else:
            metrics[f"{field.lower()}__raw"] = val
    base.update(metrics)
    return base

# -------------------- Loaders --------------------

def load_jsonl(path: Path) -> list[dict]:
    """Load one activity per line (JSONL)."""
    acts: list[dict] = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                o = json.loads(line)
            except Exception as e:
                print(f"[WARN] JSONL parse error at line {i} in {path.name}: {e}", file=sys.stderr)
                continue
            # Some wrappers put the object under "activity"
            a = o.get("activity", o)
            acts.append(a)
    return acts

def load_json_or_array(path: Path) -> list[dict]:
    """Load a single JSON file that is an object or array; unwrap common wrappers."""
    text = path.read_text(encoding="utf-8", errors="ignore").lstrip("\ufeff").strip()
    if not text:
        return []
    obj = json.loads(text)
    out: list[dict] = []

    def walk(x):
        if isinstance(x, dict):
            if "activityType" in x or "activityId" in x:
                out.append(x); return
            # unwrap common containers
            for k in ("summarizedActivitiesExport", "summarizedActivities", "activities", "activityList", "results"):
                v = x.get(k)
                if isinstance(v, list):
                    for it in v: walk(it)
                elif isinstance(v, dict):
                    walk(v)
            if "activity" in x and isinstance(x["activity"], dict):
                out.append(x["activity"])
        elif isinstance(x, list):
            for it in x: walk(it)

    walk(obj)
    return out

def gather_activities(input_path: Path) -> list[dict]:
    """Load activities from a file or directory. JSONL preferred."""
    files: list[Path] = []
    if input_path.is_dir():
        files = sorted([p for p in input_path.rglob("*") if p.suffix.lower() in {".json", ".jsonl"}])
    else:
        files = [input_path]

    activities: list[dict] = []
    for f in files:
        try:
            if f.suffix.lower() == ".jsonl":
                activities.extend(load_jsonl(f))
            else:
                activities.extend(load_json_or_array(f))
        except Exception as e:
            print(f"[WARN] Failed to read {f}: {e}", file=sys.stderr)
    return activities

# -------------------- Main --------------------

@app.command()
def run(
    input_path: Path = typer.Argument(..., help="Path to a JSONL/JSON file or a folder of such files"),
    out_dir: Path = typer.Option("data/bronze", "--out", help="Output folder for Parquet files"),
):
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_acts = gather_activities(input_path)
    if not raw_acts:
        typer.secho("No activities found in input.", fg=typer.colors.RED, err=True)
        raise typer.Exit(1)

    # Filter to running-only
    acts_run = [a for a in raw_acts if is_running_activity(a)]
    if not acts_run:
        typer.secho("No running/treadmill_running activities found.", fg=typer.colors.YELLOW)
        raise typer.Exit(1)

    # Normalize activities
    df_act = pd.DataFrame([norm_activity(a) for a in acts_run])

    # Normalize splits
    split_rows: list[dict] = []
    for a in acts_run:
        aid = a.get("activityId")
        for s in a.get("splits", []) or []:
            split_rows.append(flatten_split(aid, s))
    df_splits = pd.DataFrame(split_rows) if split_rows else pd.DataFrame(columns=["activity_id"])

    # Sort & write
    if not df_act.empty:
        if "start_time_gmt" in df_act.columns:
            df_act = df_act.sort_values(["start_time_gmt", "activity_id"], kind="mergesort").reset_index(drop=True)
        df_act.to_parquet(out_dir / "activities.parquet", index=False)
        typer.secho(f"Wrote {len(df_act):,} rows -> {out_dir/'activities.parquet'}", fg=typer.colors.GREEN)

    if not df_splits.empty:
        sort_cols = [c for c in ["activity_id", "index"] if c in df_splits.columns]
        if sort_cols:
            df_splits = df_splits.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
        df_splits.to_parquet(out_dir / "splits.parquet", index=False)
        typer.secho(f"Wrote {len(df_splits):,} rows -> {out_dir/'splits.parquet'}", fg=typer.colors.GREEN)
    else:
        typer.secho("No splits found in input (ok if export omitted them).", fg=typer.colors.YELLOW)

if __name__ == "__main__":
    app()
