"""
Parse Garmin Connect activity JSON files and write:
  - data/bronze/activities.parquet
  - data/bronze/splits.parquet

Keeps only activityType in {"running", "treadmill_running"}.
Accepts single .json, .jsonl, or a directory of files.
"""

from __future__ import annotations
import json
import sys
import typing as T
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import typer

app = typer.Typer(help="Garmin JSON → Parquet (running/treadmill only)")

# ---------- Unit helpers ----------

CM_PER_M = 100.0
CM_PER_MS_TO_MPS = 10.0  # 1 cm/ms = 10 m/s

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

# Some Garmin dumps (like your sample) spell Celsius as "CELCIUS".
# Handle both to be safe.
UNIT_MAP = {
    "CENTIMETERS_PER_MILLISECOND": ("speed_mps", CM_PER_MS_TO_MPS),
    "CENTIMETER": ("distance_m", 1.0 / CM_PER_M),
    "BPM": ("bpm", 1.0),
    "WATT": ("watt", 1.0),
    "CELSIUS": ("celsius", 1.0),
    "CELCIUS": ("celsius", 1.0),
    "MILLISECOND": ("seconds", 1.0 / 1000.0),
    "DIMENSIONLESS": ("value", 1.0),
    # add others here if you see them in your dumps
}

# ---------- Normalizers ----------

RUN_TYPES = {"running", "treadmill_running"}