#!/usr/bin/env python3
import json, math
from pathlib import Path
import pandas as pd

SILVER = Path("data/silver")
CAT_PATH = SILVER/"sessions.json"

def planned_pace_from_recent(df):
    # set a “training pace” anchor from last 2–4 weeks 10k-ish runs
    recent = df.sort_values("start_time_gmt").tail(12)
    ten_kish = recent[recent["distance_km"].between(8, 14)]
    base = ten_kish["avg_pace_s_per_km"].median()
    return float(base) if pd.notna(base) else 330.0  # fallback 5:30/km

def make_sessions(anchor_pace):
    # anchor_pace: s/km (e.g., recent steady effort)
    z2 = anchor_pace * 1.20
    tempo = anchor_pace * 0.90
    thresh = anchor_pace * 0.85
    vo2 = anchor_pace * 0.75

    sessions = [
        {"id":"z2-45", "type":"zone2", "duration_min":45, "target_pace_s_per_km":z2,
         "structure":[["steady", 45]], "treadmill_ok": True},
        {"id":"recovery-30", "type":"recovery", "duration_min":30, "target_pace_s_per_km":z2*1.05,
         "structure":[["steady", 30]], "treadmill_ok": True},
        {"id":"tempo-3x10", "type":"tempo", "duration_min":50, "target_pace_s_per_km":tempo,
         "structure":[["warmup",10], ["tempo",10], ["easy",5], ["tempo",10], ["easy",5], ["tempo",10]], "treadmill_ok": True},
        {"id":"thresh-4x6", "type":"threshold", "duration_min":55, "target_pace_s_per_km":thresh,
         "structure":[["warmup",10], ["threshold",6], ["easy",3]]*4, "treadmill_ok": True},
        {"id":"vo2-6x3", "type":"vo2max", "duration_min":50, "target_pace_s_per_km":vo2,
         "structure":[["warmup",12]] + [["vo2",3], ["easy",2]]*6 + [["cooldown",5]], "treadmill_ok": True},
        {"id":"long-90", "type":"long_run", "duration_min":90, "target_pace_s_per_km":z2*1.02,
         "structure":[["steady", 90]], "treadmill_ok": False},
    ]
    # round paces to 1s
    for s in sessions:
        s["target_pace_s_per_km"] = round(s["target_pace_s_per_km"], 1)
    return sessions

def main():
    fa = pd.read_parquet(SILVER/"features_activities.parquet")
    anchor = planned_pace_from_recent(fa)
    sessions = make_sessions(anchor)
    SILVER.mkdir(parents=True, exist_ok=True)
    CAT_PATH.write_text(json.dumps(sessions, indent=2))
    print("wrote:", CAT_PATH, "anchor pace (s/km):", anchor)

if __name__ == "__main__":
    main()
