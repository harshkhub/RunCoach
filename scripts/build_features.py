#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

BRONZE = Path("data/bronze")
SILVER = Path("data/silver")
SILVER.mkdir(parents=True, exist_ok=True)

def parse_dt(col):
    """
    Robust ISO8601 parser:
    - handles strings with/without fractional seconds, with/without timezone
    - leaves already-datetime values alone
    - coerces bad rows to NaT (won't crash)
    """
    if pd.api.types.is_datetime64_any_dtype(col):
        return col.dt.tz_convert("UTC") if col.dt.tz is not None else col.dt.tz_localize("UTC")
    # pandas >=2.0 supports format="ISO8601"; fall back to "mixed" if needed
    try:
        return pd.to_datetime(col, format="ISO8601", utc=True, errors="coerce")
    except TypeError:
        # older pandas: use mixed inference
        return pd.to_datetime(col, utc=True, errors="coerce")

def pace_s_per_km(avg_speed_mps):
    return 1000.0 / max(avg_speed_mps, 1e-6)

def load():
    acts = pd.read_parquet(BRONZE/"activities.parquet")
    acts["start_time_gmt"]  = parse_dt(acts["start_time_gmt"])
    acts["start_time_local"] = parse_dt(acts.get("start_time_local", acts["start_time_gmt"]))
    if (BRONZE/"splits.parquet").exists():
        splits = pd.read_parquet(BRONZE/"splits.parquet")
    else:
        splits = pd.DataFrame()
    return acts, splits

def per_activity_features(acts):
    df = acts.copy()
    df["distance_km"] = df["distance_m"] / 1000
    df["duration_min"] = df["duration_s"] / 60
    df["avg_pace_s_per_km"] = df["avg_speed_mps"].apply(pace_s_per_km)
    df["elev_gain_m"] = 0.0  # if you mapped elevation cm -> m earlier, fill it here
    # temp buckets
    df["temp_c"] = df[["min_temp_c","max_temp_c"]].mean(axis=1, skipna=True)
    # type flags
    df["is_treadmill"] = (df["activity_type"]=="treadmill_running").astype(int)
    return df

def weekly_rollups(df):
    w = df.set_index("start_time_gmt").sort_index()
    g = w.resample("W")
    agg = pd.DataFrame({
        "weekly_km": g["distance_km"].sum(),
        "weekly_runs": g["activity_id"].count(),
        "weekly_duration_min": g["duration_min"].sum(),
        "avg_pace_s_per_km": (1000.0 / (g["distance_m"].sum()/w.resample("W")["duration_s"].sum())).fillna(pd.NA),
    }).reset_index().rename(columns={"start_time_gmt":"week"})
    # ACWR: 7d / 28d (very rough)
    w7 = w["distance_km"].rolling("7D").sum()
    w28 = w["distance_km"].rolling("28D").sum()
    w = w.assign(acwr = (w7 / (w28/4)).fillna(0))
    acwr = w.resample("W")["acwr"].mean().reset_index(drop=True)
    agg["acwr"] = acwr
    return agg

def main():
    acts, _splits = load()
    fa = per_activity_features(acts)
    fw = weekly_rollups(fa)

    fa.to_parquet(SILVER/"features_activities.parquet", index=False)
    fw.to_parquet(SILVER/"features_weekly.parquet", index=False)

    print("wrote:", SILVER/"features_activities.parquet")
    print("wrote:", SILVER/"features_weekly.parquet")

if __name__ == "__main__":
    main()
