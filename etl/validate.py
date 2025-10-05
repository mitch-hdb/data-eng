import pandas as pd
from dateutil import parser
from datetime import date
import matplotlib.pyplot as plt
import seaborn as sns
import os

TODAY = date.today()

def parse_month_ok(x: str) -> bool:
    try:
        # accept YYYY-MM or YYYY-M
        parser.isoparse(f"{x}-01")
        return True
    except Exception:
        return False

def month_to_date(x: str) -> pd.Timestamp:
    # Use the direct | operator with None if NaT represents a missing value in your context, e.g., my_column: pd.Timestamp | None.
    try:
        return pd.to_datetime(x, format="%Y-%m", errors="raise")
    except Exception:
        try:
            return pd.to_datetime(x + "-01", errors="raise")
        except Exception:
            return pd.NaT
        
def parse_flat_type_ok(x: str) -> bool:
    flat_type_list = ['1 ROOM', '2 ROOM', '3 ROOM', '4 ROOM', '5 ROOM', 'EXECUTIVE', 'MULTI-GENERATION'] 
    # extract out enums to a different file?
    if x in flat_type_list:
        return True
    else:
        return False

def basic_filters(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    d = df.copy()
    d["month_date"] = d["month"].apply(month_to_date)
    d["valid_month"] = d["month"].apply(parse_month_ok)
    # value must be > 0
    d["valid_price"] = d["resale_price"].apply(lambda v: pd.notna(v) and v > 0)
    d["valid_area"] = d["floor_area_sqm"].apply(lambda v: pd.notna(v) and v > 0)
    # lease year must be between 1960 and today
    d["valid_lease_year"] = d["lease_commence_date"].apply(
        lambda y: pd.isna(y) or (1960 <= y <= TODAY.year)
    )
    # Flat type must be within the specified list
    d["valid_flat_type"] = d["flat_type"].apply(parse_flat_type_ok)
    
    req_str = ["town","flat_type","flat_model","block","street_name","storey_range"]
    for c in req_str:
        d[f"nonempty_{c}"] = d[c].astype("string").str.len().fillna(0) > 0

    mask_ok = (
        d["valid_month"] &
        d["valid_price"] & d["valid_area"] & d["valid_lease_year"] &
        d[[f"nonempty_{c}" for c in req_str]].all(axis=1)
    )
    
    return d[mask_ok].drop(columns=[col for col in d.columns if col.startswith("valid") or col.startswith("nonempty_")]), d[~mask_ok]

def recompute_remaining(df: pd.DataFrame, today: date) -> pd.DataFrame:
    d = df.copy()
    # if month_date missing, fall back to the 1st of month parsed from text
    d["month_date"] = pd.to_datetime(d["month"].astype(str) + "-01", errors="coerce")
    # compute against TODAY (Asia/Singapore today)
    base = pd.Timestamp(today)

    # use lease_commence_date where available
    lease_start = pd.to_numeric(d["lease_commence_date"], errors="coerce")
    lease_end_year = lease_start + 99

    # months remaining = (Dec of end_year) - (today), approx by year*12
    years_left = lease_end_year - base.year
    months_left = (years_left * 12) - (base.month - 1)
    months_left = months_left.clip(lower=0, upper=99*12)

    d["remaining_lease_months"] = months_left
    d["remaining_lease"] = (months_left // 12).astype(int).astype(str) + " years " + (months_left % 12).astype(int).astype(str) + " months"
    return d

def dedupe_keep_max(df: pd.DataFrame):
    # composite key = all columns except resale_price
    key_cols = [c for c in df.columns if c != "resale_price"]
    # mark group max
    g = df.groupby(key_cols, dropna=False, as_index=False)["resale_price"].max()
    cleaned = g.copy()

    # identify dropped duplicates (lower price than max)
    merged = df.merge(g, on=key_cols, suffixes=("_orig","_max"))
    failed_dups = merged[merged["resale_price_orig"] < merged["resale_price_max"]]
    failed_dups = failed_dups[key_cols + ["resale_price_orig"]].rename(columns={"resale_price_orig":"resale_price"})
    return cleaned, failed_dups

def flag_anomalies(df: pd.DataFrame):
    d = df.copy()
    d["price_per_sqm"] = d["resale_price"] / d["floor_area_sqm"]
    def cohort_bounds(g):
        q1, q3 = g["price_per_sqm"].quantile([0.25, 0.75])
        iqr = q3 - q1
        lo, hi = q1 - 1.5*iqr, q3 + 1.5*iqr
        return pd.Series({"lo": lo, "hi": hi})
    bounds = d.groupby(["town","flat_type"]).apply(cohort_bounds).reset_index()
    d = d.merge(bounds, on=["town","flat_type"], how="left")
    d["anomaly_ppsqm"] = ~d["price_per_sqm"].between(d["lo"], d["hi"])
    d["abs_guard"] = ~d["price_per_sqm"].between(500, 30000)
    d["is_anomaly"] = d["anomaly_ppsqm"] | d["abs_guard"]
    anomalies = d[d["is_anomaly"]].copy()
    ok = d[~d["is_anomaly"]].copy()
    return ok.drop(columns=["price_per_sqm","lo","hi","anomaly_ppsqm","abs_guard","is_anomaly"]), anomalies


# --- Detect anomalies per (town, flat_type) ---
def detect_anomalies(group):
    q1 = group["price_per_sqm"].quantile(0.25)
    q3 = group["price_per_sqm"].quantile(0.75)
    iqr = q3 - q1
    lower, upper = q1 - 1.5*iqr, q3 + 1.5*iqr
    group["anomaly"] = (group["price_per_sqm"] < lower) | (group["price_per_sqm"] > upper)
    return group

def plot_graph_anomalies(df: pd.DataFrame):
    d = df.copy()
    d["price_per_sqm"] = d["resale_price"] / d["floor_area_sqm"]
    d = d.groupby(["town", "flat_type"], group_keys=False).apply(detect_anomalies)

    os.makedirs("output/anomalies_by_town", exist_ok=True)


    for town, subdf in d.groupby("town"):
        plt.figure(figsize=(6,5))
        
        # Plot box graph with IQR range indicators
        sns.catplot(
        data=subdf,
        x="flat_type",
        y="price_per_sqm",
        col="town",
        kind="box",
        showfliers=False,
        height=6,
        aspect=1
        )
        
        # Overlay anomalies as red X markers
        sns.scatterplot(
            data=subdf,
            x="flat_type",
            y="price_per_sqm",
            hue="anomaly",
            palette={True: "red", False: "green"},
            style="anomaly",
            markers={True: "X", False: "o"},
            s=50
        )
        # Titles and labels
        plt.title(f"Town: {town} — Anomaly Detection")
        plt.ylabel("Price per sqm (SGD)")
        plt.xlabel("Flat Type")
        plt.legend()
        
        # --- Save graph ---
        output_path = f"output/anomalies_by_town/{town.replace(' ', '_').replace('/','_')}.jpg"
        plt.tight_layout()
        plt.savefig(output_path, dpi=300)
        plt.close()
        
        print(f"Saved: {output_path}")
        
    plt.close()
