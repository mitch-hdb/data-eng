import pandas as pd
from pathlib import Path

CANON_COLS = ["month","town","flat_type","block","street_name","storey_range",
              "floor_area_sqm","flat_model","lease_commence_date","remaining_lease",
              "resale_price"]

def read_and_canonize(fp: Path) -> pd.DataFrame:
    df = pd.read_csv(fp)
    # bring all expected columns in; add if missing
    for c in CANON_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    # keep only canon columns, in order
    df = df[CANON_COLS].copy()

    # basic cleaning for the Raw layer (formatting only; no imputation yet)
    for c in ["town","flat_type","flat_model","street_name","block","storey_range"]:
        df[c] = (df[c]
                 .astype("string")
                 .str.strip()
                 .str.replace(r"\s+", " ", regex=True))

    # unify month to yyyy-MM as text; keep original as-is in Raw
    df["month"] = df["month"].astype("string").str.strip()

    # numeric coercions
    df["resale_price"] = (df["resale_price"]
                          .astype("string").str.replace(",","", regex=False)
                          .astype("float64"))
    df["floor_area_sqm"] = pd.to_numeric(df["floor_area_sqm"], errors="coerce")
    df["lease_commence_date"] = pd.to_numeric(df["lease_commence_date"], errors="coerce")

    return df


def load_raw_all_format():
    base = Path("data")
    files = [
        "resale_2012mar_2014.csv",
        "resale_2015_2016.csv",
    ]
    dfs = [read_and_canonize(base/f) for f in files]
    raw = pd.concat(dfs, ignore_index=True)
    # keep exact raw format (no imputations);
    return raw
    
    
    
def load_raw_all():
    base = Path("data")
    files = [
        "resale_2012mar_2014.csv",
        "resale_2015_2016.csv",
    ]
    dfs = [pd.read_csv(base/f) for f in files]
    raw = pd.concat(dfs, ignore_index=True)
    # keep exact raw format (no imputations);
    return raw
