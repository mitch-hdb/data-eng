import hashlib, os
import pandas as pd
from typing import Union
import numpy as np

SALT = os.environ.get("PII_SALT", 'HDB_R35AL3')

def mask_value(x: str) -> str:
    h = hashlib.sha256((SALT + "|" + str(x)).encode("utf-8")).hexdigest()
    return h[:24]    # 24 hex chars

def mask_pii(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["PII_Masked"] = d["PII_Key"].apply(mask_value)
    return d

# --- Convert NaN values to -1 to write into mySQL database ---
def convert_nan_to_int(df: pd.DataFrame) -> pd.DataFrame:
# Rule: Strings/objects get "-1", numerics get -1
    for col in df.columns:
        if df[col].dtype in [np.float64, np.int64]:  # numeric columns
            df[col] = df[col].fillna(-1).astype(int)
        else:  # string/object columns
            df[col] = df[col].fillna("-1")
    return df
