# etl/mysql_helpers.py
import pandas as pd
import numpy as np

def sanitize_df_fill_minus_one(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace NaNs/NA/pd.NA in df with -1 for numeric-like columns,
    and '-1' for text-like columns. Convert boolean columns to integers
    so they can hold -1.
    Returns a new DataFrame (copy).
    """
    df = df.copy()

    for col in df.columns:
        dtype = df[col].dtype

        # Booleans: convert to pandas nullable integer then fill and cast to numpy int64
        if pd.api.types.is_bool_dtype(dtype):
            # Use pandas nullable Int64 to allow fill, then convert to normal int64
            df[col] = df[col].astype("Int64").fillna(-1).astype("int64")

        # Integer-like (including pandas nullable Int64 etc.)
        elif pd.api.types.is_integer_dtype(dtype):
            # fill NaNs (if any) with -1 then cast to int64
            df[col] = df[col].fillna(-1).astype("int64")

        # Float-like
        elif pd.api.types.is_float_dtype(dtype):
            # fill NaN with -1.0 and keep float dtype
            df[col] = df[col].fillna(-1.0).astype("float64")

        # Datetime-like
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            # Convert to ISO string and fill missing as '-1'
            df[col] = df[col].dt.strftime("%Y-%m-%d").fillna("-1").astype(str)

        # All others (object, category, string, mixed)
        else:
            df[col] = df[col].astype("string").fillna("-1").astype(str)

    # Final check — replace any remaining pandas NA/np.nan explicitly
    df = df.replace({pd.NA: "-1", np.nan: -1})

    return df