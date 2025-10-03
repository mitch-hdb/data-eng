# etl/mysql_helpers.py
import pandas as pd
import numpy as np
from etl.db import get_conn  # your pymysql get_conn() function
import pymysql

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


def df_to_mysql(df: pd.DataFrame, table: str):
    """
    Create table from df and insert rows via pymysql.
    This function expects df to be sanitized (call sanitize_df_fill_minus_one first).
    """
    conn = get_conn()
    cur = conn.cursor()

    # Defensive: sanitize here too (idempotent)
    df = sanitize_df_fill_minus_one(df)

    # Drop + recreate table
    safe_table = table.replace("`", "``")
    cur.execute(f"DROP TABLE IF EXISTS `{safe_table}`;")

    # Build column definitions using pandas dtype checks (robust vs string matching)
    col_defs = []
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            col_defs.append(f"`{col}` BIGINT")
        elif pd.api.types.is_float_dtype(dtype):
            col_defs.append(f"`{col}` DOUBLE")
        else:
            # strings, datetimes converted to strings => TEXT
            col_defs.append(f"`{col}` TEXT")

    schema = ", ".join(col_defs)
    create_sql = f"CREATE TABLE `{safe_table}` ({schema});"
    cur.execute(create_sql)

    # Prepare insert
    cols = ", ".join([f"`{c}`" for c in df.columns])
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO `{safe_table}` ({cols}) VALUES ({placeholders})"

    # Convert numpy types to native Python types for pymysql; df.values.tolist() usually does that
    # but we'll ensure no numpy types remain by building a list of tuples.
    rows = []
    for row in df.itertuples(index=False, name=None):
        # convert numpy ints/floats to python native
        converted = []
        for v in row:
            if isinstance(v, (np.integer,)):
                converted.append(int(v))
            elif isinstance(v, (np.floating,)):
                converted.append(float(v))
            else:
                converted.append(v)
        rows.append(tuple(converted))

    # Batch insert (executemany). Chunk if you expect many rows.
    cur.executemany(insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()
