import pandas as pd
from etl.db import get_conn
from etl.mysql_helpers import sanitize_df_fill_minus_one
import numpy as np
    
    
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
