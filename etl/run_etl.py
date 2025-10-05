from etl.extract import load_raw_all_format
from etl.validate import basic_filters
from etl.validate import flag_anomalies, dedupe_keep_max, recompute_remaining, plot_graph_anomalies
from etl.load import df_to_mysql
from etl.transform import build_pii
from etl.config import mask_pii, print_block
from etl.views import create_views
import pandas as pd
import numpy as np
from datetime import date
from etl.mysql_helpers import sanitize_df_fill_minus_one

def main():
    print_block('Executing py file...')
    # 1) Raw
    raw = load_raw_all_format()
    df_to_mysql(raw, "Raw")

    # 2) Validate basics, split
    ok, failed = basic_filters(raw)
    failed["fail_reason"] = "basic_validation"

    # 3) Recompute remaining lease (today = system date)
    ok = recompute_remaining(ok, date.today())

    # 4) Dedupe by composite key, keep max price
    cleaned, dup_dropped = dedupe_keep_max(ok)
    dup_dropped["fail_reason"] = "duplicate_lower_price"
    failed = pd.concat([failed, dup_dropped], ignore_index=True)

    # 5) Anomaly screening
    cleaned2, anomalies = flag_anomalies(cleaned)
    anomalies["fail_reason"] = "anomaly_ppsqm"
    failed = pd.concat([failed, anomalies], ignore_index=True)
    failed = sanitize_df_fill_minus_one(failed)
    plot_graph_anomalies(cleaned)

    df_to_mysql(cleaned2, "Cleaned")
    df_to_mysql(failed, "Failed")

    # 6) Transform (PII)
    transformed = build_pii(cleaned2)
    df_to_mysql(transformed, "Transformed")

    masked = mask_pii(transformed)
    df_to_mysql(masked.drop(columns=["PII_Key"]), "Masked")

    # 7) Create views
    create_views("Cleaned")
    print_block('Successfully executed py file...')
if __name__ == "__main__":
    main()
