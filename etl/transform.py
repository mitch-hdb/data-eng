import re
import pandas as pd

def first3_block_digits(block: str) -> str:
    digits = "".join(re.findall(r"\d", str(block)))
    return digits[:3].rjust(3, "0")

def two_digits_of_town_sum(df: pd.DataFrame) -> pd.Series:
    s = df.groupby("town")["resale_price"].sum().astype(int)
    first2 = s.astype(str).str[:2].str.rjust(2, "0")
    return first2

def build_pii(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    town_first2 = two_digits_of_town_sum(d)
    d["block3"] = d["block"].apply(first3_block_digits)
    d["month_mm"] = pd.to_datetime(d["month"]+"-01", errors="coerce").dt.strftime("%m")
    d["town2"]   = d["town"].map(town_first2)
    d["town_initial"] = d["town"].str[0].str.upper()

    d["PII_Key"] = (
        "S" + d["block3"].fillna("000") +
        d["town2"].fillna("00") +
        d["month_mm"].fillna("00") +
        d["town_initial"].fillna("X")
    )
    return d.drop(columns=["block3","month_mm","town2","town_initial"])


