import hashlib, os
import pandas as pd

from dotenv import load_dotenv, find_dotenv
load_dotenv(dotenv_path=find_dotenv(), verbose=True)

SALT = os.environ.get("PII_SALT")

def mask_value(x: str) -> str:
    h = hashlib.sha256((SALT + "|" + str(x)).encode("utf-8")).hexdigest()
    return h[:24]    # 24 hex chars

def mask_pii(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["PII_Masked"] = d["PII_Key"].apply(mask_value)
    return d

def print_block(message: str):
    print('')
    print('========================================')
    print(message)
    print('========================================')
    print('')
