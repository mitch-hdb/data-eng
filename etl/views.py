import re
import pandas as pd
from etl.db import get_conn

def create_views(cleaned_table="Cleaned"):
    conn = get_conn()
    cur = conn.cursor()

    # Helper view with storey_min / storey_max
    cur.execute(f"""
        CREATE OR REPLACE VIEW {cleaned_table}_with_storeys AS
        SELECT *,
               CAST(SUBSTRING(storey_range,1,2) AS UNSIGNED) AS storey_min,
               CAST(SUBSTRING(storey_range,8,2) AS UNSIGNED) AS storey_max
        FROM {cleaned_table};
    """)

    # Town views
    towns = pd.read_sql(f"SELECT DISTINCT town FROM {cleaned_table}", conn)
    for t in towns["town"].tolist():
        vname = "Town_" + re.sub(r"[^A-Z0-9]+","_", t.upper())
        print('town: ', t)
        print('view: ', towns["town"].tolist())
        #cur.execute(f"DROP VIEW IF EXISTS `{vname}`;")
        # cur.execute(f"""
        #     CREATE VIEW `{vname}` AS
        #     SELECT * FROM {cleaned_table} WHERE town = %s;
        # """, (t,))

    # Floor views
    # floors = pd.read_sql(f"""
    #     SELECT DISTINCT storey_min AS fl FROM {cleaned_table}_with_storeys
    #     UNION
    #     SELECT DISTINCT storey_max AS fl FROM {cleaned_table}_with_storeys
    # """, conn)["fl"].dropna().astype(int).unique().tolist()

    # for fl in floors:
    #     vname = f"Floor_{fl:02d}"
    #     cur.execute(f"DROP VIEW IF EXISTS `{vname}`;")
    #     cur.execute(f"""
    #         CREATE VIEW `{vname}` AS
    #         SELECT * FROM {cleaned_table}_with_storeys
    #         WHERE {fl} BETWEEN storey_min AND storey_max;
    #     """)

    cur.close()
    conn.close()
