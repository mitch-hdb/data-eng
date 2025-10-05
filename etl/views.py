import re
from etl.db import get_conn
import pymysql  
    
def create_views(cleaned_table="Cleaned"):
    """
    Creates:
     - helper view `{cleaned_table}_with_storeys` with parsed storey_min, storey_max
     - one view per town: Town_<SANITIZED_TOWN>
     - one view per floor: Floor_XX (XX zero-padded)
    """
    conn = get_conn()
    try:
        cur = conn.cursor()

        # 1) Helper view - parse storey_range into numeric min and max using REGEXP_SUBSTR
        #    This requires MySQL 8+ for REGEXP_SUBSTR
        helper_view = f"{cleaned_table}_with_storeys"
        cur.execute(f"DROP VIEW IF EXISTS `{helper_view}`;")

        cur.execute(f"""
            CREATE VIEW `{helper_view}` AS
            SELECT *,
                CAST(REGEXP_SUBSTR(storey_range, '\\\\d+') AS UNSIGNED) AS storey_min,
                CAST(REGEXP_SUBSTR(storey_range, '\\\\d+$') AS UNSIGNED) AS storey_max
            FROM `{cleaned_table}`;
        """)
        # No fetch, DDL executed.

        # 2) Get distinct towns (use SQL and fetch results via pymysql)
        cur.execute(f"SELECT DISTINCT town FROM `{cleaned_table}` WHERE town IS NOT NULL AND TRIM(town) <> '';")
        rows = cur.fetchall()  # returns list of dicts since cursorclass may be DictCursor; handle generically

        # rows might be list of tuples or dicts; handle both
        towns = []
        for r in rows:
            if isinstance(r, dict):
                # typical DictCursor
                town_val = r.get("town")
            else:
                # tuple-like (single column)
                town_val = r[0]
            if town_val is None:
                continue
            town_str = str(town_val).strip()
            if town_str == "":
                continue
            towns.append(town_str)

        # Create Town views
        for t in towns:
            # sanitized view name
            vname = 'town_' + re.sub(r"[^0-9A-Za-z]+", "_", t.strip().upper())
            # escape town literal safely for SQL (pymysql provides escape_string)
            safe_literal = pymysql.converters.escape_string(t)
            # drop view if exists and create
            cur.execute(f"DROP VIEW IF EXISTS `{vname}`;")
            create_sql = f"""
                CREATE VIEW `{vname}` AS
                SELECT * FROM `{cleaned_table}`
                WHERE town = '{safe_literal}';
            """
            cur.execute(create_sql)

        # 3) Create Floor views: find distinct storey_min and storey_max from helper_view
        cur.execute(f"""
            SELECT DISTINCT storey_min as fl FROM `{helper_view}` WHERE storey_min IS NOT NULL
            UNION
            SELECT DISTINCT storey_max as fl FROM `{helper_view}` WHERE storey_max IS NOT NULL;
        """)
        floor_rows = cur.fetchall()
        floors = set()
        for r in floor_rows:
            if isinstance(r, dict):
                fl = r.get("fl")
            else:
                fl = r[0]
            if fl is None:
                continue
            try:
                fl_int = int(fl)
            except Exception:
                continue
            floors.add(fl_int)

        # # Create a view per floor number
        for fl in sorted(floors):
            vname = f"Floor_{fl:02d}"
            cur.execute(f"DROP VIEW IF EXISTS `{vname}`;")
            cur.execute(f"""
                CREATE VIEW `{vname}` AS
                SELECT * FROM `{helper_view}`
                WHERE {fl} BETWEEN storey_min AND storey_max;
            """)

        # # Commit if autocommit is disabled; many get_conn() set autocommit=True, but just in case:
        try:
            conn.commit()
        except Exception:
            pass

    finally:
        cur.close()
        conn.close()
