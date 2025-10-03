# Create Python virtual environment using the venv module

- Create the virtual environment:
  - `python3 -m venv .venv`
  - Note: Use python instead of python3 on some systems, or `py -m venv .venv` on Windows.
- Activate the virtual environment:
  - `source .venv/bin/activate` (macOS)
  - `.venv\Scripts\activate.bat` (Windows)
- Once activated, your terminal prompt will typically show the name of the virtual environment in parentheses (e.g., (.venv)) to indicate that it is active. You can now install packages specific to this project using pip, and they will be isolated within this environment.

# Datasets

Datasets used: HDB Resale Flat Prices collection (1990→present slices). Cite the collection page and coverage as of Sep 2, 2025.
[data.gov.sg](https://data.gov.sg/collections/189/view)

# Requirements

Assumptions: 99-year leases; recomputed remaining lease against “today” (system date, Asia/Singapore). Composite key is all non-price columns.

Imputation policy: No imputation for critical ID fields; such rows are rejected to Failed.

Deduping rule: Same composite key with different price → keep higher price, discard lower (logged in Failed with reason duplicate_lower_price).

Anomaly heuristic: IQR on price per sqm per (town, flat_type) plus absolute guards [500, 30,000] SGD/m². Flagged rows go to Failed with anomaly_ppsqm for review.

PII Key spec:
S + first 3 digits of block (left-pad zeros) + first two digits of integer sum of all resale_price in that town (left-pad) + month (MM) + first letter of town.
Example: Block “A12B”, Town “ANG MO KIO”, Month “2012-01” → S012{TT}{01}A where {TT} depends on town price sum.

Masking: Salted SHA-256 of PII_Key, first 24 hex chars. Deterministic & unique-preserving given a constant salt.

# Writing to DB

DB objects:

Tables: Raw, Cleaned, Failed, Transformed (adds PII_Key), Masked (adds PII_Masked, excludes raw PII).

The robust fix is to sanitize the DataFrame before writing, handling each dtype appropriately:convert boolean columns to integer and fill -1,fill numeric NaNs with -1 (or -1.0 for floats),convert datetimes to strings (or fill with -1),convert object/string/category columns to strings and fill "-1".

# Notes & recommendations

This approach will replace all missing values with -1 (or "-1" for text/datetime). That is your stated business assumption; be sure downstream consumers understand -1 is a sentinel for missingness.

If you prefer to keep datetimes as NULL rather than "-1", change the datetime branch to df[col] = df[col].fillna(pd.NaT) and adjust the DB schema to use DATETIME with NULL allowed.

For very large DataFrames, consider chunked inserts (split rows into batches of e.g. 1000–5000) to reduce memory/transaction size.

If you want all tables sanitized automatically, call sanitize_df_fill_minus_one for each table prior to df_to_mysql.

# ---------

Views: one per Town (Town_ANG_MO_KIO, …) and one per Floor (Floor_02, …). Floors are computed by parsing storey_range (“01 TO 03”) and checking containment.

# Steps to Reproduce

Repro steps:

pip install -r requirements.txt

Download 2 CSVs into data/ from the collection page.
data.gov.sg

(Optional) set PII_SALT env var.

python3 -m etl.run_etl.py

Inspect hdb_resale.db with any SQLite browser.

# Notes on alternatives / extensions

Connected to Docker via Colima.

Used MySQL client.

# Create a new DB

`docker ps -a` - view docker container
`docker kill [container_id]` - Kill docker container
`docker rm [container_id]` - Remove docker container
`docker run --name mysql-hdb -p 3306:3306 -e MYSQL_ROOT_PASSWORD=1234 -e MYSQL_DATABASE=hdb_resale -d mysql:latest --default-authentication-plugin=mysql_native_password` - Create mysql client
`docker run --name mysql-hdb -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -e MYSQL_DATABASE=hdb_resale -d mysql:latest` - Run MySQL with empty root password

# Starting up mysql service

`brew services start mysql` - start mysql service
`brew services stop mysql` - stop mysql service
`brew services restart mysql` - restart mysql service

# Check MySQL

`mysql -h127.0.0.1 -uroot hdb_resale -e "SHOW TABLES;"` - Show Tables
`mysql -h127.0.0.1 -uroot hdb_resale -e "DROP TABLE Students;"` - Drop Table

`mysql -h127.0.0.1 -uroot -e "CREATE DATABASE hdb_resale"` - Create database within mysql container
`mysql -h127.0.0.1 -uroot hdb_resale_db -e "CREATE TABLE Students(StudentID INT PRIMARY KEY, FirstName VARCHAR(50) NOT NULL);"` -

# Run ETL pipeline

`python -m etl.run_etl`

# Create Table

You should see

<pre>
Raw
Cleaned
Failed
Transformed
Masked
</pre>

# To see views

`mysql -h127.0.0.1 -uroot hdb_resale -e "SHOW FULL TABLES WHERE TABLE_TYPE LIKE 'VIEW';"`
