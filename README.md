# Create Python virtual environment using the venv module

- Create the virtual environment:
  - `python3 -m venv .venv`
  - Note: Use python instead of python3 on some systems, or `py -m venv .venv` on Windows.
- Activate the virtual environment:
  - `source .venv/bin/activate` (macOS)
  - `.venv\Scripts\activate.bat` (Windows)
- Once activated, your terminal prompt will typically show the name of the virtual environment in parentheses (e.g., (.venv)) to indicate that it is active. You can now install packages specific to this project using pip, and they will be isolated within this environment.

> Current Python Version: Python 3.13.7

# Datasets

Datasets used: HDB Resale Flat Prices collection (1990→present slices). Cite the collection page and coverage as of Sep 2, 2025.
[data.gov.sg](https://data.gov.sg/collections/189/view)

# Steps to Reproduce

Reproduction steps:

1. pip install -r requirements.txt

2. Download 2 CSVs into data/ from the collection page. data.gov.sg

3. (Optional) set PII_SALT env var.

4. python3 -m etl.run_etl.py

# Notes on Extensions

Connected to Docker via Colima.

Used MySQL client for RDBMS.

# Starting up MYSQL service

1. > <code>colima start --cpu 4 --memory 4</code>
2. > <code>brew services start colima</code> - To automatically start Colima when your Mac boots, you can use Homebrew services:
3. > <code>docker run --name mysql-hdb -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -e MYSQL_DATABASE=hdb_resale -d mysql:latest</code> - Run MySQL with empty root password
4. > <code>brew services start mysql</code> - start mysql service if not started
5. > <code>mysql -h127.0.0.1 -uroot -e "CREATE DATABASE hdb_resale"</code> - Create database within mysql container

# Run ETL pipeline

> <code>python3 -m etl.run_etl</code>

# Check MySQL

1. > <code>mysql -h127.0.0.1 -uroot hdb_resale -e "SHOW TABLES;"</code> - Show Tables
2. > <code>mysql -h127.0.0.1 -uroot hdb_resale -e "DROP TABLE Students;"</code> - Drop Table
3. > <code>mysql -h127.0.0.1 -uroot hdb_resale_db -e "CREATE TABLE Students(StudentID INT PRIMARY KEY, FirstName VARCHAR(50) NOT NULL);"</code>

# Stopping / Restarting MySQL services

1. `brew services stop mysql` - stop mysql service
2. `brew services restart mysql` - restart mysql service

# Create a new DB

- `docker ps -a` - view docker container
- `docker kill [container_id]` - Kill docker container
- `docker rm [container_id]` - Remove docker container
- `docker run --name mysql-hdb -p 3306:3306 -e MYSQL_ROOT_PASSWORD=1234 -e MYSQL_DATABASE=hdb_resale -d mysql:latest --default-authentication-plugin=mysql_native_password` - Create mysql client with password
- `docker run --name mysql-hdb -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -e MYSQL_DATABASE=hdb_resale -d mysql:latest` - Run MySQL with empty root password

# Create Table

You should see

<pre>
Raw
Cleaned
Failed
Transformed
Masked
</pre>

# To see all Views

`mysql -h127.0.0.1 -uroot hdb_resale -e "SHOW FULL TABLES WHERE TABLE_TYPE LIKE 'VIEW';"`
