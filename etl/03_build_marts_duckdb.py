import os, duckdb

ROOT = os.path.dirname(os.path.dirname(__file__))
SQL = os.path.join(ROOT, "sql", "metrics.sql")
MARTS = os.path.join(ROOT, "marts")
os.makedirs(MARTS, exist_ok=True)

COLUMN_ALIASES = {
    "provider id": "PROVNUM",
    "provider number": "PROVNUM",
    "federal provider number": "PROVNUM",
    "cms certification number (ccn)": "PROVNUM",
    "ccn": "PROVNUM",
}


def main():
    con = duckdb.connect()
    with open(SQL, "r") as f:
        con.execute(f.read())
    print("Marts built to marts/*.parquet")

if __name__ == "__main__":
    main()
