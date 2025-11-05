import os, glob
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(ROOT, "data", "raw")
DOCS_DIR = os.path.join(ROOT, "docs")
os.makedirs(DOCS_DIR, exist_ok=True)

def profile_file(path):
    name = os.path.basename(path)
    df = pd.read_csv(path)
    report = {
        "file": name,
        "rows": len(df),
        "cols": df.shape[1],
        "missing_cells": int(df.isna().sum().sum()),
        "dup_rows": int(df.duplicated().sum())
    }
    return report

def main():
    rows = []
    for f in glob.glob(os.path.join(RAW_DIR, "*.csv")):
        try:
            rows.append(profile_file(f))
        except Exception as e:
            rows.append({"file": os.path.basename(f), "error": str(e)})
    out = pd.DataFrame(rows)
    out.to_csv(os.path.join(DOCS_DIR, "data_quality.csv"), index=False)
    print(out)

if __name__ == "__main__":
    main()
