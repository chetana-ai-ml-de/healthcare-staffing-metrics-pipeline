# etl/02_transform_to_parquet.py
import os
import re
import pandas as pd
import numpy as np
from datetime import datetime

# ---------- Project folders ----------
ROOT = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(ROOT, "data", "raw")
CUR_DIR = os.path.join(ROOT, "data", "curated")
os.makedirs(CUR_DIR, exist_ok=True)

# ---------- Choose your main CSV here ----------
# If you switch to a different file, just change this name.
MASTER_NAME = "NH_ProviderInfo_Oct2024.csv"

# ---------- Helper: infer YYYY-MM from filename if no date col ----------
def month_from_filename(fname: str) -> str:
    # Examples:
    # NH_ProviderInfo_Oct2024.csv -> 2024-10
    # *_20241027.csv              -> 2024-10
    # *_2024_10.csv               -> 2024-10

    # Month name + year (e.g., _Oct2024)
    m = re.search(r'_(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*[_-]?(\d{4})', fname, re.I)
    if m:
        month_map = {
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
            'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
        }
        mm = month_map[m.group(1).lower()[:3]]
        yyyy = m.group(2)
        return f"{yyyy}-{mm}"

    # e.g., _2024-10 or _2024_10
    m = re.search(r'_(20\d{2})[_-]?(0[1-9]|1[0-2])', fname)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    # e.g., _202410
    m = re.search(r'_(20\d{2})(0[1-9]|1[0-2])', fname)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    # e.g., _20241027
    m = re.search(r'_(20\d{2})(0[1-9]|1[0-2])\d{2}', fname)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    return "ALL"




def main():
    master_path = os.path.join(RAW_DIR, MASTER_NAME)
    if not os.path.exists(master_path):
        raise FileNotFoundError(
            f"Expected master file at {master_path}. "
            f"Either rename your primary CSV to {MASTER_NAME} or update MASTER_NAME in this file."
        )

    # ---------- Load CSV (strings first to avoid mixed types) ----------
    df = pd.read_csv(master_path, dtype=str)

    # ---------- Canonicalize headers (CMS → standard names) ----------
    ALIASES = {
        # IDs / names / state
        "provider id": "PROVNUM",
        "provider number": "PROVNUM",
        "federal provider number": "PROVNUM",
        "cms certification number (ccn)": "PROVNUM",
        "ccn": "PROVNUM",
        "provider ccn": "PROVNUM",
        "ccn number": "PROVNUM",

        "provider name": "PROVNAME",
        "facility name": "PROVNAME",

        "provider state": "STATE",
        "state": "STATE",

        # Dates
        "processing date": "PROCESSING_DATE",

        # HPRD (reported)
        "reported rn staffing hours per resident per day":  "RN_HPRD",
        "reported lpn staffing hours per resident per day": "LPN_HPRD",
        "reported nurse aide staffing hours per resident per day": "CNA_HPRD",

        # HPRD (case-mix adjusted)
        "case-mix rn staffing hours per resident per day":  "RN_HPRD_ADJ",
        "case-mix lpn staffing hours per resident per day": "LPN_HPRD_ADJ",
        "case-mix nurse aide staffing hours per resident per day": "CNA_HPRD_ADJ",
    }

    # lower headers for matching → rename → keep canonical names as-is
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={c: ALIASES[c] for c in df.columns if c in ALIASES})

    # ---------- Ensure required keys exist ----------
    if "PROVNUM" not in df.columns:
        df["PROVNUM"] = df.reset_index().index.astype(str).str.zfill(6)
    if "PROVNAME" not in df.columns:
        df["PROVNAME"] = "UNKNOWN"
    if "STATE" not in df.columns:
        df["STATE"] = "UNK"

    # Clean formats
    df["PROVNUM"] = df["PROVNUM"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    df["PROVNAME"] = df["PROVNAME"].astype(str).str.strip()
    df["STATE"]    = df["STATE"].astype(str).str.upper().str.strip()

    # ---------- Derive month ----------
    # Prefer PROCESSING_DATE if present; else infer from filename
    fname = os.path.basename(master_path)
    if "PROCESSING_DATE" in df.columns:
        df["PROCESSING_DATE"] = pd.to_datetime(df["PROCESSING_DATE"], errors="coerce")
        df["month"] = df["PROCESSING_DATE"].dt.to_period("M").astype(str)
        # If all NaT, fallback to filename
        if df["month"].isna().all():
            df["month"] = month_from_filename(fname)
    else:
        df["month"] = month_from_filename(fname)
    
    # --- Ensure month is not all NULL or 'ALL' ---
    if df["month"].isna().all() or (set(df["month"].dropna().unique()) == {"ALL"}):
     df["month"] = "2024-10"  # manually assign if it's a single snapshot dataset


    # ---------- Convert HPRD columns to numeric if present ----------
    for col in ["RN_HPRD", "LPN_HPRD", "CNA_HPRD", "RN_HPRD_ADJ", "LPN_HPRD_ADJ", "CNA_HPRD_ADJ"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Build TOTAL_HPRD: prefer reported; else case-mix adjusted
    df["TOTAL_HPRD_REPORTED"] = (
        (df["RN_HPRD"]  if "RN_HPRD"  in df.columns else 0) +
        (df["LPN_HPRD"] if "LPN_HPRD" in df.columns else 0) +
        (df["CNA_HPRD"] if "CNA_HPRD" in df.columns else 0)
    )
    df["TOTAL_HPRD_ADJ"] = (
        (df["RN_HPRD_ADJ"]  if "RN_HPRD_ADJ"  in df.columns else 0) +
        (df["LPN_HPRD_ADJ"] if "LPN_HPRD_ADJ" in df.columns else 0) +
        (df["CNA_HPRD_ADJ"] if "CNA_HPRD_ADJ" in df.columns else 0)
    )

    # choose reported if present on the row; else adjusted
    df["TOTAL_HPRD"] = np.where(
        pd.notna(df.get("RN_HPRD", np.nan)) |
        pd.notna(df.get("LPN_HPRD", np.nan)) |
        pd.notna(df.get("CNA_HPRD", np.nan)),
        df["TOTAL_HPRD_REPORTED"],
        df["TOTAL_HPRD_ADJ"]
    )

    # ---------- Write curated parquet ----------
    out_path = os.path.join(CUR_DIR, "nursing_data.parquet")
    df.to_parquet(out_path, index=False)
    print(f"Wrote {out_path} with {len(df):,} rows.")
    # Print a quick peek so you can confirm columns
    preview_cols = [c for c in ["PROVNUM", "PROVNAME", "STATE", "month",
                                "RN_HPRD", "LPN_HPRD", "CNA_HPRD", "TOTAL_HPRD"] if c in df.columns]
    print("Columns preview:", preview_cols)
    print(df[preview_cols].head(min(5, len(df))))

if __name__ == "__main__":
    main()
