PRAGMA threads=4;

-- Load curated data once
CREATE OR REPLACE TABLE base AS
SELECT
  COALESCE(PROVNUM, CAST(row_number() OVER () AS VARCHAR)) AS PROVNUM,
  COALESCE(PROVNAME, 'UNKNOWN') AS PROVNAME,
  COALESCE(STATE, 'UNK') AS STATE,
  COALESCE(month, 'ALL') AS month,
  CAST(COALESCE(TOTAL_HPRD, 0) AS DOUBLE) AS TOTAL_HPRD,
  CAST(COALESCE(RN_HPRD,  0) AS DOUBLE) AS RN_HPRD,
  CAST(COALESCE(LPN_HPRD, 0) AS DOUBLE) AS LPN_HPRD,
  CAST(COALESCE(CNA_HPRD, 0) AS DOUBLE) AS CNA_HPRD
FROM read_parquet('data/curated/nursing_data.parquet');

-- 1) Staffing intensity by org (HPRD)
CREATE OR REPLACE TABLE hours_by_org AS
SELECT
  PROVNUM, PROVNAME, STATE, month,
  AVG(TOTAL_HPRD) AS total_hours   -- keep name so dashboard code still works
FROM base
GROUP BY 1,2,3,4;

-- 2) Role mix (RN/LPN/CNA shares)
CREATE OR REPLACE TABLE role_mix AS
SELECT
  PROVNUM, PROVNAME, STATE, month,
  AVG(RN_HPRD)  AS rn_hprd,
  AVG(LPN_HPRD) AS lpn_hprd,
  AVG(CNA_HPRD) AS cna_hprd,
  AVG(TOTAL_HPRD) AS total_hprd
FROM base
GROUP BY 1,2,3,4;

-- 3) Top providers by staffing intensity
CREATE OR REPLACE TABLE top_staffing AS
SELECT
  PROVNUM, PROVNAME, STATE,
  AVG(TOTAL_HPRD) AS avg_total_hprd
FROM base
GROUP BY 1,2,3
ORDER BY avg_total_hprd DESC
LIMIT 10;

-- Placeholders for existing dashboard filenames
CREATE OR REPLACE TABLE overtime_pct AS
SELECT
  PROVNUM, PROVNAME, STATE, month,
  CAST(0 AS DOUBLE) AS overtime_hours,
  CAST(NULL AS DOUBLE) AS base_hours,
  CAST(NULL AS DOUBLE) AS overtime_pct
FROM base;

CREATE OR REPLACE TABLE nurse_patient_ratio AS
SELECT
  PROVNUM, PROVNAME, STATE, month,
  AVG(TOTAL_HPRD) AS nurse_hours,   -- HPRD proxy
  CAST(NULL AS DOUBLE) AS census,
  CAST(NULL AS DOUBLE) AS nurse_hours_per_patient
FROM base
GROUP BY 1,2,3,4;

-- Export marts (no subqueries inside COPY)
COPY hours_by_org        TO 'marts/hours_by_org.parquet'        (FORMAT 'parquet');
COPY overtime_pct        TO 'marts/overtime_pct.parquet'        (FORMAT 'parquet');
COPY nurse_patient_ratio TO 'marts/nurse_patient_ratio.parquet' (FORMAT 'parquet');
COPY (
  SELECT PROVNUM, PROVNAME, STATE, NULL AS avg_overtime_pct, avg_total_hprd
  FROM top_staffing
) TO 'marts/top_overtime.parquet' (FORMAT 'parquet');
