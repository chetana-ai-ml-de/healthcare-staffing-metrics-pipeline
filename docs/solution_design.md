# Solution Design

## Objective
Build a Healthcare Metrics analytics pipeline from CSVs to insights (dashboard) with a **free stack**.

## Architecture (Free Analog to AWS)
- **Source**: Google Drive shared folder (CSV)
- **Ingestion**: Python + `gdown`
- **Storage**: Local Parquet (data lake)
- **Transform**: Pandas for normalization; DuckDB SQL for marts
- **Serving**: Streamlit dashboard
- **Orchestration (optional)**: GitHub Actions nightly

## Data Model (Key Columns)
- `PROVNUM`, `PROVNAME`, `CITY`, `STATE`, `COUNTY_NAME`, `COUNTY_FIPS`
- `CY_Qtr`, `WorkDate`, `MDScensus`
- Hours columns: RN/LPN/CNA (emp/ctr/admin variants)

## Calculable Metrics (with given schema)
1. **Total Nurse Hours** by hospital/state/month
2. **Overtime %** = (ctr hours)/(total nurse hours)
3. **Nurse Hours per Patient** = total nurse hours / MDScensus
4. **Top Overtime Hospitals** (avg overtime %)

> Facility/Quality/Cost metrics requiring additional fields are stubbed for future integration.

## Data Quality
- Basic checks: row counts, missing cells, duplicates
- Type normalization: dates, numeric hours, state codes

## Assumptions & Limitations
- `*_ctr` approximates overtime. Adjust if explicit OT flags/rates become available.
- `MDScensus` used as patient census proxy for nurse-to-patient ratio.

## Future Enhancements
- Add payroll, admissions/discharges for ALOS/cost metrics
- Introduce dimensional model (dim_provider, dim_date, fact_staffing)
- Add tests (Great Expectations) and data contracts
- Cloud move: S3 + Glue + Redshift Serverless; Streamlit on EC2/ECS
