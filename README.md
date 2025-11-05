# Healthcare Metrics (Free Stack)

An end-to-end **Data Engineering + Analytics** project built entirely with **free tools**:
- **Google Drive → Parquet (local data lake)**
- **DuckDB** for SQL analytics over Parquet (no server)
- **Pandas** for light transforms
- **Streamlit** for interactive dashboard (deploy on Streamlit Community Cloud)
- **GitHub Actions** (optional) for nightly runs

## Project Layout
```
healthcare-metrics/
├─ data/
│  ├─ raw/                # downloads from Drive (CSV)
│  └─ curated/            # cleaned Parquet
├─ etl/
│  ├─ 00_download_from_drive.py
│  ├─ 01_validate_and_profile.py
│  ├─ 02_transform_to_parquet.py
│  └─ 03_build_marts_duckdb.py
├─ marts/                 # metric outputs (Parquet/CSV)
├─ dashboard/
│  └─ app.py              # Streamlit
├─ sql/
│  └─ metrics.sql         # reusable SQL for DuckDB
├─ docs/
│  ├─ architecture.mmd    # Mermaid diagram (edit in VS Code or Mermaid Live)
│  └─ solution_design.md
├─ .github/workflows/
│  └─ nightly.yml         # optional scheduler
├─ requirements.txt
└─ .env.example
```

## Quick Start (Local)
1. **Clone** this repo and `cd healthcare-metrics`
2. Create and activate a virtual env (optional)
3. Install deps: `pip install -r requirements.txt`
4. Copy `.env.example` to `.env` and confirm `GDRIVE_FOLDER_ID` matches your folder.
5. Run ETL:
   ```bash
   python etl/00_download_from_drive.py
   python etl/02_transform_to_parquet.py
   python etl/03_build_marts_duckdb.py
   ```
6. Launch the dashboard:
   ```bash
   streamlit run dashboard/app.py
   ```

## Notes
- The project expects the master file to be named **Nursing_Data.csv** inside your Drive folder (plus any supporting CSVs). All files are downloaded and the master file is processed for metrics.
- Which metrics are included out-of-the-box?
  - Total nurse hours by hospital/state/month
  - Overtime % (using *_ctr columns as proxy for overtime)
  - Nurse hours per patient (hours / MDScensus) as a ratio proxy
  - Top 10 overtime hospitals
- Extend `sql/metrics.sql` to add more metrics as you obtain more data (ALOS, readmissions, costs, etc.).

## Why These Tools?
- **DuckDB + Parquet**: Columnar, fast, zero infra; similar to Redshift/Athena workflows.
- **gdown**: Simple, free download from a shared Google Drive folder.
- **Streamlit**: Free dashboard hosting through Streamlit Community Cloud.
- **GitHub Actions**: Free scheduling for personal projects.

## Deploy to Streamlit Cloud
- Push this repo to GitHub.
- Go to share.streamlit.io → New App → select your repo and branch → set `dashboard/app.py` as the entrypoint.
- Ensure your repo includes the `marts/` and `data/curated/` outputs or adjust the app to run ETL on startup.

## License
MIT
