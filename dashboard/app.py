import streamlit as st
import pandas as pd
import numpy as np
import os

st.set_page_config(page_title="Healthcare Metrics", layout="wide")
st.title("Healthcare Staffing & Facility Insights")

def load_parquet(path):
    if not os.path.exists(path):
        st.warning(f"Missing file: {path}. Did you run the ETL steps?")
        return pd.DataFrame()
    return pd.read_parquet(path)

hours = load_parquet("marts/hours_by_org.parquet")
ot    = load_parquet("marts/overtime_pct.parquet")            # placeholder
ratio = load_parquet("marts/nurse_patient_ratio.parquet")     # placeholder
topot = load_parquet("marts/top_overtime.parquet")            # repurposed as top staffing

# ⛔ Old code stopped the app if ot/ratio were empty; we only need hours.
if hours.empty:
    st.stop()

states = sorted(hours["STATE"].dropna().unique().tolist())
state = st.sidebar.selectbox("State", ["ALL"] + states)

def apply_state(df):
    return df if state == "ALL" else df[df.STATE == state]

hrs_state  = apply_state(hours)
ot_state   = apply_state(ot) if not ot.empty else pd.DataFrame()
ratio_state= apply_state(ratio) if not ratio.empty else pd.DataFrame()

# ---------- NEW KPI ROW (HPRD-based) ----------
c1, c2, c3, c4 = st.columns(4)

# Facilities
c1.metric("Facilities", f"{hrs_state['PROVNUM'].nunique():,}")

# Avg Staffing (HPRD)
avg_hprd = float(hrs_state["total_hours"].mean()) if not hrs_state.empty else 0.0
c2.metric("Avg Staffing (HPRD)", f"{avg_hprd:.2f}")

# Avg Nurse Hrs/Patient (only if you actually have it; else fallback)
if not ratio_state.empty and "nurse_hours_per_patient" in ratio_state:
    nhpp = ratio_state["nurse_hours_per_patient"].astype(float)
    nhpp = nhpp[np.isfinite(nhpp)]
    c3.metric("Avg Nurse Hrs/Patient", f"{(nhpp.mean() if len(nhpp) else 0):.2f}")
else:
    c3.metric("States in View", f"{hrs_state['STATE'].nunique():,}")

# Avg Overtime % (show only if real, else N/A or replace with another KPI)
if not ot_state.empty and "overtime_pct" in ot_state:
    otpct = pd.to_numeric(ot_state["overtime_pct"], errors="coerce")
    otpct = otpct[np.isfinite(otpct)]
    if len(otpct):
        c4.metric("Avg Overtime %", f"{otpct.mean():.1%}")
    else:
        c4.metric("Avg Overtime %", "N/A")
else:
    # Replace with a useful fallback if you prefer:
    # c4.metric("Months in View", f"{hrs_state['month'].nunique():,}")
    c4.metric("Avg Overtime %", "N/A")

# ---------- TRENDS ----------
st.subheader("Average Staffing (HPRD) by Month")

def build_trend(df):
    # Guard clauses
    if df is None or df.empty:
        return pd.DataFrame(columns=["month","avg_hprd"])
    if "month" not in df.columns or "total_hours" not in df.columns:
        return pd.DataFrame(columns=["month","avg_hprd"])

    # Clean month values
    t = df.loc[df["month"].notna(), ["month","total_hours"]].copy()
    # Drop rows with placeholder month
    t = t[t["month"].astype(str).str.upper() != "ALL"]

    if t.empty:
        return pd.DataFrame(columns=["month","avg_hprd"])

    # Make month sortable like YYYY-MM
    t["month"] = t["month"].astype(str)
    # Aggregate and sort
    t = (t.groupby("month", as_index=False)["total_hours"]
           .mean()
           .rename(columns={"total_hours":"avg_hprd"})
           .sort_values("month"))
    return t

trend = build_trend(hrs_state)

if trend.empty:
    # Graceful fallback if there is only one month (or none):
    if not hrs_state.empty:
        # show a single KPI instead of an empty chart
        single = float(hrs_state["total_hours"].mean())
        st.metric("Avg Staffing (HPRD) — Current View", f"{single:.2f}")
        with st.expander("Why no line chart?"):
            st.write("No multiple month values found in the current filter. "
                     "Either the dataset is a single-month snapshot or month could not be inferred.")
    else:
        st.info("No data for the current filters.")
else:
    st.line_chart(trend.set_index("month")["avg_hprd"])


# ---------- LEADERBOARD ----------
st.subheader("Top Providers by Staffing (HPRD)")
if not topot.empty and "avg_total_hprd" in topot.columns:
    T = apply_state(topot.rename(columns={"avg_total_hprd":"HPRD"}))
    T = T.sort_values("HPRD", ascending=False).head(10)
    st.dataframe(T[["PROVNAME","STATE","HPRD"]])
else:
    T = hrs_state.groupby(["PROVNUM","PROVNAME","STATE"], as_index=False)["total_hours"].mean()
    T = T.rename(columns={"total_hours":"HPRD"}).sort_values("HPRD", ascending=False).head(10)
    st.dataframe(T[["PROVNAME","STATE","HPRD"]])

st.caption("HPRD = Hours Per Resident Per Day. Overtime and nurse-hours-per-patient are placeholders unless sourced.")
