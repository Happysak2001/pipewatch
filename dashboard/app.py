import sys
import json
import sqlite3
import pandas as pd
import streamlit as st
from pathlib import Path

sys.path.append(".")

DB_PATH = "data/observability.db"

st.set_page_config(
    page_title="PipeWatch",
    page_icon=".",
    layout="wide",
)


def bootstrap():
    """Generate data and populate the database if it doesn't exist yet."""
    if Path(DB_PATH).exists():
        return
    from src.generator import generate_dataset
    from src.validator import validate_all
    from src.store import create_tables, save_pipeline_runs, save_validation_results
    from src.alerts import run_alert_engine, save_alerts

    Path("data").mkdir(exist_ok=True)
    df = generate_dataset(days=30)
    df.to_csv("data/pipeline_runs.csv", index=False)
    results = validate_all(df)
    create_tables()
    save_pipeline_runs(df)
    save_validation_results(results)
    alerts = run_alert_engine(results)
    save_alerts(alerts)


bootstrap()


# ── Data loaders ─────────────────────────────────────────────────────────────

def load(query: str) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def load_summary() -> pd.DataFrame:
    return load("""
        SELECT
            pipeline_name,
            COUNT(*)                                           AS total_runs,
            SUM(CASE WHEN overall = 'PASS' THEN 1 ELSE 0 END) AS passed,
            SUM(CASE WHEN overall = 'WARN' THEN 1 ELSE 0 END) AS warned,
            SUM(CASE WHEN overall = 'FAIL' THEN 1 ELSE 0 END) AS failed,
            ROUND(AVG(health_score), 1)                        AS avg_score
        FROM validation_results
        GROUP BY pipeline_name
        ORDER BY avg_score ASC
    """)


def load_trend(pipeline: str) -> pd.DataFrame:
    return load(f"""
        SELECT run_time, health_score, overall
        FROM validation_results
        WHERE pipeline_name = '{pipeline}'
        ORDER BY run_time ASC
    """)


def load_alerts() -> pd.DataFrame:
    return load("""
        SELECT pipeline_name, severity, rule_name, message, run_time, health_score
        FROM alerts
        ORDER BY run_time DESC
    """)


def load_check_breakdown() -> pd.DataFrame:
    rows = load("SELECT pipeline_name, checks FROM validation_results")
    records = []
    for _, r in rows.iterrows():
        for c in json.loads(r["checks"]):
            records.append({
                "pipeline_name": r["pipeline_name"],
                "check":         c["check"],
                "result":        c["result"],
            })
    return pd.DataFrame(records)


# ── Header ────────────────────────────────────────────────────────────────────

st.title("PipeWatch")
st.caption("Live health monitoring across all pipelines")
st.divider()


# ── Top KPI cards ─────────────────────────────────────────────────────────────

summary = load_summary()
alerts  = load_alerts()

total_runs  = int(summary["total_runs"].sum())
total_pass  = int(summary["passed"].sum())
total_fail  = int(summary["failed"].sum())
total_warn  = int(summary["warned"].sum())
avg_score   = round(summary["avg_score"].mean(), 1)
critical_ct = len(alerts[alerts["severity"] == "CRITICAL"])

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Runs",       total_runs)
c2.metric("Passed",           total_pass)
c3.metric("Warnings",         total_warn)
c4.metric("Failed",           total_fail)
c5.metric("Avg Health Score", f"{avg_score}/100")

st.divider()


# ── Pipeline Health Summary table ─────────────────────────────────────────────

st.subheader("Pipeline Health Summary")


def color_score(val):
    if val >= 75:
        return "background-color: #d4edda; color: #155724"
    elif val >= 50:
        return "background-color: #fff3cd; color: #856404"
    return "background-color: #f8d7da; color: #721c24"


def color_overall(val):
    colors = {"PASS": "#d4edda", "WARN": "#fff3cd", "FAIL": "#f8d7da"}
    return f"background-color: {colors.get(val, 'white')}"


styled = summary.style \
    .map(color_score, subset=["avg_score"])

st.dataframe(styled, use_container_width=True, hide_index=True)

st.divider()


# ── Health Score Trend ────────────────────────────────────────────────────────

st.subheader("Health Score Trend")

selected = st.selectbox("Select pipeline", summary["pipeline_name"].tolist())
trend    = load_trend(selected)
trend["run_time"] = pd.to_datetime(trend["run_time"])

st.line_chart(trend.set_index("run_time")[["health_score"]], height=250)

st.divider()


# ── Check Breakdown ───────────────────────────────────────────────────────────

st.subheader("Check Failure Breakdown")

breakdown = load_check_breakdown()
fail_counts = (
    breakdown[breakdown["result"] == "FAIL"]
    .groupby(["pipeline_name", "check"])
    .size()
    .reset_index(name="failures")
)

if not fail_counts.empty:
    pivot = fail_counts.pivot(index="pipeline_name", columns="check", values="failures").fillna(0)
    st.bar_chart(pivot, height=280)
else:
    st.info("No failures recorded.")

st.divider()


# ── Recent Alerts ─────────────────────────────────────────────────────────────

st.subheader("Recent Alerts")

severity_filter = st.radio("Filter by severity", ["ALL", "CRITICAL", "WARNING"], horizontal=True)

filtered = alerts if severity_filter == "ALL" else alerts[alerts["severity"] == severity_filter]

for _, row in filtered.head(15).iterrows():
    color = "#f8d7da" if row["severity"] == "CRITICAL" else "#fff3cd"
    label = "CRITICAL" if row["severity"] == "CRITICAL" else "WARNING"
    st.markdown(
        f"""
        <div style="background:{color}; padding:10px 14px; border-radius:6px; margin-bottom:8px;">
            <b>[{label}]</b> &nbsp; <b>{row['pipeline_name']}</b>
            &nbsp;|&nbsp; {row['run_time']}
            &nbsp;|&nbsp; Score: {row['health_score']}/100<br>
            <span style="font-size:0.9em;">{row['rule_name']} — {row['message']}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.divider()
st.caption("Built with Python, SQLite, and Streamlit")
