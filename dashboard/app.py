import sys
import json
import sqlite3
import pandas as pd
import streamlit as st
from pathlib import Path
from datetime import datetime

sys.path.append(".")

DB_PATH = "data/observability.db"

st.set_page_config(
    page_title="PipeWatch",
    page_icon=".",
    layout="wide",
)


def bootstrap():
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


# ── Data loaders ──────────────────────────────────────────────────────────────

def load(query: str) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def load_summary() -> pd.DataFrame:
    df = load("""
        SELECT
            pipeline_name,
            COUNT(*)                                           AS total_runs,
            SUM(CASE WHEN overall = 'PASS' THEN 1 ELSE 0 END) AS passed,
            SUM(CASE WHEN overall = 'WARN' THEN 1 ELSE 0 END) AS warned,
            SUM(CASE WHEN overall = 'FAIL' THEN 1 ELSE 0 END) AS failed,
            ROUND(AVG(health_score), 1)                        AS avg_score,
            MAX(run_time)                                      AS last_run
        FROM validation_results
        GROUP BY pipeline_name
        ORDER BY avg_score ASC
    """)
    df["status"] = df["avg_score"].apply(
        lambda s: "✅ Healthy" if s >= 75 else ("⚠️ Degraded" if s >= 50 else "❌ Critical")
    )
    return df


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


# ── Sidebar — Guide ───────────────────────────────────────────────────────────

with st.sidebar:
    st.title("What is PipeWatch?")
    st.markdown("""
PipeWatch monitors data pipelines and tells you whether they are **healthy or failing**.

Think of it like a health checkup for your data — every time a pipeline runs, PipeWatch examines it and gives it a score.
    """)

    st.divider()
    st.subheader("The 5 Health Checks")

    with st.expander("Freshness — Is the data recent?"):
        st.markdown("""
Every pipeline is supposed to run at a scheduled time (e.g. 6am daily).

If the data that arrives is too old, something went wrong — the pipeline ran late, or didn't run at all.

- **PASS** — Data is less than 6 hours old
- **WARN** — Data is 6 to 24 hours old
- **FAIL** — Data is older than 24 hours
        """)

    with st.expander("Row Count — Did the volume drop?"):
        st.markdown("""
Each pipeline normally moves a certain number of records every day. For example, the orders pipeline usually brings 50,000 rows.

If today it only brought 5,000 rows — something broke upstream. Maybe the source database had an issue, or the pipeline partially failed.

- **PASS** — Row count is within 10% of yesterday
- **WARN** — Dropped 10% to 25%
- **FAIL** — Dropped more than 25%
        """)

    with st.expander("Schema Drift — Did the structure change?"):
        st.markdown("""
Data has a structure — column names, types, layout. This is called a schema.

If someone on another team renames a column or adds a new one without telling you, your pipeline breaks silently. Data starts coming in the wrong shape.

- **PASS** — Schema version is recognized
- **FAIL** — An unknown schema version appeared
        """)

    with st.expander("Null Rate — Are values missing?"):
        st.markdown("""
Nulls mean empty values — fields with no data. A small number of nulls is normal. But if 30% of a column is empty, something went wrong — maybe a join failed or a source field stopped populating.

- **PASS** — Less than 5% nulls
- **WARN** — 5% to 15% nulls
- **FAIL** — More than 15% nulls
        """)

    with st.expander("Failed Records — Did records error out?"):
        st.markdown("""
Some records fail to process — maybe they have invalid formats, missing required fields, or hit a transformation error. A small number is acceptable. A large spike means the pipeline is broken.

- **PASS** — Less than 1% failed
- **WARN** — 1% to 10% failed
- **FAIL** — More than 10% failed
        """)

    st.divider()
    st.subheader("Health Score")
    st.markdown("""
Each run gets a score from **0 to 100**:

- Each WARN deducts **10 points**
- Each FAIL deducts **25 points**

| Score | Status |
|-------|--------|
| 75-100 | PASS |
| 50-74 | WARN |
| 0-49 | FAIL |
    """)

    st.divider()
    st.subheader("The 5 Pipelines")
    st.markdown("""
| Pipeline | What it moves |
|----------|--------------|
| orders_etl | Customer orders |
| user_events | Clicks and actions |
| inventory_sync | Stock counts |
| payments_etl | Transactions |
| clickstream | Page visits |
    """)

    st.divider()
    st.caption("Built with Python, SQLite, Streamlit, Pytest and GitHub Actions")


# ── Main — Header ─────────────────────────────────────────────────────────────

st.title("PipeWatch")
st.caption("Live health monitoring across all pipelines")
st.divider()


# ── Simulate a Pipeline Run ───────────────────────────────────────────────────

st.subheader("Try It — Simulate a Pipeline Run")
st.markdown("Pick a pipeline and a fault type. See how PipeWatch detects the problem in real time.")

FAULT_LABELS = {
    "None (healthy run)":           None,
    "Row Drop — volume crashed":    "row_drop",
    "Schema Drift — unknown column":"schema_drift",
    "High Nulls — missing values":  "high_nulls",
    "Failed Records — errors spiked":"failed_records",
    "Stale Data — pipeline was late":"stale",
}

col_a, col_b, col_c = st.columns([2, 2, 1])
with col_a:
    sim_pipeline = st.selectbox("Pipeline", [
        "kafka_clinical_ingestion",
        "databricks_sales_aggregation",
        "inventory_sync_pipeline",
        "payments_etl_pipeline",
        "clickstream_events_pipeline",
    ])
with col_b:
    sim_fault_label = st.selectbox("Fault to inject", list(FAULT_LABELS.keys()))
with col_c:
    st.markdown("<br>", unsafe_allow_html=True)
    run_sim = st.button("Run Simulation", use_container_width=True)

if run_sim:
    from src.generator import generate_pipeline_run
    from src.validator import validate_run
    from src.store import get_connection, create_tables
    from src.alerts import evaluate_alerts

    fault = FAULT_LABELS[sim_fault_label]
    raw   = generate_pipeline_run(sim_pipeline, datetime.now(), inject_fault=fault)
    run_series = pd.Series(raw)
    report = validate_run(run_series)
    sim_alerts = evaluate_alerts(report)

    conn = get_connection()
    create_tables()
    conn.execute("""
        INSERT INTO validation_results (pipeline_name, run_time, health_score, overall, checks)
        VALUES (?, ?, ?, ?, ?)
    """, (
        report["pipeline_name"], str(report["run_time"]),
        report["health_score"], report["overall"],
        json.dumps(report["checks"])
    ))
    for a in sim_alerts:
        conn.execute("""
            INSERT INTO alerts (rule_id, rule_name, severity, pipeline_name, run_time, health_score, message, triggered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            a["rule_id"], a["rule_name"], a["severity"],
            a["pipeline_name"], a["run_time"], a["health_score"],
            a["message"], a["triggered_at"]
        ))
    conn.commit()
    conn.close()

    score   = report["health_score"]
    overall = report["overall"]
    status_label = "Healthy" if overall == "PASS" else ("Degraded" if overall == "WARN" else "Critical")
    status_icon  = "✅" if overall == "PASS" else ("⚠️" if overall == "WARN" else "❌")
    color   = "#d4edda" if overall == "PASS" else ("#fff3cd" if overall == "WARN" else "#f8d7da")

    st.markdown(f"""
    <div style="background:{color}; padding:16px 20px; border-radius:8px; margin-top:12px;">
        <h4 style="margin:0">{status_icon} {sim_pipeline} &nbsp;|&nbsp; {status_label} &nbsp;|&nbsp; Health Score: {score}/100</h4>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("**Why this result:**")
    check_icons = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}
    for check in report["checks"]:
        r      = check["result"]
        icon   = check_icons[r]
        color2 = "#d4edda" if r == "PASS" else ("#fff3cd" if r == "WARN" else "#f8d7da")
        st.markdown(f"""
        <div style="background:{color2}; padding:8px 14px; border-radius:6px; margin-top:6px; font-size:0.9em;">
            {icon} <b>{check['check']}</b> — {check['message']}
        </div>
        """, unsafe_allow_html=True)

    # Score breakdown
    warn_ct = sum(1 for c in report["checks"] if c["result"] == "WARN")
    fail_ct = sum(1 for c in report["checks"] if c["result"] == "FAIL")
    st.markdown(f"""
    **Score breakdown:** 100 base
    {"− " + str(warn_ct * 10) + " (" + str(warn_ct) + " warning/s × 10)" if warn_ct else ""}
    {"− " + str(fail_ct * 25) + " (" + str(fail_ct) + " failure/s × 25)" if fail_ct else ""}
    **= {score}/100**
    """)

    if sim_alerts:
        st.markdown("**Alerts fired:**")
        for a in sim_alerts:
            if a["severity"] == "CRITICAL":
                st.error(f"❌ [{a['severity']}] {a['rule_name']} — {a['message']}")
            else:
                st.warning(f"⚠️ [{a['severity']}] {a['rule_name']} — {a['message']}")

    st.success("Result saved. Dashboard below updated.")

st.divider()


# ── KPI Cards ─────────────────────────────────────────────────────────────────

summary = load_summary()
alerts  = load_alerts()

total_runs = int(summary["total_runs"].sum())
total_pass = int(summary["passed"].sum())
total_fail = int(summary["failed"].sum())
total_warn = int(summary["warned"].sum())
avg_score  = round(summary["avg_score"].mean(), 1)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Runs",       total_runs)
c2.metric("Passed",           total_pass)
c3.metric("Warnings",         total_warn)
c4.metric("Failed",           total_fail)
c5.metric("Avg Health Score", f"{avg_score}/100")

st.divider()


# ── Pipeline Health Summary ───────────────────────────────────────────────────

st.subheader("Pipeline Health Summary")
st.caption("Average health score per pipeline across all runs. Green = healthy, Yellow = degraded, Red = critical.")


def color_score(val):
    if val >= 75:
        return "background-color: #d4edda; color: #155724"
    elif val >= 50:
        return "background-color: #fff3cd; color: #856404"
    return "background-color: #f8d7da; color: #721c24"


display_summary = summary[["pipeline_name", "status", "avg_score", "total_runs", "passed", "warned", "failed", "last_run"]].copy()
display_summary.columns = ["Pipeline", "Status", "Avg Score", "Total Runs", "Passed", "Warned", "Failed", "Last Run"]
styled = display_summary.style.map(color_score, subset=["Avg Score"])
st.dataframe(styled, use_container_width=True, hide_index=True)

# Freshness section
st.subheader("Pipeline Freshness")
st.caption("When each pipeline last ran and how fresh the data is right now.")

now = datetime.now()
for _, row in summary.iterrows():
    try:
        last = pd.to_datetime(row["last_run"], format="mixed")
        age_h = (now - last).total_seconds() / 3600
        if age_h < 6:
            f_icon, f_label = "✅", f"Fresh — {age_h:.1f}h ago"
        elif age_h < 24:
            f_icon, f_label = "⚠️", f"Aging — {age_h:.1f}h ago"
        else:
            f_icon, f_label = "❌", f"Stale — {age_h:.1f}h ago"
    except Exception:
        f_icon, f_label = "?", "Unknown"

    st.markdown(f"""
    <div style="display:flex; justify-content:space-between; padding:8px 14px;
         border-radius:6px; margin-bottom:6px; background:#f8f9fa; font-size:0.9em;">
        <b>{row['pipeline_name']}</b>
        <span>Last run: {row['last_run']}</span>
        <span>{f_icon} {f_label}</span>
    </div>
    """, unsafe_allow_html=True)

st.divider()


# ── Health Score Trend ────────────────────────────────────────────────────────

st.subheader("Health Score Trend")
st.caption("How the health score of a pipeline changed day by day. A sudden drop means something broke.")

selected = st.selectbox("Select pipeline", summary["pipeline_name"].tolist())
trend = load_trend(selected)
trend["run_time"] = pd.to_datetime(trend["run_time"], format="mixed", dayfirst=False)
st.line_chart(trend.set_index("run_time")[["health_score"]], height=250)

st.divider()


# ── Check Failure Breakdown ───────────────────────────────────────────────────

st.subheader("Check Failure Breakdown")
st.caption("Which health checks are failing most often, per pipeline. Taller bar = more failures of that check type.")

breakdown  = load_check_breakdown()
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
st.caption("Every time a pipeline run breaks a rule, an alert is fired. CRITICAL = act now. WARNING = keep watching.")

severity_filter = st.radio("Filter by severity", ["ALL", "CRITICAL", "WARNING"], horizontal=True)
filtered = alerts if severity_filter == "ALL" else alerts[alerts["severity"] == severity_filter]

for _, row in filtered.head(15).iterrows():
    color = "#f8d7da" if row["severity"] == "CRITICAL" else "#fff3cd"
    label = row["severity"]
    st.markdown(f"""
    <div style="background:{color}; padding:10px 14px; border-radius:6px; margin-bottom:8px;">
        <b>[{label}]</b> &nbsp; <b>{row['pipeline_name']}</b>
        &nbsp;|&nbsp; {row['run_time']}
        &nbsp;|&nbsp; Score: {row['health_score']}/100<br>
        <span style="font-size:0.9em;">{row['rule_name']} — {row['message']}</span>
    </div>
    """, unsafe_allow_html=True)
