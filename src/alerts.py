import pandas as pd
import json
from datetime import datetime
from src.store import get_all_results, get_connection

ALERT_RULES = [
    {
        "rule_id":   "ALERT-001",
        "name":      "Pipeline Health Critical",
        "condition": lambda score, overall: overall == "FAIL",
        "severity":  "CRITICAL",
        "message":   "Pipeline health dropped to FAIL. Immediate investigation required.",
    },
    {
        "rule_id":   "ALERT-002",
        "name":      "Pipeline Health Degraded",
        "condition": lambda score, overall: overall == "WARN",
        "severity":  "WARNING",
        "message":   "Pipeline health degraded. Monitor closely.",
    },
    {
        "rule_id":   "ALERT-003",
        "name":      "Critical Health Score",
        "condition": lambda score, overall: score < 40,
        "severity":  "CRITICAL",
        "message":   "Health score below 40. Multiple checks failing simultaneously.",
    },
    {
        "rule_id":   "ALERT-004",
        "name":      "Schema Drift Detected",
        "condition": lambda score, overall, checks: any(
            c["check"] == "schema_drift" and c["result"] == "FAIL" for c in checks
        ),
        "severity":  "CRITICAL",
        "message":   "Schema drift detected. Downstream consumers may be broken.",
    },
    {
        "rule_id":   "ALERT-005",
        "name":      "Data Freshness Failure",
        "condition": lambda score, overall, checks: any(
            c["check"] == "freshness" and c["result"] == "FAIL" for c in checks
        ),
        "severity":  "WARNING",
        "message":   "Data is stale. Pipeline may not have run on schedule.",
    },
]


def evaluate_alerts(result: dict) -> list:
    """Evaluate all alert rules against one validation result. Returns triggered alerts."""
    triggered = []
    score   = result["health_score"]
    overall = result["overall"]
    checks  = result["checks"]

    for rule in ALERT_RULES:
        try:
            # Rules 1-3 only need score + overall; rules 4-5 also need checks
            if rule["rule_id"] in ("ALERT-004", "ALERT-005"):
                fired = rule["condition"](score, overall, checks)
            else:
                fired = rule["condition"](score, overall)
        except Exception:
            fired = False

        if fired:
            triggered.append({
                "rule_id":       rule["rule_id"],
                "rule_name":     rule["name"],
                "severity":      rule["severity"],
                "pipeline_name": result["pipeline_name"],
                "run_time":      str(result["run_time"]),
                "health_score":  score,
                "message":       rule["message"],
                "triggered_at":  datetime.now().isoformat(),
            })

    return triggered


def run_alert_engine(results: list) -> list:
    """Run alert evaluation across all validation results."""
    all_alerts = []
    for result in results:
        alerts = evaluate_alerts(result)
        all_alerts.extend(alerts)
    return all_alerts


def save_alerts(alerts: list):
    """Persist alerts to the database."""
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id       TEXT,
            rule_name     TEXT,
            severity      TEXT,
            pipeline_name TEXT,
            run_time      TEXT,
            health_score  INTEGER,
            message       TEXT,
            triggered_at  TEXT
        )
    """)
    rows = [(
        a["rule_id"], a["rule_name"], a["severity"],
        a["pipeline_name"], a["run_time"], a["health_score"],
        a["message"], a["triggered_at"]
    ) for a in alerts]

    conn.executemany("""
        INSERT INTO alerts
            (rule_id, rule_name, severity, pipeline_name, run_time, health_score, message, triggered_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()


def get_alert_summary() -> pd.DataFrame:
    """Summary of alerts grouped by pipeline and severity."""
    conn = get_connection()
    df = pd.read_sql("""
        SELECT
            pipeline_name,
            severity,
            COUNT(*) AS alert_count
        FROM alerts
        GROUP BY pipeline_name, severity
        ORDER BY pipeline_name, severity
    """, conn)
    conn.close()
    return df


if __name__ == "__main__":
    import sys
    sys.path.append(".")
    from src.validator import validate_all
    import pandas as pd

    df = pd.read_csv("data/pipeline_runs.csv")
    df["run_time"] = pd.to_datetime(df["run_time"])

    results = validate_all(df)
    alerts  = run_alert_engine(results)
    save_alerts(alerts)

    print(f"Total alerts triggered: {len(alerts)}")
    print()

    critical = [a for a in alerts if a["severity"] == "CRITICAL"]
    warnings = [a for a in alerts if a["severity"] == "WARNING"]
    print(f"  CRITICAL : {len(critical)}")
    print(f"  WARNING  : {len(warnings)}")

    print("\nAlert Summary by Pipeline:")
    print("=" * 50)
    print(get_alert_summary().to_string(index=False))

    print("\nSample CRITICAL Alerts:")
    print("=" * 50)
    for a in critical[:3]:
        print(f"[{a['rule_id']}] {a['pipeline_name']} | {a['run_time']}")
        print(f"  Rule     : {a['rule_name']}")
        print(f"  Score    : {a['health_score']}/100")
        print(f"  Message  : {a['message']}")
        print()
