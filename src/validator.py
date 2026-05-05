import pandas as pd
from datetime import datetime, timedelta

# Thresholds — tunable per business requirement
THRESHOLDS = {
    "max_delay_minutes":     30,    # pipeline is late if delayed more than 30 min
    "row_drop_warn_pct":     0.10,  # warn if row count drops more than 10%
    "row_drop_fail_pct":     0.25,  # fail if row count drops more than 25%
    "null_rate_warn":        0.05,  # warn if nulls > 5% of rows
    "null_rate_fail":        0.15,  # fail if nulls > 15% of rows
    "failed_record_warn":    0.01,  # warn if failed records > 1%
    "failed_record_fail":    0.10,  # fail if failed records > 10%
    "freshness_warn_hours":  6,     # warn if data is older than 6 hours
    "freshness_fail_hours":  24,    # fail if data is older than 24 hours
}

KNOWN_SCHEMA_VERSIONS = {
    "orders_etl":      ["v1", "v2", "v3"],
    "user_events":     ["v1", "v2"],
    "inventory_sync":  ["v1"],
    "payments_etl":    ["v1", "v2", "v3"],
    "clickstream":     ["v1", "v2"],
}


def check_freshness(run_time: datetime, now: datetime = None) -> dict:
    """Is the data recent enough?"""
    if now is None:
        now = datetime.now()

    age_hours = (now - run_time).total_seconds() / 3600

    if age_hours > THRESHOLDS["freshness_fail_hours"]:
        result, message = "FAIL", f"Data is {age_hours:.1f}h old — exceeds 24h limit"
    elif age_hours > THRESHOLDS["freshness_warn_hours"]:
        result, message = "WARN", f"Data is {age_hours:.1f}h old — exceeds 6h warning"
    else:
        result, message = "PASS", f"Data is {age_hours:.1f}h old — fresh"

    return {"check": "freshness", "result": result, "message": message}


def check_row_count(current_rows: int, previous_rows: int) -> dict:
    """Did row count suddenly drop compared to the previous run?"""
    if previous_rows == 0:
        return {"check": "row_count", "result": "PASS", "message": "No previous run to compare"}

    drop_pct = (previous_rows - current_rows) / previous_rows

    if drop_pct > THRESHOLDS["row_drop_fail_pct"]:
        result = "FAIL"
        message = f"Row count dropped {drop_pct:.1%} — exceeds 25% threshold (prev: {previous_rows}, curr: {current_rows})"
    elif drop_pct > THRESHOLDS["row_drop_warn_pct"]:
        result = "WARN"
        message = f"Row count dropped {drop_pct:.1%} — exceeds 10% warning (prev: {previous_rows}, curr: {current_rows})"
    elif drop_pct < -0.5:
        result = "WARN"
        message = f"Row count spiked {abs(drop_pct):.1%} — unusually high volume"
    else:
        result = "PASS"
        message = f"Row count stable — {current_rows} rows ({drop_pct:+.1%} vs previous)"

    return {"check": "row_count", "result": result, "message": message}


def check_schema(pipeline_name: str, schema_version: str) -> dict:
    """Is the schema version one we recognize?"""
    known = KNOWN_SCHEMA_VERSIONS.get(pipeline_name, [])

    if schema_version not in known:
        result = "FAIL"
        message = f"Unknown schema version '{schema_version}' — expected one of {known}"
    else:
        result = "PASS"
        message = f"Schema version '{schema_version}' is recognized"

    return {"check": "schema_drift", "result": result, "message": message}


def check_null_rate(null_count: int, row_count: int) -> dict:
    """Are there too many null/missing values?"""
    if row_count == 0:
        return {"check": "null_rate", "result": "FAIL", "message": "Row count is zero — cannot compute null rate"}

    null_rate = null_count / row_count

    if null_rate > THRESHOLDS["null_rate_fail"]:
        result = "FAIL"
        message = f"Null rate is {null_rate:.1%} — exceeds 15% threshold"
    elif null_rate > THRESHOLDS["null_rate_warn"]:
        result = "WARN"
        message = f"Null rate is {null_rate:.1%} — exceeds 5% warning"
    else:
        result = "PASS"
        message = f"Null rate is {null_rate:.1%} — within acceptable range"

    return {"check": "null_rate", "result": result, "message": message}


def check_failed_records(failed_records: int, row_count: int) -> dict:
    """Did too many records fail to process?"""
    if row_count == 0:
        return {"check": "failed_records", "result": "FAIL", "message": "Row count is zero"}

    fail_rate = failed_records / row_count

    if fail_rate > THRESHOLDS["failed_record_fail"]:
        result = "FAIL"
        message = f"Failed record rate is {fail_rate:.1%} — exceeds 10% threshold"
    elif fail_rate > THRESHOLDS["failed_record_warn"]:
        result = "WARN"
        message = f"Failed record rate is {fail_rate:.1%} — exceeds 1% warning"
    else:
        result = "PASS"
        message = f"Failed record rate is {fail_rate:.1%} — within acceptable range"

    return {"check": "failed_records", "result": result, "message": message}


def score_run(check_results: list) -> int:
    """Convert check results into a 0–100 health score."""
    score = 100
    deductions = {"WARN": 10, "FAIL": 25}
    for check in check_results:
        score -= deductions.get(check["result"], 0)
    return max(0, score)


def validate_run(run: pd.Series, previous_run: pd.Series = None) -> dict:
    """Run all 5 checks on a single pipeline run. Returns a validation report."""
    now = datetime.now()
    run_time = pd.to_datetime(run["run_time"])
    previous_rows = int(previous_run["row_count"]) if previous_run is not None else 0

    checks = [
        check_freshness(run_time, now),
        check_row_count(int(run["row_count"]), previous_rows),
        check_schema(run["pipeline_name"], run["schema_version"]),
        check_null_rate(int(run["null_count"]), int(run["row_count"])),
        check_failed_records(int(run["failed_records"]), int(run["row_count"])),
    ]

    health_score = score_run(checks)
    overall = "PASS" if health_score >= 75 else ("WARN" if health_score >= 50 else "FAIL")

    return {
        "pipeline_name":  run["pipeline_name"],
        "run_time":       run["run_time"],
        "health_score":   health_score,
        "overall":        overall,
        "checks":         checks,
    }


def validate_all(df: pd.DataFrame) -> list:
    """Validate every run in the dataset. Each pipeline is compared to its own previous run."""
    results = []

    for pipeline in df["pipeline_name"].unique():
        pipeline_runs = df[df["pipeline_name"] == pipeline].sort_values("run_time").reset_index(drop=True)

        for i, row in pipeline_runs.iterrows():
            previous = pipeline_runs.iloc[i - 1] if i > 0 else None
            report = validate_run(row, previous)
            results.append(report)

    return results


if __name__ == "__main__":
    df = pd.read_csv("data/pipeline_runs.csv")
    df["run_time"] = pd.to_datetime(df["run_time"])

    results = validate_all(df)

    print(f"Validated {len(results)} pipeline runs\n")
    print("=" * 60)

    # Show only unhealthy runs
    unhealthy = [r for r in results if r["overall"] != "PASS"]
    print(f"Unhealthy runs: {len(unhealthy)} out of {len(results)}\n")

    for r in unhealthy[:5]:
        print(f"Pipeline : {r['pipeline_name']}")
        print(f"Run Time : {r['run_time']}")
        print(f"Health   : {r['health_score']}/100  [{r['overall']}]")
        for check in r["checks"]:
            if check["result"] != "PASS":
                print(f"  [{check['result']}] {check['check']}: {check['message']}")
        print("-" * 60)
