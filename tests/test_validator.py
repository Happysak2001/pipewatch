import pytest
from datetime import datetime, timedelta
from src.validator import (
    check_freshness,
    check_row_count,
    check_schema,
    check_null_rate,
    check_failed_records,
    score_run,
)


# ── Freshness ─────────────────────────────────────────────────────────────────

def test_freshness_pass():
    now = datetime.now()
    run_time = now - timedelta(hours=2)
    result = check_freshness(run_time, now)
    assert result["result"] == "PASS"

def test_freshness_warn():
    now = datetime.now()
    run_time = now - timedelta(hours=10)
    result = check_freshness(run_time, now)
    assert result["result"] == "WARN"

def test_freshness_fail():
    now = datetime.now()
    run_time = now - timedelta(hours=25)
    result = check_freshness(run_time, now)
    assert result["result"] == "FAIL"


# ── Row Count ─────────────────────────────────────────────────────────────────

def test_row_count_pass():
    result = check_row_count(current_rows=48000, previous_rows=50000)
    assert result["result"] == "PASS"

def test_row_count_warn():
    result = check_row_count(current_rows=43000, previous_rows=50000)
    assert result["result"] == "WARN"

def test_row_count_fail():
    result = check_row_count(current_rows=10000, previous_rows=50000)
    assert result["result"] == "FAIL"

def test_row_count_no_previous():
    result = check_row_count(current_rows=50000, previous_rows=0)
    assert result["result"] == "PASS"


# ── Schema ────────────────────────────────────────────────────────────────────

def test_schema_pass():
    result = check_schema("kafka_clinical_ingestion", "v3")
    assert result["result"] == "PASS"

def test_schema_fail_unknown_version():
    result = check_schema("kafka_clinical_ingestion", "v99")
    assert result["result"] == "FAIL"

def test_schema_fail_unknown_pipeline():
    # unknown pipeline has no recognized schemas, so any version fails
    result = check_schema("ghost_pipeline", "v1")
    assert result["result"] == "FAIL"


# ── Null Rate ─────────────────────────────────────────────────────────────────

def test_null_rate_pass():
    result = check_null_rate(null_count=100, row_count=10000)
    assert result["result"] == "PASS"

def test_null_rate_warn():
    result = check_null_rate(null_count=700, row_count=10000)
    assert result["result"] == "WARN"

def test_null_rate_fail():
    result = check_null_rate(null_count=2000, row_count=10000)
    assert result["result"] == "FAIL"

def test_null_rate_zero_rows():
    result = check_null_rate(null_count=0, row_count=0)
    assert result["result"] == "FAIL"


# ── Failed Records ────────────────────────────────────────────────────────────

def test_failed_records_pass():
    result = check_failed_records(failed_records=10, row_count=10000)
    assert result["result"] == "PASS"

def test_failed_records_warn():
    result = check_failed_records(failed_records=150, row_count=10000)
    assert result["result"] == "WARN"

def test_failed_records_fail():
    result = check_failed_records(failed_records=2000, row_count=10000)
    assert result["result"] == "FAIL"

def test_failed_records_zero_rows():
    result = check_failed_records(failed_records=0, row_count=0)
    assert result["result"] == "FAIL"


# ── Health Score ──────────────────────────────────────────────────────────────

def test_score_all_pass():
    checks = [
        {"check": "freshness",       "result": "PASS"},
        {"check": "row_count",       "result": "PASS"},
        {"check": "schema_drift",    "result": "PASS"},
        {"check": "null_rate",       "result": "PASS"},
        {"check": "failed_records",  "result": "PASS"},
    ]
    assert score_run(checks) == 100

def test_score_one_warn():
    checks = [
        {"check": "freshness",       "result": "WARN"},
        {"check": "row_count",       "result": "PASS"},
        {"check": "schema_drift",    "result": "PASS"},
        {"check": "null_rate",       "result": "PASS"},
        {"check": "failed_records",  "result": "PASS"},
    ]
    assert score_run(checks) == 90

def test_score_one_fail():
    checks = [
        {"check": "freshness",       "result": "FAIL"},
        {"check": "row_count",       "result": "PASS"},
        {"check": "schema_drift",    "result": "PASS"},
        {"check": "null_rate",       "result": "PASS"},
        {"check": "failed_records",  "result": "PASS"},
    ]
    assert score_run(checks) == 75

def test_score_cannot_go_below_zero():
    checks = [{"check": f"check_{i}", "result": "FAIL"} for i in range(10)]
    assert score_run(checks) == 0
