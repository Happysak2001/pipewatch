import pandas as pd
import random
from datetime import datetime, timedelta

PIPELINES = [
    "kafka_clinical_ingestion",
    "databricks_sales_aggregation",
    "inventory_sync_pipeline",
    "payments_etl_pipeline",
    "clickstream_events_pipeline",
]

SCHEMA_VERSIONS = {
    "kafka_clinical_ingestion":      ["v1", "v2", "v3"],
    "databricks_sales_aggregation":  ["v1", "v2"],
    "inventory_sync_pipeline":       ["v1"],
    "payments_etl_pipeline":         ["v1", "v2", "v3"],
    "clickstream_events_pipeline":   ["v1", "v2"],
}

BASELINE_ROWS = {
    "kafka_clinical_ingestion":      50000,
    "databricks_sales_aggregation":  200000,
    "inventory_sync_pipeline":       15000,
    "payments_etl_pipeline":         30000,
    "clickstream_events_pipeline":   500000,
}


def generate_pipeline_run(pipeline_name: str, run_time: datetime, inject_fault: str = None) -> dict:
    """
    Generate one pipeline run record.
    inject_fault options: "row_drop", "schema_drift", "high_nulls", "failed_records", "stale", None
    """
    baseline = BASELINE_ROWS[pipeline_name]
    schema_versions = SCHEMA_VERSIONS[pipeline_name]

    # Defaults — healthy run
    row_count = int(random.gauss(baseline, baseline * 0.05))   # ±5% natural variation
    null_count = int(row_count * random.uniform(0.001, 0.02))  # 0.1%–2% nulls
    failed_records = int(row_count * random.uniform(0.0001, 0.005))  # <0.5% failures
    schema_version = schema_versions[-1]                        # latest schema
    status = "success"
    delay_minutes = random.randint(0, 5)                        # small natural delay

    # Inject faults for simulation
    if inject_fault == "row_drop":
        row_count = int(baseline * random.uniform(0.1, 0.4))   # drop to 10–40% of normal
    elif inject_fault == "schema_drift":
        schema_version = f"v{len(schema_versions) + 1}"        # unknown new version
    elif inject_fault == "high_nulls":
        null_count = int(row_count * random.uniform(0.25, 0.6))  # 25–60% nulls
    elif inject_fault == "failed_records":
        failed_records = int(row_count * random.uniform(0.15, 0.4))  # 15–40% failures
        status = "partial_failure"
    elif inject_fault == "stale":
        delay_minutes = random.randint(180, 480)                # 3–8 hour delay
        status = "late"

    records_processed = row_count - failed_records

    return {
        "pipeline_name":     pipeline_name,
        "run_time":          (run_time + timedelta(minutes=delay_minutes)).isoformat(),
        "scheduled_time":    run_time.isoformat(),
        "records_processed": records_processed,
        "failed_records":    failed_records,
        "schema_version":    schema_version,
        "row_count":         row_count,
        "null_count":        null_count,
        "status":            status,
    }


def generate_dataset(days: int = 30, fault_rate: float = 0.15) -> pd.DataFrame:
    """
    Generate `days` worth of daily pipeline runs for all pipelines.
    About `fault_rate` fraction of runs will have an injected fault.
    """
    faults = ["row_drop", "schema_drift", "high_nulls", "failed_records", "stale"]
    records = []

    base_date = datetime.now() - timedelta(days=days)

    for day in range(days):
        run_date = base_date + timedelta(days=day)
        scheduled_time = run_date.replace(hour=6, minute=0, second=0, microsecond=0)

        for pipeline in PIPELINES:
            fault = random.choice(faults) if random.random() < fault_rate else None
            record = generate_pipeline_run(pipeline, scheduled_time, inject_fault=fault)
            records.append(record)

    df = pd.DataFrame(records)
    df["run_time"] = pd.to_datetime(df["run_time"])
    df["scheduled_time"] = pd.to_datetime(df["scheduled_time"])
    df = df.sort_values("run_time").reset_index(drop=True)
    return df


if __name__ == "__main__":
    df = generate_dataset(days=30)
    output_path = "data/pipeline_runs.csv"
    df.to_csv(output_path, index=False)
    print(f"Generated {len(df)} pipeline run records -> {output_path}")
    print("\nSample:")
    print(df.head(10).to_string())
    print(f"\nStatus distribution:\n{df['status'].value_counts()}")
