import sqlite3
import json
import pandas as pd
from pathlib import Path

DB_PATH = "data/observability.db"


def get_connection():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def create_tables():
    """Create the database tables if they don't exist."""
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_name    TEXT NOT NULL,
            run_time         TEXT NOT NULL,
            row_count        INTEGER,
            null_count       INTEGER,
            failed_records   INTEGER,
            records_processed INTEGER,
            schema_version   TEXT,
            status           TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS validation_results (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_name TEXT NOT NULL,
            run_time      TEXT NOT NULL,
            health_score  INTEGER,
            overall       TEXT,
            checks        TEXT,
            created_at    TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


def save_pipeline_runs(df: pd.DataFrame):
    """Save raw pipeline run records into the database."""
    conn = get_connection()
    df.to_sql("pipeline_runs", conn, if_exists="replace", index=False)
    conn.close()
    print(f"Saved {len(df)} pipeline runs to database.")


def save_validation_results(results: list):
    """Save validation reports into the database."""
    conn = get_connection()
    rows = []
    for r in results:
        rows.append((
            r["pipeline_name"],
            str(r["run_time"]),
            r["health_score"],
            r["overall"],
            json.dumps(r["checks"]),
        ))
    conn.executemany("""
        INSERT INTO validation_results (pipeline_name, run_time, health_score, overall, checks)
        VALUES (?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()
    print(f"Saved {len(rows)} validation results to database.")


def get_all_results() -> pd.DataFrame:
    """Load all validation results from the database."""
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM validation_results ORDER BY run_time DESC", conn)
    conn.close()
    df["checks"] = df["checks"].apply(json.loads)
    return df


def get_pipeline_summary() -> pd.DataFrame:
    """Summary stats per pipeline: total runs, pass/warn/fail counts, avg health score."""
    conn = get_connection()
    df = pd.read_sql("""
        SELECT
            pipeline_name,
            COUNT(*)                                         AS total_runs,
            SUM(CASE WHEN overall = 'PASS' THEN 1 ELSE 0 END) AS passed,
            SUM(CASE WHEN overall = 'WARN' THEN 1 ELSE 0 END) AS warned,
            SUM(CASE WHEN overall = 'FAIL' THEN 1 ELSE 0 END) AS failed,
            ROUND(AVG(health_score), 1)                      AS avg_health_score
        FROM validation_results
        GROUP BY pipeline_name
        ORDER BY avg_health_score ASC
    """, conn)
    conn.close()
    return df


def get_recent_failures(limit: int = 10) -> pd.DataFrame:
    """Get the most recent FAIL results across all pipelines."""
    conn = get_connection()
    df = pd.read_sql(f"""
        SELECT pipeline_name, run_time, health_score, checks
        FROM validation_results
        WHERE overall = 'FAIL'
        ORDER BY run_time DESC
        LIMIT {limit}
    """, conn)
    conn.close()
    return df


if __name__ == "__main__":
    import sys
    sys.path.append(".")
    from src.generator import generate_dataset
    from src.validator import validate_all

    # Step 1 — generate data
    df = generate_dataset(days=30)
    df.to_csv("data/pipeline_runs.csv", index=False)

    # Step 2 — validate
    results = validate_all(df)

    # Step 3 — store everything
    create_tables()
    save_pipeline_runs(df)
    save_validation_results(results)

    # Step 4 — query back and display
    print("\nPipeline Health Summary:")
    print("=" * 55)
    print(get_pipeline_summary().to_string(index=False))

    print("\nRecent Failures:")
    print("=" * 55)
    failures = get_recent_failures(5)
    for _, row in failures.iterrows():
        print(f"{row['pipeline_name']} | {row['run_time']} | score: {row['health_score']}")
        for check in json.loads(row["checks"]):
            if check["result"] != "PASS":
                print(f"  [{check['result']}] {check['check']}: {check['message']}")
        print()
