# PipeWatch

Data pipeline observability system that monitors pipeline health, detects data quality issues, and alerts on failures.

## What it does

Simulates production data pipelines and monitors them across 5 dimensions:

| Check | What it catches |
|-------|----------------|
| Freshness | Pipeline ran late or not at all |
| Row Count | Volume dropped unexpectedly |
| Schema Drift | Unexpected column changes upstream |
| Null Rate | Too many missing values |
| Failed Records | Too many records erroring out |

Every pipeline run gets a health score (0–100) and an overall status of PASS / WARN / FAIL.

## Project Structure

```
data-pipeline-observability/
├── src/
│   ├── generator.py   # Generates fake pipeline run data
│   ├── validator.py   # Runs 5 quality checks per run
│   ├── store.py       # Saves results to SQLite
│   └── alerts.py      # Alert rules engine
├── dashboard/
│   └── app.py         # Streamlit dashboard
├── tests/
│   └── test_validator.py  # 22 pytest tests
└── .github/workflows/
    └── ci.yml         # GitHub Actions CI
```

## Quick Start

```bash
pip install -r requirements.txt

# Generate data, validate, store, alert
python src/generator.py
python -m src.store
python -m src.alerts

# Run the dashboard
streamlit run dashboard/app.py

# Run tests
python -m pytest tests/ -v
```

## Tech Stack

Python, Pandas, SQLite, Streamlit, Pytest, GitHub Actions
