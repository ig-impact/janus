# Janus (Demo Monitoring Plan)

## How to run locally

Add necessary secrets to `.env` (`.env.template` available)

```sh
uv run streamlit main.py
```

This repository captures a minimal monitoring plan for the data pipeline spanning:
- `impact-initiatives/klt` (dlt pipeline for KoboToolbox)
- `impact-initiatives/matryoshka` (dbt project)

The goal is a simple, demo-grade monitor focused only on critical signals and a visual summary that is easy for non-technical stakeholders to understand.

## Minimal Monitoring Scope

### 1) GitHub Actions Health
Track the latest workflow runs and outcomes for:
- `klt`: daily incremental pipeline, hourly audit log pipeline, and manual batch runs.
- `matryoshka`: daily dbt run.

Capture only:
- status/conclusion
- last success timestamp
- run duration
- failure reason (when available from Actions run metadata)

### 2) Dependency Check
Confirm that a successful `klt` run triggers a `matryoshka` dbt run. Alert if dbt does not run or fails after a successful klt run.

### 3) Minimal Database Sanity
Use a simple daily row-count check on 1–2 representative tables (exact tables TBD). This is only to show trend and detect obvious issues (e.g., zero rows).

## Health Rules (Critical Only)
- Red: latest run failed or missed SLA.
- Amber: run is stale and nearing SLA breach.
- Green: run succeeded within SLA.
- Data anomaly: daily row count is zero or drops below a simple threshold.

## Simple Visual Output (Demo)
Single page dashboard (HTML or Markdown) with:
- Traffic-light status per pipeline (klt, audit log, dbt).
- Last success timestamps.
- 7-day bar chart of daily row counts (ingestion + final model).
- One-sentence plain-language summary of system health.

## Alerts (Minimal)
Only notify on Red conditions:
- failed run
- missed SLA
- zero/near-zero row count

## Demo Outputs
- JSON status snapshot (for integrations).
- One simple HTML or Markdown report (for stakeholders).
