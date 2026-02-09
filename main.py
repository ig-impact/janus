import os
from datetime import datetime

import psycopg
from psycopg import sql
import requests
import streamlit as st
from dotenv import load_dotenv

API_VERSION = "2022-11-28"
ORCHESTRATION_WORKFLOWS = [
    {
        "title": "Matryoshka",
        "owner": "impact-initiatives",
        "repo": "matryoshka",
        "workflow_id": "run-dbt-aci.yml",
    },
    {
        "title": "KLT - Pipeline",
        "owner": "impact-initiatives",
        "repo": "klt",
        "workflow_id": "run-pipeline-dev.yml",
    },
    {
        "title": "KLT - Audit Log",
        "owner": "impact-initiatives",
        "repo": "klt",
        "workflow_id": "run-audit-log.yml",
    },
]


def build_headers(token: str) -> dict:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": API_VERSION,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def fetch_latest_workflow_run_for_workflow(
    token: str, owner: str, repo: str, workflow_id: str
) -> dict:
    url = (
        f"https://api.github.com/repos/{owner}/{repo}"
        f"/actions/workflows/{workflow_id}/runs"
    )
    response = requests.get(
        url,
        headers=build_headers(token),
        params={"per_page": 1},
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()
    runs = payload.get("workflow_runs", [])
    return runs[0] if runs else {}


def render_orchestration_panel(token: str, config: dict) -> None:
    owner = config["owner"]
    repo = config["repo"]
    workflow_id = config["workflow_id"]
    st.markdown(f"### {config['title']}")
    try:
        latest_run = fetch_latest_workflow_run_for_workflow(
            token, owner, repo, workflow_id
        )
    except requests.HTTPError as exc:
        st.error("Failed to fetch workflow runs from GitHub.")
        st.code(str(exc))
        return

    if not latest_run:
        st.info(f"No workflow runs found for {workflow_id}.")
        return

    conclusion = latest_run.get("conclusion")
    status = latest_run.get("status", "unknown")
    if conclusion == "success":
        status_emoji = "✅"
    elif conclusion in {"failure", "cancelled", "timed_out", "action_required"}:
        status_emoji = "❌"
    elif status != "completed":
        status_emoji = "⏳"
    else:
        status_emoji = "⚪"

    st.write(f"Repository: {owner}/{repo}")
    st.write(f"Workflow: {latest_run.get('name', 'Unknown')}")
    st.write(f"Status: {status_emoji} {status}")
    st.write(f"Conclusion: {conclusion or 'unknown'}")
    st.write(f"Run number: {latest_run.get('run_number', 'unknown')}")
    st.write(f"Started: {latest_run.get('run_started_at', 'unknown')}")
    completed_at = latest_run.get("completed_at")
    run_duration = format_duration(
        latest_run.get("run_started_at"), completed_at or latest_run.get("updated_at")
    )
    if run_duration:
        st.write(f"Duration: {run_duration}")
    run_url = latest_run.get("html_url")
    if run_url:
        st.markdown("[Open run]({})".format(run_url))


def orchestration_page() -> None:
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN") or ""

    st.title("Janus")
    st.subheader("Orchestration")

    for config in ORCHESTRATION_WORKFLOWS:
        container = st.container(border=True)
        with container:
            render_orchestration_panel(token, config)


def format_duration(started_at: str | None, ended_at: str | None) -> str:
    if not started_at or not ended_at:
        return ""
    try:
        start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        end = datetime.fromisoformat(ended_at.replace("Z", "+00:00"))
    except ValueError:
        return ""
    duration = end - start
    if duration.total_seconds() < 0:
        return ""
    total_seconds = int(duration.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def format_timestamp(value: object) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S %Z").strip()
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    return parsed.strftime("%Y-%m-%d %H:%M:%S %Z").strip()


def home_page() -> None:
    st.title("Janus")
    st.subheader("Overview")
    st.write("Use the sidebar to navigate to orchestration status.")


def stats_page() -> None:
    st.title("Janus")
    st.subheader("Database Stats")
    load_dotenv()
    st.write("Database connection status")

    config = {
        "dbname": os.getenv("DBNAME", ""),
        "host": os.getenv("HOST", ""),
        "user": os.getenv("DB_USER", ""),
        "password": os.getenv("PASSWORD", ""),
    }

    schema_staging = os.getenv("SCHEMA_STAGING", "")

    if not all(config.values()):
        st.warning("Database env vars are not fully configured.")
        return

    if not schema_staging:
        st.warning("SCHEMA_STAGING is not configured.")
        return

    try:
        with psycopg.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute("select 1;")
                cur.fetchone()
        st.success("Database connection is alive.")
    except psycopg.Error as exc:
        st.error("Database connection failed.")
        st.code(str(exc))
        return

    st.subheader("Staging metrics")
    table_names = ["stg_klt__kobo_asset", "stg_klt__kobo_submission"]
    try:
        with psycopg.connect(**config) as conn:
            with conn.cursor() as cur:
                for table in table_names:
                    count_query = sql.SQL("select count(*) from {}.{}").format(
                        sql.Identifier(schema_staging),
                        sql.Identifier(table),
                    )
                    cur.execute(count_query)
                    count = cur.fetchone()[0]

                    range_query = sql.SQL(
                        "select min(loaded_at), max(loaded_at) from {}.{}"
                    ).format(
                        sql.Identifier(schema_staging),
                        sql.Identifier(table),
                    )
                    cur.execute(range_query)
                    earliest, latest = cur.fetchone()

                    st.metric(f"{table} rows", f"{count:,}")
                    st.caption(
                        "Loaded at range: {} -> {}".format(
                            format_timestamp(earliest),
                            format_timestamp(latest),
                        )
                    )

                    if table == "stg_klt__kobo_asset":
                        created_query = sql.SQL(
                            "select min(created_at), max(created_at) from {}.{}"
                        ).format(
                            sql.Identifier(schema_staging),
                            sql.Identifier(table),
                        )
                        cur.execute(created_query)
                        created_earliest, created_latest = cur.fetchone()
                        st.caption(
                            "Created at range: {} -> {}".format(
                                format_timestamp(created_earliest),
                                format_timestamp(created_latest),
                            )
                        )

                    if table == "stg_klt__kobo_submission":
                        submitted_query = sql.SQL(
                            "select min(submitted_at), max(submitted_at) from {}.{}"
                        ).format(
                            sql.Identifier(schema_staging),
                            sql.Identifier(table),
                        )
                        cur.execute(submitted_query)
                        submitted_earliest, submitted_latest = cur.fetchone()
                        st.caption(
                            "Submitted at range: {} -> {}".format(
                                format_timestamp(submitted_earliest),
                                format_timestamp(submitted_latest),
                            )
                        )
    except psycopg.Error as exc:
        st.error("Failed to fetch staging row counts.")
        st.code(str(exc))


pages = [
    st.Page(home_page, title="Home"),
    st.Page(orchestration_page, title="Orchestration"),
    st.Page(stats_page, title="Stats"),
]
navigation = st.navigation(pages, position="sidebar")
navigation.run()
