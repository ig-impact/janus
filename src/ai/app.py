from __future__ import annotations

import os

import psycopg
import requests
import streamlit as st
from dotenv import load_dotenv

from db import DbConfig, check_connection, fetch_range, fetch_row_count, validate_config
from github_api import WorkflowConfig, fetch_latest_workflow_run_for_workflow
from utils import format_duration, format_timestamp

ORCHESTRATION_WORKFLOWS = [
    WorkflowConfig(
        title="Matryoshka",
        owner="impact-initiatives",
        repo="matryoshka",
        workflow_id="run-dbt-aci.yml",
    ),
    WorkflowConfig(
        title="KLT - Pipeline",
        owner="impact-initiatives",
        repo="klt",
        workflow_id="run-pipeline-dev.yml",
    ),
    WorkflowConfig(
        title="KLT - Audit Log",
        owner="impact-initiatives",
        repo="klt",
        workflow_id="run-audit-log.yml",
    ),
]


def orchestration_page() -> None:
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN") or ""

    st.title("Janus")
    st.subheader("Orchestration")

    for config in ORCHESTRATION_WORKFLOWS:
        container = st.container(border=True)
        with container:
            render_orchestration_panel(token, config)


def render_orchestration_panel(token: str, config: WorkflowConfig) -> None:
    st.markdown(f"### {config.title}")
    try:
        latest_run = fetch_latest_workflow_run_for_workflow(
            token, config.owner, config.repo, config.workflow_id
        )
    except requests.HTTPError as exc:
        st.error("Failed to fetch workflow runs from GitHub.")
        st.code(str(exc))
        return

    if not latest_run:
        st.info(f"No workflow runs found for {config.workflow_id}.")
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

    st.write(f"Repository: {config.owner}/{config.repo}")
    st.write(f"Workflow: {latest_run.get('name', 'Unknown')}")
    st.write(f"Status: {status_emoji} {status}")
    st.write(f"Conclusion: {conclusion or 'unknown'}")
    st.write(f"Run number: {latest_run.get('run_number', 'unknown')}")
    st.write(f"Started: {latest_run.get('run_started_at', 'unknown')}")
    run_duration = format_duration(
        latest_run.get("run_started_at"),
        latest_run.get("completed_at") or latest_run.get("updated_at"),
    )
    if run_duration:
        st.write(f"Duration: {run_duration}")
    run_url = latest_run.get("html_url")
    if run_url:
        st.markdown("[Open run]({})".format(run_url))


def stats_page() -> None:
    st.title("Janus")
    st.subheader("Database Stats")
    load_dotenv()

    config = DbConfig(
        dbname=os.getenv("DBNAME", ""),
        host=os.getenv("HOST", ""),
        user=os.getenv("DB_USER", ""),
        password=os.getenv("PASSWORD", ""),
        schema_staging=os.getenv("SCHEMA_STAGING", ""),
    )

    missing = validate_config(config)
    if missing:
        st.warning("Database env vars are not fully configured.")
        st.code(", ".join(missing))
        return

    st.write("Database connection status")
    try:
        check_connection(config)
        st.success("Database connection is alive.")
    except psycopg.Error as exc:
        st.error("Database connection failed.")
        st.code(str(exc))
        return

    st.subheader("Staging metrics")
    table_names = ["stg_klt__kobo_asset", "stg_klt__kobo_submission"]
    for table in table_names:
        count = fetch_row_count(config, table)
        earliest, latest = fetch_range(config, table, "loaded_at")

        st.metric(f"{table} rows", f"{count:,}")
        st.caption(
            "Loaded at range: {} -> {}".format(
                format_timestamp(earliest),
                format_timestamp(latest),
            )
        )

        if table == "stg_klt__kobo_asset":
            created_earliest, created_latest = fetch_range(config, table, "created_at")
            st.caption(
                "Created at range: {} -> {}".format(
                    format_timestamp(created_earliest),
                    format_timestamp(created_latest),
                )
            )

        if table == "stg_klt__kobo_submission":
            submitted_earliest, submitted_latest = fetch_range(
                config, table, "submitted_at"
            )
            st.caption(
                "Submitted at range: {} -> {}".format(
                    format_timestamp(submitted_earliest),
                    format_timestamp(submitted_latest),
                )
            )


def run_app() -> None:
    pages = [
        st.Page(orchestration_page, title="Orchestration"),
        st.Page(stats_page, title="Stats"),
    ]
    navigation = st.navigation(pages, position="sidebar")
    navigation.run()
