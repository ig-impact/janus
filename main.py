import os
from datetime import datetime

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


def home_page() -> None:
    st.title("Janus")
    st.subheader("Overview")
    st.write("Use the sidebar to navigate to orchestration status.")


pages = [
    st.Page(home_page, title="Home"),
    st.Page(orchestration_page, title="Orchestration"),
]
navigation = st.navigation(pages, position="sidebar")
navigation.run()
