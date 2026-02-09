from __future__ import annotations

from dataclasses import dataclass

import requests
import streamlit as st


API_VERSION = "2022-11-28"


@dataclass(frozen=True)
class WorkflowConfig:
    title: str
    owner: str
    repo: str
    workflow_id: str


def build_headers(token: str) -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": API_VERSION,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


@st.cache_data(ttl=120)
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
