from __future__ import annotations

import os
from typing import Any

import psycopg
import streamlit as st
from dotenv import load_dotenv
from github import Auth, Github
from psycopg import sql
from psycopg.rows import dict_row


@st.cache_resource
def get_db_connection() -> psycopg.Connection:
    load_dotenv()
    dbname = os.getenv("DBNAME")
    host = os.getenv("HOST")
    user = os.getenv("DB_USER")
    password = os.getenv("PASSWORD")
    return psycopg.connect(
        dbname=dbname,
        host=host,
        user=user,
        password=password,
    )


@st.cache_resource
def get_github_client():
    load_dotenv()
    auth = Auth.Token(os.getenv("G_TOKEN", ""))
    return Github(auth=auth)


st.title("Janus")
st.logo(
    "https://www.impact-initiatives.org/wp-content/themes/impactinitiatives/static/min/img/logo-impact-new.png",
    link="https://streamlit.io/gallery",
    icon_image="https://www.impact-initiatives.org/wp-content/themes/impactinitiatives/static/min/img/logo-impact-new.png",
)
st.header("Orchestration")


# workflows = {
#     "audit_log": 231782097,  # KLT Audit Log Pipeline
#     "incremental": 214422987,  # Run KLT Pipeline (Daily)
# }


@st.cache_data(ttl=3600)
def get_run_history(repo_name: str, workflow_id: int) -> list[dict[str, Any]]:
    repo = get_github_client().get_repo(repo_name)
    workflow = repo.get_workflow(workflow_id)
    workflow_runs = list(workflow.get_runs())
    run_info = [
        {
            "status": wf.status,
            "started_at": wf.run_started_at,
            "conclusion": wf.conclusion,
            "duration": wf.timing().run_duration_ms / 1000 / 60,
        }
        for wf in workflow_runs
    ]
    return run_info


audit_log_run_history = get_run_history("impact-initiatives/klt", 231782097)
daily_ingestion = get_run_history("impact-initiatives/klt", 214422987)

tab_1, tab_2 = st.tabs(["Audit Logs", "Daily Data Ingestion"])

with tab_1:
    st.scatter_chart(
        audit_log_run_history,
        x="started_at",
        x_label="Started at",
        y_label="Duration (Minutes)",
        y="duration",
        color="conclusion",
        width="stretch",
    )
with tab_2:
    st.scatter_chart(
        daily_ingestion,
        x="started_at",
        y="duration",
        color="conclusion",
        width="stretch",
        x_label="Started at",
        y_label="Duration (Minutes)",
    )

st.header("Database")


def check_connection() -> bool:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("select 1;")
            cur.fetchone()
        return True
    except Exception:
        conn.rollback()


with st.spinner():
    try:
        check_connection()
        st.success("Connected")
    except Exception as e:
        st.error(f"{e}")


@st.cache_data()
def fetch_row_count(table: str) -> int:
    query = sql.SQL("select count(*) from {}.{}").format(
        sql.Identifier("dummy"),
        sql.Identifier(table),
    )
    conn = get_db_connection()
    with conn.cursor() as cur:
        try:
            cur.execute(query)
            row = cur.fetchone()
            return int(row[0]) if row else 0
        except Exception:
            conn.rollback()


@st.cache_data()
def get_date_range(table: str, column: str):
    query = sql.SQL("""
    select min({}) as min_range, max({}) as max_range
    from {}.{}
    """).format(
        sql.Identifier(column),
        sql.Identifier(column),
        sql.Identifier("dummy"),
        sql.Identifier(table),
    )
    conn = get_db_connection()
    with conn.cursor(row_factory=dict_row) as cur:
        try:
            cur.execute(query)
            row = cur.fetchone()
            return row
        except Exception:
            conn.rollback()


@st.cache_data()
def get_count_by_date(table: str, column: str):
    query = sql.SQL("""
    select
        date({}) as event_on,
        count(*) as count_on_event
    from
        {}.{}
    group by
        date({}) ;
    """).format(
        sql.Identifier(column),
        sql.Identifier("dummy"),
        sql.Identifier(table),
        sql.Identifier(column),
    )

    conn = get_db_connection()
    with conn.cursor(row_factory=dict_row) as cur:
        try:
            cur.execute(query)
            row = cur.fetchall()
            return row
        except Exception:
            conn.rollback()


cols = st.columns(3)

with cols[0]:
    st.metric(label="Projects", value=fetch_row_count("stg_klt__kobo_asset"))

with cols[1]:
    st.metric(label="Submissions", value=fetch_row_count("stg_klt__kobo_submission"))

with cols[2]:
    st.metric(
        label="Survey Questions",
        value=fetch_row_count("stg_klt__kobo_asset_content__survey"),
    )

tab_assets, tab_submissions, tab_questions = st.tabs(
    ["Projects", "Submissions", "Survey Questions"]
)

with tab_assets:
    earliest, latest = get_date_range("stg_klt__kobo_asset", "modified_at").values()

    st.text(f"Earliest Project: {earliest.date()}")
    st.text(f"Latest Project: {latest.date()}")

    st.bar_chart(
        get_count_by_date("stg_klt__kobo_asset", "loaded_at"),
        x="event_on",
        y="count_on_event",
    )

with tab_submissions:
    earliest, latest = get_date_range(
        "stg_klt__kobo_submission", "submitted_at"
    ).values()
    st.text(f"Earliest Submission: {earliest.date()}")
    st.text(f"Latest Submission: {latest.date()}")

    st.bar_chart(
        get_count_by_date("stg_klt__kobo_submission", "loaded_at"),
        x="event_on",
        y="count_on_event",
    )

with tab_questions:
    st.bar_chart(
        get_count_by_date("stg_klt__kobo_asset_content__survey", "loaded_at"),
        x="event_on",
        y="count_on_event",
    )
