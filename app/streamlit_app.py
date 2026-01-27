import os
import sqlite3
from datetime import timedelta
from pathlib import Path

import pandas as pd
import streamlit as st


st.set_page_config(page_title="Traffic Pipeline Monitor", layout="wide")


def _get_db_path() -> str:
    return os.getenv("SQLITE_DB_PATH", "./data/traffic.db")


def _ensure_db(db_path: str) -> None:
    if Path(db_path).exists():
        return
    sample_path = Path(os.getenv("SAMPLE_JSONL_PATH", "./data/sample/traffic_sample.jsonl"))
    if not sample_path.exists():
        return
    try:
        from scripts.load_to_sqlite import run as load_run

        load_run(jsonl_dir=sample_path.parent, db_path=Path(db_path), topic=None)
    except Exception:
        return


@st.cache_data(ttl=60)
def load_data(db_path: str, limit: int) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        query = """
        SELECT
            topic,
            partition,
            offset,
            interval_start,
            road_name,
            delay_seconds,
            speed_kmh,
            travel_time_seconds,
            link_id,
            dumped_at
        FROM traffic_observations
        ORDER BY interval_start DESC
        LIMIT ?;
        """
        df = pd.read_sql_query(query, conn, params=(limit,))
    except sqlite3.Error:
        df = pd.DataFrame()
    finally:
        conn.close()
    return df


st.title("Victoria Traffic — Local SQLite Demo")
st.caption("Streamlit reads from a local SQLite DB loaded from JSONL dumps.")

with st.sidebar:
    st.header("Settings")
    page = st.selectbox("Dashboard", ["Traffic performance", "Kafka monitor"])
    db_path = st.text_input("SQLite DB path", value=_get_db_path())
    window_minutes = st.slider("Window (minutes)", min_value=10, max_value=240, value=60, step=10)
    row_limit = st.slider("Max rows", min_value=100, max_value=5000, value=1000, step=100)
    if st.button("Refresh data"):
        st.cache_data.clear()

_ensure_db(db_path)
data = load_data(db_path, row_limit)

if data.empty:
    st.warning("No rows found. Load JSONL into SQLite first.")
    st.code("python scripts/load_to_sqlite.py --jsonl-dir ./data/raw --db-path ./data/traffic.db")
    st.stop()

data["interval_start"] = pd.to_datetime(data["interval_start"], utc=True, errors="coerce")
data["dumped_at"] = pd.to_datetime(data["dumped_at"], utc=True, errors="coerce")
data = data.dropna(subset=["interval_start"]).copy()

latest_time = data["interval_start"].max()
window_start = latest_time - timedelta(minutes=window_minutes)
windowed = data[data["interval_start"] >= window_start].copy()

if page == "Traffic performance":
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Latest interval (UTC)", latest_time.strftime("%Y-%m-%d %H:%M:%S"))
    col2.metric("Rows (window)", f"{len(windowed):,}")
    col3.metric("Unique links", f"{windowed['link_id'].nunique():,}")
    col4.metric("Avg delay (s)", f"{windowed['delay_seconds'].mean():.1f}")

    st.subheader("Delay over time (avg)")
    series = (
        windowed.groupby("interval_start", as_index=False)["delay_seconds"]
        .mean()
        .sort_values("interval_start")
    )
    st.line_chart(series, x="interval_start", y="delay_seconds")

    st.subheader("Latest delays by road")
    latest_rows = (
        windowed.sort_values("interval_start")
        .groupby("road_name", as_index=False)
        .tail(1)
        .sort_values("delay_seconds", ascending=False)
    )
    st.dataframe(
        latest_rows[["interval_start", "road_name", "delay_seconds", "speed_kmh"]].head(30),
        use_container_width=True,
    )

    st.subheader("Recent records")
    st.dataframe(
        windowed.sort_values("interval_start", ascending=False).head(50),
        use_container_width=True,
    )
else:
    st.subheader("Kafka monitor (from SQLite)")
    windowed = windowed.dropna(subset=["dumped_at"]).copy()
    windowed["event_to_dump_latency_seconds"] = (
        windowed["dumped_at"] - windowed["interval_start"]
    ).dt.total_seconds()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Latest interval (UTC)", latest_time.strftime("%Y-%m-%d %H:%M:%S"))
    col2.metric("Records (window)", f"{len(windowed):,}")
    col3.metric("Avg event→dump (s)", f"{windowed['event_to_dump_latency_seconds'].mean():.1f}")
    col4.metric("Max event→dump (s)", f"{windowed['event_to_dump_latency_seconds'].max():.1f}")

    st.subheader("Records per 5-minute window")
    windowed["dumped_5m"] = windowed["dumped_at"].dt.floor("5min")
    per_window = (
        windowed.groupby("dumped_5m", as_index=False)
        .size()
        .rename(columns={"size": "records"})
        .sort_values("dumped_5m")
    )
    st.line_chart(per_window, x="dumped_5m", y="records")

    st.subheader("Partition distribution (window)")
    if "partition" in windowed.columns:
        partition_counts = (
            windowed.groupby("partition", as_index=False)
            .size()
            .rename(columns={"size": "records"})
            .sort_values("partition")
        )
        st.bar_chart(partition_counts, x="partition", y="records")
    else:
        st.info("Partition column not available in this dataset.")

    st.subheader("Max offset by partition")
    if "partition" in windowed.columns and "offset" in windowed.columns:
        max_offsets = (
            windowed.groupby("partition", as_index=False)["offset"]
            .max()
            .sort_values("partition")
        )
        st.dataframe(max_offsets, use_container_width=True)
    else:
        st.info("Partition/offset columns not available in this dataset.")

st.caption("Tip: use the Refresh button after you load new data.")
