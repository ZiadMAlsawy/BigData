"""Streamlit dashboard for MP3 — polls parquet sinks, renders 5 panels."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from common import OUTPUT_DIR

st.set_page_config(page_title="MP3 Real-Time Recs", layout="wide",
                   initial_sidebar_state="collapsed")

REFRESH_SECONDS = 2

WIN_ITEMS = OUTPUT_DIR / "windows" / "items"
WIN_USERS = OUTPUT_DIR / "windows" / "users"
RECS = OUTPUT_DIR / "recs"
ALERTS = OUTPUT_DIR / "alerts"
LATENCY = OUTPUT_DIR / "latency"


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    parts = list(path.glob("*.parquet")) + list(path.glob("**/*.parquet"))
    parts = [p for p in parts if "_spark_metadata" not in p.parts]
    if not parts:
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        frames = []
        for p in parts[-50:]:
            try:
                frames.append(pd.read_parquet(p))
            except Exception:
                pass
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main():
    st.title("Real-Time Book Recommendations — MP3")
    st.caption(f"Auto-refresh every {REFRESH_SECONDS}s. Source: {OUTPUT_DIR}")

    # autorefresh via meta-tag fallback
    st.markdown(
        f"<meta http-equiv='refresh' content='{REFRESH_SECONDS}'>",
        unsafe_allow_html=True,
    )

    items = _read_parquet(WIN_ITEMS)
    users = _read_parquet(WIN_USERS)
    recs = _read_parquet(RECS)
    alerts = _read_parquet(ALERTS)
    latency = _read_parquet(LATENCY)

    # ---- Top metrics row -------------------------------------------------
    cols = st.columns(4)
    cols[0].metric("Active items (last batch)", int(items["item_id"].nunique()) if not items.empty else 0)
    cols[1].metric("Active users (last batch)", int(users["user_id"].nunique()) if not users.empty else 0)
    cols[2].metric("Recs generated", int(len(recs)))
    if not latency.empty:
        p50 = latency["latency_ms"].quantile(0.50)
        p95 = latency["latency_ms"].quantile(0.95)
        cols[3].metric("Latency p95 (ms)", f"{p95:.0f}", delta=f"p50 {p50:.0f}")
    else:
        cols[3].metric("Latency p95 (ms)", "—")

    st.divider()

    # ---- Panel 1: Live recommendations ----------------------------------
    st.subheader("1. Live recommendations")
    if recs.empty:
        st.info("Waiting for first recommendation batch…")
    else:
        latest_batch = recs["batch_id"].max() if "batch_id" in recs else None
        view = recs[recs["batch_id"] == latest_batch] if latest_batch is not None else recs
        st.dataframe(
            view.sort_values(["user_id", "rank"]).head(50),
            use_container_width=True, hide_index=True,
        )

    # ---- Panel 2: Trending books ----------------------------------------
    st.subheader("2. Trending books (custom score)")
    if items.empty:
        st.info("No window data yet.")
    else:
        latest_win = items["window_end"].max()
        snap = items[items["window_end"] == latest_win].nlargest(10, "trending_score")
        if not snap.empty:
            fig = px.bar(snap.sort_values("trending_score"),
                         x="trending_score", y="item_id", orientation="h",
                         hover_data=["count", "avg_rating"],
                         title=f"Top-10 trending @ {latest_win}")
            fig.update_layout(height=350, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig, use_container_width=True)

    # ---- Panel 3: User activity -----------------------------------------
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("3. User activity (rolling)")
        if users.empty:
            st.info("No user windows yet.")
        else:
            top_users = (users.groupby("user_id")["count"].sum()
                              .nlargest(15).reset_index())
            fig = px.bar(top_users, x="user_id", y="count",
                         title="Most active users")
            fig.update_layout(height=350, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig, use_container_width=True)

    # ---- Panel 4: Alerts feed -------------------------------------------
    with col2:
        st.subheader("4. Alerts feed")
        if alerts.empty:
            st.info("No alerts triggered yet.")
        else:
            latest = alerts.sort_values("window_end", ascending=False).head(20)
            for _, r in latest.iterrows():
                kind = r.get("alert_type", "?")
                color = "#f59e0b" if kind == "trending_item" else "#ef4444"
                st.markdown(
                    f"<div style='padding:6px;border-left:4px solid {color};"
                    f"margin:4px 0;background:#1a1d23'>"
                    f"<b>{kind}</b> — subject <code>{r.get('subject')}</code> "
                    f"metric={r.get('metric'):.2f} count={r.get('magnitude')} "
                    f"<span style='color:#888'>@ {r.get('window_end')}</span></div>",
                    unsafe_allow_html=True)

    # ---- Panel 5: Streaming metrics + latency histogram -----------------
    st.subheader("5. Streaming metrics — latency distribution")
    if latency.empty:
        st.info("No latency samples yet.")
    else:
        recent = latency.tail(2000)
        fig = px.histogram(recent, x="latency_ms", nbins=40,
                           title="End-to-end latency (most recent samples)")
        fig.add_vline(x=5000, line_dash="dash", line_color="red",
                      annotation_text="5 s budget")
        fig.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
