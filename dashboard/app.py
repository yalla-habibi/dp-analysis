import json
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

from disinfo_lab.config import cfg
from disinfo_lab.storage import detect_mode, storage_paths, ensure_storage

st.set_page_config(page_title="Disinfo Lab Dashboard", layout="wide")
st.title("Disinfo Lab Dashboard")

mode = detect_mode()
db_path, articles_csv, labels_csv = storage_paths()

st.caption(f"Storage mode: {mode}")

if mode == "sqlite":
    engine = create_engine(cfg.db_url, future=True)

    q = text("""
    SELECT
      a.id as article_id,
      a.created_at,
      a.url,
      a.title,
      a.category,
      a.published_at,
      l.task,
      l.model,
      l.created_at as labeled_at,
      l.json as label_json
    FROM articles a
    LEFT JOIN llm_labels l ON l.article_id = a.id
    ORDER BY a.id DESC
    LIMIT 500
    """)

    with engine.connect() as conn:
        rows = conn.execute(q).mappings().all()

    df = pd.DataFrame(rows)

elif mode == "csv":
    # read-only tryb z CSV
    if not articles_csv.exists():
        df = pd.DataFrame()
    else:
        a = pd.read_csv(articles_csv)
        l = pd.read_csv(labels_csv) if labels_csv.exists() else pd.DataFrame()

        if not l.empty:
            df = a.merge(l, how="left", left_on="id", right_on="article_id", suffixes=("", "_label"))
            df = df.rename(columns={"id": "article_id", "json": "label_json"})
        else:
            df = a.rename(columns={"id": "article_id"})
            df["task"] = None
            df["model"] = None
            df["labeled_at"] = None
            df["label_json"] = None

        df = df.sort_values("article_id", ascending=False).head(500)

else:
    # empty: wymuś utworzenie sqlite (przydatne w chmurze)
    ensure_storage()
    st.info("No sqlite/csv found. Created empty sqlite. Run ingest first.")
    df = pd.DataFrame()

st.subheader("Najnowsze rekordy (do 500)")
st.dataframe(df, use_container_width=True)

st.subheader("Agregaty")
col1, col2, col3 = st.columns(3)
col1.metric("Articles", int(df["article_id"].nunique()) if not df.empty else 0)
col2.metric("Labels", int(df["label_json"].notna().sum()) if (not df.empty and "label_json" in df.columns) else 0)
col3.metric("Categories", int(df["category"].dropna().nunique()) if (not df.empty and "category" in df.columns) else 0)

if not df.empty and "category" in df.columns:
    st.subheader("Artykuły per kategoria")
    cat = df["category"].fillna("None").value_counts().reset_index()
    cat.columns = ["category", "count"]
    st.bar_chart(cat.set_index("category"))

st.subheader("Podgląd etykiety JSON")
if not df.empty:
    ids = df["article_id"].dropna().astype(int).tolist()
    pick = st.selectbox("Wybierz article_id", ids[:200] if ids else [])
    if pick:
        row = df[df["article_id"] == pick].head(1)
        if not row.empty and pd.notna(row.iloc[0].get("label_json")):
            try:
                st.json(json.loads(row.iloc[0]["label_json"]))
            except Exception:
                st.code(str(row.iloc[0]["label_json"]))
        else:
            st.info("Brak etykiety dla tego artykułu.")
