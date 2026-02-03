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

        # Ujednolić identyfikator artykułu w articles.csv
        # (czasem może już istnieć article_id, ale standardowo jest id)
        if "article_id" not in a.columns and "id" in a.columns:
            a = a.rename(columns={"id": "article_id"})

        # W labels.csv oczekujemy article_id; jeśli ktoś miał id -> popraw
        if not l.empty and "article_id" not in l.columns and "id" in l.columns:
            l = l.rename(columns={"id": "article_id"})

        # Ujednolić JSON etykiety
        if not l.empty and "label_json" not in l.columns and "json" in l.columns:
            l = l.rename(columns={"json": "label_json"})

        # Jeżeli nie ma labeli, przygotuj puste kolumny
        if l.empty:
            df = a.copy()
            for col in ["task", "model", "labeled_at", "label_json"]:
                if col not in df.columns:
                    df[col] = None
        else:
            # MERGE po article_id; po merge nie chcemy duplikatów
            # Uwaga: labels mogą mieć wiele rekordów na artykuł (różne task/model)
            # W dashboardzie weźmiemy najnowszy label per article_id (po created_at jeśli jest).
            l2 = l.copy()

            if "created_at" in l2.columns:
                l2 = l2.sort_values("created_at", ascending=False)
                l2 = l2.drop_duplicates(subset=["article_id"], keep="first")
            elif "id" in l2.columns:
                l2 = l2.sort_values("id", ascending=False)
                l2 = l2.drop_duplicates(subset=["article_id"], keep="first")
            else:
                l2 = l2.drop_duplicates(subset=["article_id"], keep="first")

            df = a.merge(l2, how="left", on="article_id", suffixes=("", "_label"))

            # Jeśli po merge pojawiły się przypadkiem duplikaty nazw, zbij je
            # (pandas na ogół suffixuje, ale lepiej być odpornym)
            df = df.loc[:, ~df.columns.duplicated()]

            # Ujednolić nazwę czasu etykiety
            if "labeled_at" not in df.columns and "created_at_label" in df.columns:
                df = df.rename(columns={"created_at_label": "labeled_at"})
            elif "labeled_at" not in df.columns and "created_at" in l2.columns:
                # jeżeli label.created_at weszło jako created_at (gdy articles nie miały created_at)
                # zabezpieczenie:
                pass

        # final sort
        if "article_id" in df.columns:
            df = df.sort_values("article_id", ascending=False).head(500)
        else:
            df = df.head(500)

else:
    # empty: wymuś utworzenie sqlite (przydatne w chmurze)
    ensure_storage()
    st.info("No sqlite/csv found. Created empty sqlite. Run ingest first.")
    df = pd.DataFrame()

st.subheader("Najnowsze rekordy (do 500)")
st.dataframe(df, use_container_width=True)

st.subheader("Agregaty")
col1, col2, col3 = st.columns(3)
col1.metric("Articles", int(df["article_id"].nunique()) if (not df.empty and "article_id" in df.columns) else 0)
col2.metric("Labels", int(df["label_json"].notna().sum()) if (not df.empty and "label_json" in df.columns) else 0)
col3.metric("Categories", int(df["category"].dropna().nunique()) if (not df.empty and "category" in df.columns) else 0)

if not df.empty and "category" in df.columns:
    st.subheader("Artykuły per kategoria")
    cat = df["category"].fillna("None").value_counts().reset_index()
    cat.columns = ["category", "count"]
    st.bar_chart(cat.set_index("category"))

st.subheader("Podgląd etykiety JSON")
if not df.empty and "article_id" in df.columns:
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
