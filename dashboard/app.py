import json
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

from disinfo_lab.config import cfg

st.set_page_config(page_title="Disinfo Lab Dashboard", layout="wide")
engine = create_engine(cfg.db_url, future=True)

st.title("Disinfo Lab Dashboard")

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

st.subheader("Najnowsze rekordy (do 500)")
st.dataframe(df, use_container_width=True)

st.subheader("Agregaty")
col1, col2, col3 = st.columns(3)
col1.metric("Articles", int(df["article_id"].nunique()) if not df.empty else 0)
col2.metric("Labels", int(df["label_json"].notna().sum()) if not df.empty else 0)
col3.metric("Categories", int(df["category"].dropna().nunique()) if not df.empty else 0)

if not df.empty:
    st.subheader("Artykuły per kategoria")
    cat = df["category"].fillna("None").value_counts().reset_index()
    cat.columns = ["category", "count"]
    st.bar_chart(cat.set_index("category"))

st.subheader("Podgląd etykiety JSON")
ids = df["article_id"].dropna().astype(int).tolist()
pick = st.selectbox("Wybierz article_id", ids[:200] if ids else [])
if pick:
    row = df[df["article_id"] == pick].head(1)
    if not row.empty and pd.notna(row.iloc[0]["label_json"]):
        try:
            st.json(json.loads(row.iloc[0]["label_json"]))
        except Exception:
            st.code(row.iloc[0]["label_json"])
    else:
        st.info("Brak etykiety dla tego artykułu.")
