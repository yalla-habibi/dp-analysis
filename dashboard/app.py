# disinfo_lab/dashboard/app.py
from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st
from sqlalchemy import func

from disinfo_lab.config import cfg
from disinfo_lab.storage import detect_mode, storage_paths, ensure_storage
from disinfo_lab.db import make_session, Article, LLMLabel


# -----------------------------
# Konfiguracja osi stance
# -----------------------------
DEFAULT_STANCE_KEYS = ["UE", "NATO", "USA", "Ukraina", "Rosja", "Rzad_Polski"]
STANCE_VALUES_ORDER = [-2, -1, 0, 1, 2]
STANCE_LABELS = {
    "Rzad_Polski": "Rząd Polski",
}


def _stance_display_name(k: str) -> str:
    return STANCE_LABELS.get(k, k)


def _safe_json_loads(s: str) -> Dict[str, Any]:
    try:
        obj = json.loads(s) if s else {}
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _list_to_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, list):
        return "; ".join([str(i).strip() for i in x if str(i).strip()])
    if isinstance(x, str):
        return x.strip()
    return str(x)


def _evidence_to_str(evidence: Any) -> str:
    """
    evidence może być:
    - dict: {axis: [quote,...]}
    - list: ["quote", ...]
    - string
    """
    if evidence is None:
        return ""
    if isinstance(evidence, dict):
        parts: List[str] = []
        for k, v in evidence.items():
            if isinstance(v, list):
                vv = "; ".join([str(i).strip() for i in v if str(i).strip()])
            else:
                vv = str(v).strip()
            if vv:
                parts.append(f"{k}: {vv}")
        return " | ".join(parts)
    if isinstance(evidence, list):
        return "; ".join([str(i).strip() for i in evidence if str(i).strip()])
    if isinstance(evidence, str):
        return evidence.strip()
    return str(evidence)


def _detect_stance_keys_from_df(df: pd.DataFrame) -> List[str]:
    """
    Jeśli mamy kolumny stance_*, wyciągnij klucze.
    """
    cols = [c for c in df.columns if c.startswith("stance_")]
    keys = [c.replace("stance_", "", 1) for c in cols]
    # uporządkuj: najpierw domyślne, potem reszta
    out: List[str] = []
    for k in DEFAULT_STANCE_KEYS:
        if k in keys:
            out.append(k)
    for k in sorted(set(keys) - set(out)):
        out.append(k)
    return out or DEFAULT_STANCE_KEYS


# -----------------------------
# Task/model listy
# -----------------------------
@st.cache_data(show_spinner=False, ttl=60)
def list_tasks_models(mode: str) -> Tuple[List[str], List[str]]:
    db_path, articles_csv, labels_csv = storage_paths()

    tasks: List[str] = []
    models: List[str] = []

    if mode == "sqlite":
        Session = make_session(cfg.db_url)
        with Session() as s:
            tasks = [r[0] for r in s.query(LLMLabel.task).distinct().all() if r and r[0]]
            models = [r[0] for r in s.query(LLMLabel.model).distinct().all() if r and r[0]]

    elif mode == "csv":
        if labels_csv.exists():
            l = pd.read_csv(labels_csv)
            if "task" in l.columns:
                tasks = sorted([x for x in l["task"].dropna().astype(str).unique().tolist() if x.strip()])
            if "model" in l.columns:
                models = sorted([x for x in l["model"].dropna().astype(str).unique().tolist() if x.strip()])

    tasks = sorted(set(tasks))
    models = sorted(set(models))
    return tasks, models


# -----------------------------
# Wczytanie danych (sqlite/csv)
# -----------------------------
@st.cache_data(show_spinner=True, ttl=60)
def load_joined_dataframe(mode: str, task: str, model: str) -> pd.DataFrame:
    """
    Ładuje najnowszą etykietę per artykuł dla (task, model) i łączy z Article.
    Rozwija JSON do kolumn, w tym stance_* dla każdej osi.
    """
    db_path, articles_csv, labels_csv = storage_paths()

    records: List[Dict[str, Any]] = []

    if mode == "sqlite":
        Session = make_session(cfg.db_url)
        with Session() as s:
            subq = (
                s.query(
                    LLMLabel.article_id.label("article_id"),
                    func.max(LLMLabel.id).label("max_id"),
                )
                .filter(LLMLabel.task == task, LLMLabel.model == model)
                .group_by(LLMLabel.article_id)
                .subquery()
            )

            q = (
                s.query(Article, LLMLabel)
                .join(subq, subq.c.article_id == Article.id)
                .join(LLMLabel, LLMLabel.id == subq.c.max_id)
            )

            rows = q.all()

        for art, lbl in rows:
            j = _safe_json_loads(lbl.json)

            stance = j.get("stance") if isinstance(j.get("stance"), dict) else {}
            evidence = j.get("evidence")

            stance_cols = {f"stance_{k}": stance.get(k, None) for k in stance.keys()}  # dynamic

            rec = {
                # Article fields
                "article_id": art.id,
                "created_at": getattr(art, "created_at", None),
                "url": art.url,
                "title": art.title,
                "category": art.category,
                "published_at": art.published_at,
                "source_hint": art.source_hint,
                "raw_html": art.raw_html,
                "clean_text": art.clean_text,

                # Label fields
                "label_id": lbl.id,
                "label_task": lbl.task,
                "label_model": lbl.model,
                "label_created_at": getattr(lbl, "created_at", None),

                # JSON expanded (minimalnie, zgodnie z Twoim obecnym promptem)
                "language": j.get("language"),
                "techniques": _list_to_str(j.get("techniques")),
                "evidence": _evidence_to_str(evidence),
                "confidence": j.get("confidence", None),
                "notes": j.get("notes", ""),

                # keep original JSON
                "label_json": lbl.json,
            }
            rec.update(stance_cols)
            records.append(rec)

    elif mode == "csv":
        if not articles_csv.exists():
            return pd.DataFrame()

        a = pd.read_csv(articles_csv)

        l = pd.read_csv(labels_csv) if labels_csv.exists() else pd.DataFrame()

        # ujednolicenia
        if "article_id" not in a.columns and "id" in a.columns:
            a = a.rename(columns={"id": "article_id"})
        if "label_json" not in l.columns and "json" in l.columns:
            l = l.rename(columns={"json": "label_json"})

        # filtr task/model
        if not l.empty:
            if "task" in l.columns:
                l = l[l["task"].astype(str) == str(task)]
            if "model" in l.columns:
                l = l[l["model"].astype(str) == str(model)]

        # najnowszy label per article_id
        if l.empty:
            df0 = a.copy()
            df0["label_id"] = None
            df0["label_task"] = task
            df0["label_model"] = model
            df0["label_created_at"] = None
            df0["label_json"] = None
        else:
            l2 = l.copy()
            if "created_at" in l2.columns:
                l2 = l2.sort_values("created_at", ascending=False)
                l2 = l2.drop_duplicates(subset=["article_id"], keep="first")
                l2 = l2.rename(columns={"created_at": "label_created_at"})
            elif "id" in l2.columns:
                l2 = l2.sort_values("id", ascending=False)
                l2 = l2.drop_duplicates(subset=["article_id"], keep="first")

            # nazwijmy konsekwentnie
            if "id" in l2.columns:
                l2 = l2.rename(columns={"id": "label_id"})
            if "task" in l2.columns and "label_task" not in l2.columns:
                l2 = l2.rename(columns={"task": "label_task"})
            if "model" in l2.columns and "label_model" not in l2.columns:
                l2 = l2.rename(columns={"model": "label_model"})

            df0 = a.merge(l2, how="left", on="article_id", suffixes=("", "_lbl"))
            df0 = df0.loc[:, ~df0.columns.duplicated()]

        # rozwiń JSON
        for _, row in df0.iterrows():
            j = _safe_json_loads(str(row.get("label_json") or ""))

            stance = j.get("stance") if isinstance(j.get("stance"), dict) else {}
            evidence = j.get("evidence")

            rec = {
                "article_id": row.get("article_id"),
                "created_at": row.get("created_at"),
                "url": row.get("url"),
                "title": row.get("title"),
                "category": row.get("category"),
                "published_at": row.get("published_at"),
                "source_hint": row.get("source_hint"),
                "raw_html": row.get("raw_html"),
                "clean_text": row.get("clean_text"),

                "label_id": row.get("label_id"),
                "label_task": row.get("label_task", task),
                "label_model": row.get("label_model", model),
                "label_created_at": row.get("label_created_at"),

                "language": j.get("language"),
                "techniques": _list_to_str(j.get("techniques")),
                "evidence": _evidence_to_str(evidence),
                "confidence": j.get("confidence", None),
                "notes": j.get("notes", ""),

                "label_json": row.get("label_json"),
            }

            for k, v in stance.items():
                rec[f"stance_{k}"] = v

            records.append(rec)

    else:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(records)

    if not df.empty:
        # daty do wykresów
        df["published_dt"] = pd.to_datetime(df.get("published_at"), errors="coerce")
        df["label_created_dt"] = pd.to_datetime(df.get("label_created_at"), errors="coerce")
        df["article_created_dt"] = pd.to_datetime(df.get("created_at"), errors="coerce")

    return df


# -----------------------------
# Analityka stance
# -----------------------------
def stance_distribution(df: pd.DataFrame, stance_col: str) -> pd.DataFrame:
    if df.empty or stance_col not in df.columns:
        return pd.DataFrame({"liczba": [0, 0, 0, 0, 0]}, index=STANCE_VALUES_ORDER)

    s = pd.to_numeric(df[stance_col], errors="coerce")
    counts = s.value_counts(dropna=True).to_dict()
    out = {v: int(counts.get(v, 0)) for v in STANCE_VALUES_ORDER}
    return pd.DataFrame({"liczba": list(out.values())}, index=list(out.keys()))


def stance_trend(df: pd.DataFrame, stance_col: str, date_col: str = "published_dt") -> pd.DataFrame:
    if df.empty or stance_col not in df.columns or date_col not in df.columns:
        return pd.DataFrame()

    tmp = df[[date_col, stance_col]].copy()
    tmp[stance_col] = pd.to_numeric(tmp[stance_col], errors="coerce")
    tmp = tmp.dropna(subset=[date_col, stance_col])
    if tmp.empty:
        return pd.DataFrame()

    tmp["dzień"] = tmp[date_col].dt.date
    g = tmp.groupby("dzień")[stance_col].mean().reset_index()
    g = g.sort_values("dzień")
    g = g.set_index("dzień")
    return g


# -----------------------------
# Filtry
# -----------------------------
def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    st.sidebar.subheader("Filtry")

    # category
    if "category" in df.columns:
        cats = sorted([c for c in df["category"].dropna().astype(str).unique().tolist() if c.strip()])
        chosen_cat = st.sidebar.multiselect("Kategoria", options=cats, default=[])
        if chosen_cat:
            df = df[df["category"].astype(str).isin(chosen_cat)]

    # confidence
    if "confidence" in df.columns:
        conf_min, conf_max = st.sidebar.slider("Pewność (confidence) – min/max", 0.0, 1.0, (0.0, 1.0), 0.01)
        c = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.0)
        df = df[(c >= conf_min) & (c <= conf_max)]

    # full-text-ish search
    q = st.sidebar.text_input("Szukaj (title / clean_text / notes / evidence)", value="").strip()
    if q:
        ql = q.lower()

        def contains(row: pd.Series) -> bool:
            for col in ["title", "clean_text", "notes", "evidence"]:
                v = row.get(col)
                if isinstance(v, str) and ql in v.lower():
                    return True
            return False

        df = df[df.apply(contains, axis=1)]

    # techniques multiselect
    tech_options: List[str] = []
    if "techniques" in df.columns:
        all_tech = set()
        for v in df["techniques"].fillna("").astype(str).tolist():
            for t in [x.strip() for x in v.split(";") if x.strip()]:
                all_tech.add(t)
        tech_options = sorted(all_tech)

    chosen = st.sidebar.multiselect("Techniki wpływu", options=tech_options, default=[])
    if chosen and "techniques" in df.columns:
        def has_any(v: str) -> bool:
            vs = set([x.strip() for x in str(v).split(";") if x.strip()])
            return any(t in vs for t in chosen)

        df = df[df["techniques"].apply(has_any)]

    return df


# -----------------------------
# UI
# -----------------------------
def main() -> None:
    st.set_page_config(page_title="Disinfo Lab — panel", layout="wide")
    st.title("Disinfo Lab — panel analityczny")

    mode = detect_mode()
    db_path, articles_csv, labels_csv = storage_paths()
    st.caption(f"Storage mode: **{mode}**")

    if mode == "empty":
        ensure_storage()
        st.info("Brak danych storage. Utworzono pustą bazę sqlite. Uruchom ingest i labeling.")
        st.stop()

    tasks, models = list_tasks_models(mode)
    if not tasks or not models:
        st.warning("Brak etykiet w storage — uruchom ingest i labeling.")
        st.stop()

    st.sidebar.subheader("Źródło danych")
    st.sidebar.caption(f"DB: {cfg.db_url}" if mode == "sqlite" else f"CSV: {articles_csv.name} / {labels_csv.name}")

    task = st.sidebar.selectbox("Task (wariant etykietowania)", tasks, index=max(0, len(tasks) - 1))
    model = st.sidebar.selectbox("Model (LLM)", models, index=max(0, len(models) - 1))

    st.markdown(
        """
Panel służy do eksploracji artykułów oraz wyników automatycznego oznaczania przez LLM.
Workflow: **ingest → parse → label → dashboard**.

**Stance**: skala od **-2 do +2** (negatywnie … neutralnie … pozytywnie).  
**Evidence**: krótkie cytaty wspierające wnioski.  
**Confidence**: przybliżona pewność (0–1) do filtrowania/QA.
        """
    )

    with st.spinner("Wczytuję dane..."):
        df = load_joined_dataframe(mode=mode, task=task, model=model)

    if df.empty:
        st.warning("Brak danych dla wybranego task/model.")
        st.stop()

    # stance keys dynamic
    stance_keys = _detect_stance_keys_from_df(df)

    df_f = apply_filters(df)
    st.caption(f"Rekordy po filtrach: **{len(df_f)}** / {len(df)}")

    st.header("Wektory narracyjne — rozkłady i trendy")

    cols = st.columns(2)
    for i, key in enumerate(stance_keys):
        stance_col = f"stance_{key}"
        dist = stance_distribution(df_f, stance_col)

        with cols[i % 2]:
            st.subheader(f"Stance: {_stance_display_name(key)}")
            st.bar_chart(dist["liczba"])

            trend = stance_trend(df_f, stance_col, date_col="published_dt")
            if trend.empty:
                st.caption("Trend: brak dat lub brak danych stance po filtrach.")
            else:
                st.line_chart(trend[stance_col])

    st.header("Tabela danych — wszystkie pola rekordu")

    show_big_cols = st.checkbox(
        "Pokaż pełne `raw_html` i `clean_text` (duże kolumny, wolniej)",
        value=False,
    )

    df_view = df_f.copy()

    # skracanie dużych pól
    if not show_big_cols:
        if "raw_html" in df_view.columns:
            df_view["raw_html"] = df_view["raw_html"].fillna("").astype(str).str.slice(0, 500)
        if "clean_text" in df_view.columns:
            df_view["clean_text"] = df_view["clean_text"].fillna("").astype(str).str.slice(0, 800)

    ordered_cols = [
        "article_id", "url", "title", "category", "published_at", "created_at", "source_hint",
        *[f"stance_{k}" for k in stance_keys],
        "techniques", "evidence", "confidence", "language", "notes",
        "label_id", "label_task", "label_model", "label_created_at",
        "clean_text", "raw_html",
        "label_json",
    ]
    ordered_cols = [c for c in ordered_cols if c in df_view.columns]
    df_view = df_view[ordered_cols]

    st.dataframe(df_view, use_container_width=True, hide_index=True)

    st.subheader("Podgląd pojedynczego rekordu")
    pick = st.selectbox("Wybierz `article_id`", options=df_view["article_id"].tolist(), index=0)
    row = df_f[df_f["article_id"] == pick].iloc[0].to_dict()
    st.write(f"**URL:** {row.get('url')}")
    st.write(f"**Tytuł:** {row.get('title')}")
    st.write(f"**Kategoria:** {row.get('category')}")
    st.json(_safe_json_loads(str(row.get("label_json") or "{}")))

    st.subheader("Eksport")
    csv_bytes = df_view.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Pobierz CSV (rekordy po filtrach)",
        data=csv_bytes,
        file_name=f"disinfo_lab_{task}_{model}_filtered.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
