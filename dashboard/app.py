from __future__ import annotations

import json
from typing import Any

import pandas as pd
import streamlit as st

from disinfo_lab.config import cfg
from disinfo_lab.db import connect
from disinfo_lab.storage import detect_mode, ensure_storage, storage_paths


DEFAULT_STANCE_KEYS = ["UE", "NATO", "USA", "Ukraina", "Rosja", "Rzad_Polski"]
STANCE_VALUES = [-2, -1, 0, 1, 2]
STANCE_LABELS = {"Rzad_Polski": "Rzad Polski"}


def parse_json(text: Any) -> dict[str, Any]:
    if not text:
        return {}
    try:
        data = json.loads(text)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def join_list(value: Any) -> str:
    if isinstance(value, list):
        return "; ".join(str(item).strip() for item in value if str(item).strip())
    if value is None:
        return ""
    return str(value).strip()


def flatten_evidence(value: Any) -> str:
    if isinstance(value, dict):
        parts = []
        for key, item in value.items():
            text = join_list(item)
            if text:
                parts.append(f"{key}: {text}")
        return " | ".join(parts)
    return join_list(value)


def stance_name(key: str) -> str:
    return STANCE_LABELS.get(key, key)


def ordered_stance_keys(df: pd.DataFrame) -> list[str]:
    keys = [column.removeprefix("stance_") for column in df.columns if column.startswith("stance_")]
    ordered = [key for key in DEFAULT_STANCE_KEYS if key in keys]
    extras = sorted(key for key in keys if key not in ordered)
    return ordered + extras or DEFAULT_STANCE_KEYS


@st.cache_data(show_spinner=False, ttl=60)
def list_tasks_models(mode: str) -> tuple[list[str], list[str]]:
    _, _, labels_csv = storage_paths()

    if mode == "sqlite":
        with connect(cfg.db_url) as con:
            tasks = [row["task"] for row in con.execute("SELECT DISTINCT task FROM llm_labels WHERE task IS NOT NULL ORDER BY task")]
            models = [row["model"] for row in con.execute("SELECT DISTINCT model FROM llm_labels WHERE model IS NOT NULL ORDER BY model")]
        return tasks, models

    if mode == "csv" and labels_csv.exists():
        labels = pd.read_csv(labels_csv)
        tasks = sorted(x for x in labels.get("task", pd.Series(dtype=str)).dropna().astype(str).unique() if x.strip())
        models = sorted(x for x in labels.get("model", pd.Series(dtype=str)).dropna().astype(str).unique() if x.strip())
        return tasks, models

    return [], []


def _latest_labels_sql(task: str, model: str) -> str:
    return """
        SELECT
            a.id AS article_id,
            a.created_at,
            a.url,
            a.title,
            a.category,
            a.published_at,
            a.source_hint,
            a.raw_html,
            a.clean_text,
            l.id AS label_id,
            l.task AS label_task,
            l.model AS label_model,
            l.created_at AS label_created_at,
            l.json AS label_json
        FROM articles a
        JOIN (
            SELECT article_id, MAX(id) AS max_id
            FROM llm_labels
            WHERE task = ? AND model = ?
            GROUP BY article_id
        ) latest ON latest.article_id = a.id
        JOIN llm_labels l ON l.id = latest.max_id
        ORDER BY a.id DESC
    """


def expand_label_json(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    rows: list[dict[str, Any]] = []
    for record in df.to_dict(orient="records"):
        label = parse_json(record.get("label_json"))
        stance = label.get("stance") if isinstance(label.get("stance"), dict) else {}

        record["language"] = label.get("language")
        record["techniques"] = join_list(label.get("techniques"))
        record["evidence"] = flatten_evidence(label.get("evidence"))
        record["confidence"] = label.get("confidence")
        record["notes"] = label.get("notes", "")

        for key, value in stance.items():
            record[f"stance_{key}"] = value

        rows.append(record)

    out = pd.DataFrame(rows)
    out["published_dt"] = pd.to_datetime(out.get("published_at"), errors="coerce", format="mixed")
    out["label_created_dt"] = pd.to_datetime(out.get("label_created_at"), errors="coerce")
    out["article_created_dt"] = pd.to_datetime(out.get("created_at"), errors="coerce")
    return out


@st.cache_data(show_spinner=True, ttl=60)
def load_joined_dataframe(mode: str, task: str, model: str) -> pd.DataFrame:
    _, articles_csv, labels_csv = storage_paths()

    if mode == "sqlite":
        with connect(cfg.db_url) as con:
            df = pd.read_sql_query(_latest_labels_sql(task, model), con, params=[task, model])
        return expand_label_json(df)

    if mode == "csv" and articles_csv.exists():
        articles = pd.read_csv(articles_csv).rename(columns={"id": "article_id"})
        if labels_csv.exists():
            labels = pd.read_csv(labels_csv).rename(columns={"id": "label_id", "task": "label_task", "model": "label_model", "json": "label_json"})
            labels = labels[
                (labels["label_task"].astype(str) == str(task))
                & (labels["label_model"].astype(str) == str(model))
            ]
            if "created_at" in labels.columns:
                labels = labels.sort_values("created_at", ascending=False).drop_duplicates("article_id", keep="first")
                labels = labels.rename(columns={"created_at": "label_created_at"})
            else:
                labels = labels.sort_values("label_id", ascending=False).drop_duplicates("article_id", keep="first")
            df = articles.merge(labels, how="left", on="article_id")
        else:
            df = articles.copy()
            df["label_id"] = None
            df["label_task"] = task
            df["label_model"] = model
            df["label_created_at"] = None
            df["label_json"] = None
        return expand_label_json(df)

    return pd.DataFrame()


def stance_distribution(df: pd.DataFrame, column: str) -> pd.DataFrame:
    values = pd.to_numeric(df.get(column), errors="coerce")
    counts = values.value_counts().to_dict()
    return pd.DataFrame({"liczba": [int(counts.get(value, 0)) for value in STANCE_VALUES]}, index=STANCE_VALUES)


def stance_trend(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column not in df.columns or "published_dt" not in df.columns:
        return pd.DataFrame()

    trend = df[["published_dt", column]].copy()
    trend[column] = pd.to_numeric(trend[column], errors="coerce")
    trend = trend.dropna(subset=["published_dt", column])
    if trend.empty:
        return pd.DataFrame()

    trend["day"] = trend["published_dt"].dt.date
    return trend.groupby("day")[column].mean().to_frame()


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    st.sidebar.subheader("Filtry")

    categories = sorted(x for x in df.get("category", pd.Series(dtype=str)).dropna().astype(str).unique() if x.strip())
    chosen_categories = st.sidebar.multiselect("Kategoria", categories)
    if chosen_categories:
        df = df[df["category"].astype(str).isin(chosen_categories)]

    confidence = pd.to_numeric(df.get("confidence"), errors="coerce").fillna(0.0)
    min_conf, max_conf = st.sidebar.slider("Pewnosc (confidence)", 0.0, 1.0, (0.0, 1.0), 0.01)
    df = df[(confidence >= min_conf) & (confidence <= max_conf)]

    query = st.sidebar.text_input("Szukaj (title / clean_text / notes / evidence)", "").strip().lower()
    if query:
        mask = df[["title", "clean_text", "notes", "evidence"]].fillna("").astype(str).apply(
            lambda column: column.str.lower().str.contains(query, regex=False)
        )
        df = df[mask.any(axis=1)]

    techniques: set[str] = set()
    for value in df.get("techniques", pd.Series(dtype=str)).fillna("").astype(str):
        techniques.update(item.strip() for item in value.split(";") if item.strip())

    chosen_techniques = st.sidebar.multiselect("Techniki wplywu", sorted(techniques))
    if chosen_techniques:
        chosen_set = set(chosen_techniques)
        df = df[
            df["techniques"].fillna("").astype(str).apply(
                lambda value: bool(chosen_set & {item.strip() for item in value.split(";") if item.strip()})
            )
        ]

    return df


def build_table(df: pd.DataFrame, stance_keys: list[str], show_full_text: bool) -> pd.DataFrame:
    table = df.copy()
    if not show_full_text:
        if "raw_html" in table.columns:
            table["raw_html"] = table["raw_html"].fillna("").astype(str).str.slice(0, 500)
        if "clean_text" in table.columns:
            table["clean_text"] = table["clean_text"].fillna("").astype(str).str.slice(0, 800)

    ordered_columns = [
        "article_id",
        "url",
        "title",
        "category",
        "published_at",
        "created_at",
        "source_hint",
        *[f"stance_{key}" for key in stance_keys],
        "techniques",
        "evidence",
        "confidence",
        "language",
        "notes",
        "label_id",
        "label_task",
        "label_model",
        "label_created_at",
        "clean_text",
        "raw_html",
        "label_json",
    ]
    return table[[column for column in ordered_columns if column in table.columns]]


def main() -> None:
    st.set_page_config(page_title="Disinfo Lab", layout="wide")
    st.title("Disinfo Lab")

    mode = detect_mode()
    _, articles_csv, labels_csv = storage_paths()
    st.caption(f"Storage mode: {mode}")

    if mode == "empty":
        ensure_storage()
        st.info("Brak danych. Utworzylem pusta baze sqlite, teraz mozna uruchomic ingest i labeling.")
        st.stop()

    tasks, models = list_tasks_models(mode)
    if not tasks or not models:
        st.warning("Brak etykiet. Najpierw uruchom ingest i labeling.")
        st.stop()

    st.sidebar.subheader("Zrodlo danych")
    if mode == "sqlite":
        st.sidebar.caption(f"DB: {cfg.db_url}")
    else:
        st.sidebar.caption(f"CSV: {articles_csv.name} / {labels_csv.name}")

    task = st.sidebar.selectbox("Task", tasks, index=len(tasks) - 1)
    model = st.sidebar.selectbox("Model", models, index=len(models) - 1)

    st.write("Workflow: ingest -> parse -> label -> dashboard")

    with st.spinner("Wczytuje dane..."):
        df = load_joined_dataframe(mode, task, model)

    if df.empty:
        st.warning("Brak danych dla wybranego task/model.")
        st.stop()

    filtered = apply_filters(df)
    stance_keys = ordered_stance_keys(filtered)

    st.caption(f"Rekordy po filtrach: {len(filtered)} / {len(df)}")
    st.header("Wektory narracyjne")

    columns = st.columns(2)
    for index, key in enumerate(stance_keys):
        stance_column = f"stance_{key}"
        with columns[index % 2]:
            st.subheader(f"Stance: {stance_name(key)}")
            st.bar_chart(stance_distribution(filtered, stance_column)["liczba"])

            trend = stance_trend(filtered, stance_column)
            if trend.empty:
                st.caption("Brak trendu dla tej osi.")
            else:
                st.line_chart(trend[stance_column])

    st.header("Tabela danych")
    show_full_text = st.checkbox("Pokaz pelne raw_html i clean_text", value=False)
    table = build_table(filtered, stance_keys, show_full_text)
    st.dataframe(table, use_container_width=True, hide_index=True)

    st.subheader("Podglad rekordu")
    article_id = st.selectbox("Wybierz article_id", options=table["article_id"].tolist(), index=0)
    row = filtered[filtered["article_id"] == article_id].iloc[0]
    st.write(f"URL: {row.get('url')}")
    st.write(f"Tytul: {row.get('title')}")
    st.write(f"Kategoria: {row.get('category')}")
    st.json(parse_json(row.get("label_json")))

    st.subheader("Eksport")
    st.download_button(
        "Pobierz CSV",
        data=table.to_csv(index=False).encode("utf-8"),
        file_name=f"disinfo_lab_{task}_{model}_filtered.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
