from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from disinfo_lab.db import init_db, sqlite_path_from_db_url
from disinfo_lab.config import cfg, assert_cfg

assert_cfg(cfg)


ARTICLES_CSV = "articles.csv"
LABELS_CSV = "llm_labels.csv"


def _data_dir() -> Path:
    d = Path(cfg.data_dir).expanduser().resolve()
    d.mkdir(parents=True, exist_ok=True)
    return d


def storage_paths() -> Tuple[Path, Path, Path]:
    d = _data_dir()
    try:
        db_path = sqlite_path_from_db_url(cfg.db_url)
    except ValueError:
        # fallback: trzymamy db w data/ jeśli db_url nie wskazuje sqlite
        db_path = (d / "disinfo_lab.sqlite3").resolve()
    return db_path, (d / ARTICLES_CSV), (d / LABELS_CSV)


def ensure_storage() -> Path:
    """
    Gwarantuje, że na końcu masz lokalną sqlite3 (do ingest/label).
    - jeśli sqlite istnieje -> OK
    - jeśli nie ma sqlite, ale są CSV -> odtwórz sqlite z CSV
    - jeśli nie ma nic -> utwórz pustą sqlite
    Zwraca ścieżkę do pliku sqlite3.
    """
    db_path, articles_csv, labels_csv = storage_paths()

    if db_path.exists():
        return db_path

    # Tworzymy pustą bazę
    init_db(f"sqlite:///{db_path.as_posix()}")

    # Jeśli CSV istnieją, rehydratacja
    if articles_csv.exists():
        import_csv_to_sqlite(db_path, articles_csv, labels_csv if labels_csv.exists() else None)

    return db_path


def export_sqlite_to_csv(db_path: Path) -> None:
    """
    Eksportuje tabele articles i llm_labels do CSV (mirror).
    """
    _, articles_csv, labels_csv = storage_paths()

    con = sqlite3.connect(db_path.as_posix())
    try:
        # articles
        df_a = pd.read_sql_query("SELECT * FROM articles ORDER BY id ASC", con)
        df_a.to_csv(articles_csv.as_posix(), index=False)

        # llm_labels
        df_l = pd.read_sql_query("SELECT * FROM llm_labels ORDER BY id ASC", con)
        df_l.to_csv(labels_csv.as_posix(), index=False)
    finally:
        con.close()


def import_csv_to_sqlite(db_path: Path, articles_csv: Path, labels_csv: Optional[Path]) -> None:
    """
    Wczytuje CSV do sqlite (rehydratacja).
    Robi to ostrożnie: czyści tabele i ładuje od nowa.
    """
    con = sqlite3.connect(db_path.as_posix())
    try:
        cur = con.cursor()
        # upewnij się, że tabele istnieją
        # (init_db już je zrobił)
        cur.execute("DELETE FROM llm_labels;")
        cur.execute("DELETE FROM articles;")
        con.commit()

        df_a = pd.read_csv(articles_csv.as_posix())
        df_a.to_sql("articles", con, if_exists="append", index=False)

        if labels_csv and labels_csv.exists():
            df_l = pd.read_csv(labels_csv.as_posix())
            if not df_l.empty:
                df_l.to_sql("llm_labels", con, if_exists="append", index=False)

        con.commit()
    finally:
        con.close()


def detect_mode() -> str:
    """
    Do dashboardu: czy używamy sqlite czy csv.
    """
    db_path, articles_csv, _ = storage_paths()
    if db_path.exists():
        return "sqlite"
    if articles_csv.exists():
        return "csv"
    return "empty"
