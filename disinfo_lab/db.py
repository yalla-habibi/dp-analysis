from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable, Optional


def sqlite_path_from_db_url(db_url: str) -> Path:
    if not db_url.startswith("sqlite:///"):
        raise ValueError(f"Only sqlite DB URLs are supported, got: {db_url}")

    raw_path = db_url.removeprefix("sqlite:///")
    path = Path(raw_path)
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    return path


def connect(db_url: str) -> sqlite3.Connection:
    path = sqlite_path_from_db_url(db_url)
    path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(path.as_posix())
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    return con


def init_db(db_url: str) -> None:
    with connect(db_url) as con:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                title TEXT,
                category TEXT,
                published_at TEXT,
                source_hint TEXT,
                raw_html TEXT,
                clean_text TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS llm_labels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER NOT NULL,
                model TEXT NOT NULL,
                task TEXT NOT NULL,
                json TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(article_id) REFERENCES articles(id) ON DELETE CASCADE,
                UNIQUE(article_id, model, task)
            );

            CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url);
            CREATE INDEX IF NOT EXISTS idx_llm_labels_article_id ON llm_labels(article_id);
            """
        )


def fetch_all(con: sqlite3.Connection, query: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    return list(con.execute(query, tuple(params)).fetchall())


def fetch_one(con: sqlite3.Connection, query: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
    return con.execute(query, tuple(params)).fetchone()
