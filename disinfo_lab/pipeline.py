from __future__ import annotations

from datetime import datetime, UTC
import html as html_mod
import json
import re
import sqlite3
from typing import Iterable, Optional

from disinfo_lab.config import cfg, assert_cfg
from disinfo_lab.db import connect, init_db
from disinfo_lab.crawl import collect_wp_posts, WPPostRef
from disinfo_lab.parse import fetch_article_html, extract_clean_text, parse_meta, infer_category_from_url
from disinfo_lab.llm_label import STANCE_KEYS, ollama_label
from disinfo_lab.storage import ensure_storage, export_sqlite_to_csv, sqlite_path_from_db_url

assert_cfg(cfg)

# Make sure storage exists before any pipeline work starts.
ensure_storage()

WP_CATEGORY_MAP = {
    9: "opinia",
}

AXIS_KEYWORDS = {
    "UE": ["ue", "unia europejska", "unia", "bruksela", "komisja europejska", "parlament europejski"],
    "NATO": ["nato", "sojusz", "pakt polnocnoatlantycki", "polnocnoatlantycki"],
    "USA": ["usa", "stany zjednoczone", "ameryka", "waszyngton", "biden", "pentagon"],
    "Ukraina": ["ukraina", "kijow", "zelenski", "donbas", "charkow", "odesa"],
    "Rosja": ["rosja", "kreml", "moskwa", "putin", "federacja rosyjska", "rf"],
    "Niemcy": ["niemcy", "berlin", "scholz", "bundestag"],
    "Litwa": ["litwa", "wilno", "vilnius"],
    "Bialorus": ["bialorus", "bialoruś", "minsk", "mińsk", "lukaszenka", "łukaszenka"],
    "Rzad_Polski": ["rzad", "rząd", "premier", "minist", "koalicja", "sejm", "kancelaria premiera", "rząd rp", "rzad rp"],
}


def _utc_now_str() -> str:
    return datetime.now(UTC).replace(tzinfo=None).isoformat(sep=" ")


def _export_mirror_csv() -> None:
    path = sqlite_path_from_db_url(cfg.db_url)
    if path is not None and path.exists():
        export_sqlite_to_csv(path)


def _category_from_wp_categories(category_ids: list[int]) -> Optional[str]:
    for category_id in category_ids:
        if category_id in WP_CATEGORY_MAP:
            return WP_CATEGORY_MAP[category_id]
    return None


def _article_exists(con: sqlite3.Connection, url: str) -> bool:
    row = con.execute("SELECT 1 FROM articles WHERE url = ? LIMIT 1", (url,)).fetchone()
    return row is not None


def _insert_article(
    con: sqlite3.Connection,
    *,
    url: str,
    title: Optional[str],
    category: Optional[str],
    published_at: Optional[str],
    source_hint: Optional[str],
    raw_html: str,
    clean_text: str,
) -> None:
    con.execute(
        """
        INSERT INTO articles (
            url, title, category, published_at, source_hint, raw_html, clean_text, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            url,
            title,
            category,
            published_at,
            source_hint,
            raw_html,
            clean_text,
            _utc_now_str(),
        ),
    )


def _shorten(text: str | None, limit: int = 90) -> str:
    if not text:
        return ""
    text = text.strip().replace("\n", " ")
    return text if len(text) <= limit else text[: limit - 3] + "..."


def sanitize_text(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        return ""

    if "<" in cleaned and ">" in cleaned:
        cleaned = re.sub(r"(?is)<script.*?>.*?</script>", " ", cleaned)
        cleaned = re.sub(r"(?is)<style.*?>.*?</style>", " ", cleaned)
        cleaned = re.sub(r"(?is)<noscript.*?>.*?</noscript>", " ", cleaned)
        cleaned = re.sub(r"(?is)<[^>]+>", " ", cleaned)

    cleaned = html_mod.unescape(cleaned)

    junk_patterns = [
        r"(?i)cookies?",
        r"(?i)polityka prywatnosci",
        r"(?i)polityka prywatności",
        r"(?i)udostepnij|udostępnij|share|tweet|facebook|x\.com",
        r"(?i)subskrybuj|newsletter",
        r"(?i)zaloguj|logowanie|rejestracja",
        r"(?i)komentarze?\b",
    ]
    for pattern in junk_patterns:
        cleaned = re.sub(pattern, " ", cleaned)

    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def detect_axes(text: str) -> list[str]:
    lowered = (text or "").casefold()
    found: list[str] = []
    for axis, keywords in AXIS_KEYWORDS.items():
        for keyword in keywords:
            if keyword.casefold() in lowered:
                found.append(axis)
                break
    return found


def sentence_split(text: str) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []

    parts = re.split(r"(?<=[\.\!\?\…])\s+", text)
    return [part.strip() for part in parts if len(part.strip()) >= 20]


def axis_focused_excerpt(text: str, *, max_chars: int = 2600, max_sentences: int = 22) -> str:
    cleaned = sanitize_text(text)
    sentences = sentence_split(cleaned)
    if not sentences:
        return cleaned[:max_chars]

    chosen: list[str] = []
    for sentence in sentences[:3]:
        chosen.append(sentence)

    lowered_sentences = [(sentence, sentence.casefold()) for sentence in sentences]
    for _, keywords in AXIS_KEYWORDS.items():
        for sentence, lowered in lowered_sentences:
            if any(keyword.casefold() in lowered for keyword in keywords):
                if sentence not in chosen:
                    chosen.append(sentence)
                if len(chosen) >= max_sentences:
                    break
        if len(chosen) >= max_sentences:
            break

    for sentence in sentences[-2:]:
        if sentence not in chosen:
            chosen.append(sentence)

    excerpt = "\n".join(chosen)
    if len(excerpt) > max_chars:
        excerpt = excerpt[:max_chars].rstrip()
    return excerpt.strip()


def make_llm_input(title: str | None, text: str) -> tuple[str, list[str]]:
    cleaned = sanitize_text(text)
    axes = detect_axes(cleaned)
    excerpt = axis_focused_excerpt(cleaned)
    axes_line = ", ".join(axes) if axes else "(brak)"

    llm_input = (
        f"TYTUL: {title or ''}\n"
        f"WYKRYTE_OSIE: {axes_line}\n"
        f"TEKST:\n{excerpt}\n"
    )
    return llm_input, axes


def all_stance_zero(label: dict[str, object]) -> bool:
    stance = label.get("stance") if isinstance(label, dict) else {}
    if not isinstance(stance, dict):
        return True

    for key in STANCE_KEYS:
        try:
            if int(stance.get(key, 0)) != 0:
                return False
        except Exception:
            continue
    return True


def log_saved(article: sqlite3.Row, label: dict[str, object]) -> None:
    stance = label.get("stance") if isinstance(label, dict) else {}
    techniques = label.get("techniques") if isinstance(label, dict) else []
    confidence = label.get("confidence") if isinstance(label, dict) else None
    evidence = label.get("evidence") if isinstance(label, dict) else []

    compact_stance = {key: stance.get(key) for key in STANCE_KEYS} if isinstance(stance, dict) else {}
    evidence_list = evidence if isinstance(evidence, list) else []

    print(
        "[saved] "
        f"url={article['url']} | conf={confidence} | stance={compact_stance} | tech={techniques} | "
        f"ev1={_shorten(evidence_list[0] if len(evidence_list) > 0 else '')!r} | "
        f"ev2={_shorten(evidence_list[1] if len(evidence_list) > 1 else '')!r} | "
        f"ev3={_shorten(evidence_list[2] if len(evidence_list) > 2 else '')!r} | "
        f"title={_shorten(article['title'])!r}",
        flush=True,
    )


async def ingest_urls(urls: Iterable[str], forced_category: Optional[str] = None) -> tuple[int, int, int]:
    init_db(cfg.db_url)

    added = skipped = failed = 0
    with connect(cfg.db_url) as con:
        for url in urls:
            if _article_exists(con, url):
                skipped += 1
                continue

            try:
                html = await fetch_article_html(url)
                meta = parse_meta(html)
                text = extract_clean_text(html)
                category = forced_category or meta.get("category") or infer_category_from_url(url)

                _insert_article(
                    con,
                    url=url,
                    title=meta.get("title"),
                    category=category,
                    published_at=meta.get("published_at"),
                    source_hint=meta.get("source_hint"),
                    raw_html=html,
                    clean_text=text,
                )
                con.commit()
                added += 1
            except Exception:
                con.rollback()
                failed += 1

    _export_mirror_csv()
    return added, skipped, failed


async def ingest_latest_wp(category_id: Optional[int], limit: int = 50) -> tuple[int, int, int]:
    init_db(cfg.db_url)

    posts: list[WPPostRef] = await collect_wp_posts(
        category_id=category_id,
        per_page=limit,
        pages=1,
    )
    forced_category = WP_CATEGORY_MAP.get(category_id) if category_id is not None else None

    added = skipped = failed = 0
    with connect(cfg.db_url) as con:
        for post in posts:
            url = post["link"]
            if _article_exists(con, url):
                skipped += 1
                continue

            try:
                html = await fetch_article_html(url)
                meta = parse_meta(html)
                text = extract_clean_text(html)

                category = (
                    forced_category
                    or _category_from_wp_categories(post.get("categories", []))
                    or meta.get("category")
                    or infer_category_from_url(url)
                )

                _insert_article(
                    con,
                    url=url,
                    title=meta.get("title"),
                    category=category,
                    published_at=meta.get("published_at"),
                    source_hint=meta.get("source_hint"),
                    raw_html=html,
                    clean_text=text,
                )
                con.commit()
                added += 1
            except Exception:
                con.rollback()
                failed += 1

    _export_mirror_csv()
    return added, skipped, failed


async def label_latest(
    task: str,
    batch_limit: int = 200,
    category_filter: str | None = None,
) -> tuple[int, int, int]:
    init_db(cfg.db_url)

    articles_query = """
        SELECT id, url, title, category, clean_text
        FROM articles
        {where_clause}
        ORDER BY id DESC
        LIMIT ?
    """
    where_clause = "WHERE category = ?" if category_filter else ""
    params: list[object] = [category_filter, batch_limit] if category_filter else [batch_limit]

    added = skipped = failed = retried = 0
    with connect(cfg.db_url) as con:
        articles = con.execute(articles_query.format(where_clause=where_clause), params).fetchall()

        for index, article in enumerate(articles, start=1):
            clean_text = (article["clean_text"] or "").strip()
            if not clean_text:
                skipped += 1
                continue

            exists = con.execute(
                """
                SELECT 1
                FROM llm_labels
                WHERE article_id = ? AND model = ? AND task = ?
                LIMIT 1
                """,
                (article["id"], cfg.ollama_model, task),
            ).fetchone()
            if exists:
                skipped += 1
                continue

            try:
                llm_input, axes = make_llm_input(article["title"], clean_text)
                print(f"[{index}/{len(articles)}] label -> {article['url']}", flush=True)
                label = await ollama_label(llm_input)

                if axes and all_stance_zero(label):
                    retried += 1
                    retry_input = (
                        llm_input
                        + "\nUWAGA: WYKRYTE_OSIE nie sa puste. Co najmniej jedna z nich musi miec stance != 0. "
                          "Jesli sygnaly sa slabe, wybierz +/-1 i obniz confidence.\n"
                    )
                    print(f"[{index}/{len(articles)}] retry(all-zero) -> {article['url']}", flush=True)
                    label = await ollama_label(retry_input)

                con.execute(
                    """
                    INSERT INTO llm_labels (article_id, model, task, json, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        article["id"],
                        cfg.ollama_model,
                        task,
                        json.dumps(label, ensure_ascii=False),
                        _utc_now_str(),
                    ),
                )
                con.commit()
                added += 1
                log_saved(article, label)
            except Exception as exc:
                con.rollback()
                failed += 1
                print(
                    f"[{index}/{len(articles)}] LABEL FAILED article_id={article['id']} "
                    f"url={article['url']} err={type(exc).__name__}: {exc}",
                    flush=True,
                )

    _export_mirror_csv()
    print(
        f"Done. added={added} skipped={skipped} failed={failed} retried={retried}",
        flush=True,
    )
    return added, skipped, failed
