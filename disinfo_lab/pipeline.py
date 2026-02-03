from __future__ import annotations

import json
from typing import Iterable, Optional, Tuple

from disinfo_lab.config import cfg, assert_cfg
from disinfo_lab.db import init_db, make_session, Article, LLMLabel
from disinfo_lab.crawl import collect_all_urls
from disinfo_lab.parse import fetch_article_html, extract_clean_text, parse_meta, infer_category_from_url
from disinfo_lab.llm_label import ollama_label
from disinfo_lab.storage import ensure_storage, export_sqlite_to_csv, sqlite_path_from_db_url

assert_cfg(cfg)

# Upewnij się, że DB istnieje (albo została odtworzona z CSV)
ensure_storage()

Session = make_session(cfg.db_url)


def _export_mirror_csv() -> None:
    p = sqlite_path_from_db_url(cfg.db_url)
    if p is not None and p.exists():
        export_sqlite_to_csv(p)


async def ingest_urls(urls: Iterable[str], forced_category: Optional[str] = None) -> Tuple[int, int, int]:
    init_db(cfg.db_url)

    added = skipped = failed = 0
    with Session() as s:
        for url in urls:
            if s.query(Article).filter_by(url=url).first():
                skipped += 1
                continue

            try:
                html = await fetch_article_html(url)
                meta = parse_meta(html)
                text = extract_clean_text(html)
                category = forced_category or meta.get("category") or infer_category_from_url(url)

                s.add(
                    Article(
                        url=url,
                        title=meta.get("title"),
                        category=category,
                        published_at=meta.get("published_at"),
                        source_hint=meta.get("source_hint"),
                        raw_html=html,
                        clean_text=text,
                    )
                )
                s.commit()
                added += 1
            except Exception:
                s.rollback()
                failed += 1

    # mirror CSV po ingest
    _export_mirror_csv()
    return added, skipped, failed


async def ingest_latest_wp(category_id: Optional[int], limit: int = 50) -> Tuple[int, int, int]:
    urls = await collect_all_urls(
        mode="wp-api",
        wp_category_id=category_id,
        wp_per_page=limit,
        wp_pages=1,
    )
    forced = str(category_id) if category_id is not None else None
    return await ingest_urls(urls, forced_category=forced)


async def label_latest(task: str, batch_limit: int = 200, category_filter: str | None = None) -> Tuple[int, int, int]:
    init_db(cfg.db_url)

    added = skipped = failed = 0
    with Session() as s:
        q = s.query(Article)
        if category_filter:
            q = q.filter(Article.category == category_filter)

        arts = q.order_by(Article.id.desc()).limit(batch_limit).all()

        for a in arts:
            if not (a.clean_text or "").strip():
                skipped += 1
                continue

            exists = (
                s.query(LLMLabel)
                .filter_by(article_id=a.id, model=cfg.ollama_model, task=task)
                .first()
            )
            if exists:
                skipped += 1
                continue

            try:
                payload = f"TITLE: {a.title or ''}\nURL: {a.url}\n\nTEXT:\n{a.clean_text}"
                lbl = await ollama_label(payload)

                s.add(
                    LLMLabel(
                        article_id=a.id,
                        model=cfg.ollama_model,
                        task=task,
                        json=json.dumps(lbl, ensure_ascii=False),
                    )
                )
                s.commit()
                added += 1
            except Exception as e:
                s.rollback()
                failed += 1
                print(f"[label_latest] FAILED article_id={a.id} err={type(e).__name__}: {e}")

    # mirror CSV po label
    _export_mirror_csv()
    return added, skipped, failed
