from __future__ import annotations

import re
from typing import Optional, Dict
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from disinfo_lab.config import cfg, assert_cfg

assert_cfg(cfg)


async def fetch_article_html(url: str) -> str:
    headers = {"User-Agent": cfg.user_agent}
    async with httpx.AsyncClient(timeout=cfg.request_timeout_s, headers=headers, follow_redirects=True) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.text


def extract_clean_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    container = soup.find("article") or soup.find("main") or soup.body or soup
    text = container.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def parse_meta(html: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")

    title: Optional[str] = None
    if soup.title and soup.title.get_text(strip=True):
        title = soup.title.get_text(strip=True)

    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        title = og["content"].strip()

    published_at: Optional[str] = None
    txt = soup.get_text(separator="\n", strip=True)
    m = re.search(r"\b(\d{1,2}\s+[A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż]+(?:,)?\s+\d{4})\b", txt)
    if m:
        published_at = m.group(1)

    source_hint: Optional[str] = None
    m2 = re.search(r"Źródło:\s*([^\n\r]+)", txt)
    if m2:
        source_hint = m2.group(1).strip()

    return {
        "title": title,
        "published_at": published_at,
        "source_hint": source_hint,
        "category": None,
    }


def infer_category_from_url(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
        parts = [x for x in p.path.split("/") if x]
        return parts[0] if parts else None
    except Exception:
        return None
