from __future__ import annotations

from typing import Literal, Optional, List, TypedDict
from urllib.parse import urljoin

import httpx

from disinfo_lab.config import cfg, assert_cfg

assert_cfg(cfg)


def _wp_api_base() -> str:
    if cfg.wp_api_base.strip():
        base = cfg.wp_api_base.strip()
        if not base.endswith("/"):
            base += "/"
        return base
    return urljoin(cfg.base_url, "wp-json/wp/v2/")


class WPPostRef(TypedDict):
    link: str
    categories: List[int]


async def collect_all_urls(
    mode: Literal["wp-api", "sitemap"] = "wp-api",
    *,
    wp_category_id: Optional[int] = None,
    wp_per_page: int = 50,
    wp_pages: int = 1,
    sitemap_url: Optional[str] = None,
) -> List[str]:
    """
    Backward-compatible helper that returns only URLs.

    Prefer `collect_wp_posts()` if you need WP taxonomy fields like categories.
    """
    if mode == "wp-api":
        return await _collect_wp_urls(
            category_id=wp_category_id,
            per_page=wp_per_page,
            pages=wp_pages,
        )
    if mode == "sitemap":
        if not sitemap_url:
            sitemap_url = urljoin(cfg.base_url, "sitemap.xml")
        return await _collect_sitemap_urls(sitemap_url)
    raise ValueError(f"Unknown mode: {mode}")


async def collect_wp_posts(*, category_id: Optional[int], per_page: int, pages: int) -> List[WPPostRef]:
    """
    Collect posts from WordPress REST API including categories.

    Returns a deduplicated list (by link) of {link, categories}.
    """
    base = _wp_api_base()
    endpoint = urljoin(base, "posts")

    headers = {"User-Agent": cfg.user_agent}
    posts: List[WPPostRef] = []

    async with httpx.AsyncClient(timeout=cfg.request_timeout_s, headers=headers, follow_redirects=True) as client:
        for page in range(1, max(1, pages) + 1):
            params = {
                "per_page": min(max(1, per_page), 100),
                "page": page,
                "_fields": "link,categories",
                "orderby": "date",
                "order": "desc",
            }
            if category_id is not None:
                params["categories"] = str(category_id)

            r = await client.get(endpoint, params=params)
            r.raise_for_status()
            data = r.json()

            for obj in data:
                link = obj.get("link")
                if not link:
                    continue

                cats = obj.get("categories") or []
                # WP returns list[int], but be defensive
                try:
                    cats_int = [int(x) for x in cats]
                except Exception:
                    cats_int = []

                posts.append({"link": link, "categories": cats_int})

    # dedupe by link
    seen = set()
    out: List[WPPostRef] = []
    for p in posts:
        if p["link"] not in seen:
            seen.add(p["link"])
            out.append(p)
    return out


async def _collect_wp_urls(*, category_id: Optional[int], per_page: int, pages: int) -> List[str]:
    base = _wp_api_base()
    endpoint = urljoin(base, "posts")

    headers = {"User-Agent": cfg.user_agent}
    urls: List[str] = []

    async with httpx.AsyncClient(timeout=cfg.request_timeout_s, headers=headers, follow_redirects=True) as client:
        for page in range(1, max(1, pages) + 1):
            params = {
                "per_page": min(max(1, per_page), 100),
                "page": page,
                "_fields": "link",
                "orderby": "date",
                "order": "desc",
            }
            if category_id is not None:
                params["categories"] = str(category_id)

            r = await client.get(endpoint, params=params)
            r.raise_for_status()
            data = r.json()
            for obj in data:
                link = obj.get("link")
                if link:
                    urls.append(link)

    # dedupe
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


async def _collect_sitemap_urls(sitemap_url: str) -> List[str]:
    import re

    headers = {"User-Agent": cfg.user_agent}
    async with httpx.AsyncClient(timeout=cfg.request_timeout_s, headers=headers, follow_redirects=True) as client:
        r = await client.get(sitemap_url)
        r.raise_for_status()
        xml = r.text

    locs = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", xml, flags=re.IGNORECASE)
    return [u for u in locs if u.startswith("http")]
