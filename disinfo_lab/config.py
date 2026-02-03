from __future__ import annotations

from dataclasses import dataclass
import os


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v else default
    except ValueError:
        return default


def _normalize_base(url: str) -> str:
    url = (url or "").strip()
    if not url.endswith("/"):
        url += "/"
    return url


@dataclass(frozen=True)
class Config:
    # --- źródło danych / strona ---
    base_url: str = _normalize_base(_env("DISINFO_BASE_URL", "https://d1kgvz5kzmht23.cloudfront.net/"))
    user_agent: str = _env("DISINFO_USER_AGENT", "disinfo-lab/1.0 (+https://example.local)")
    request_timeout_s: int = _env_int("DISINFO_TIMEOUT_S", 25)

    # --- WordPress REST API (opcjonalne) ---
    # Jeśli puste: crawl.py użyje base_url + "wp-json/wp/v2/"
    wp_api_base: str = _env("DISINFO_WP_API_BASE", "").strip()
    wp_per_page: int = _env_int("DISINFO_WP_PER_PAGE", 50)

    # --- DB ---
    db_url: str = _env("DISINFO_DB_URL", "sqlite:///disinfo_lab.sqlite3")

    # --- Ollama ---
    ollama_base_url: str = _env("OLLAMA_BASE_URL", "http://localhost:11434").strip()
    ollama_model: str = _env("OLLAMA_MODEL", "llama3.1:8b").strip()
    ollama_timeout_s: int = _env_int("OLLAMA_TIMEOUT_S", 120)


cfg = Config()


def assert_cfg(obj) -> None:
    if not isinstance(obj, Config):
        raise RuntimeError(
            "Config import error: expected `cfg` to be an instance of Config.\n"
            "Use: `from disinfo_lab.config import cfg` everywhere.\n"
            f"Got: {type(obj)}"
        )
