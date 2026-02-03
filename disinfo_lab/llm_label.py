from __future__ import annotations

import json
from typing import Any, Dict

import httpx

from disinfo_lab.config import cfg, assert_cfg

assert_cfg(cfg)


SYSTEM = """Jesteś klasyfikatorem OSINT / cyber-intelligence.
Zwracaj WYŁĄCZNIE poprawny JSON (bez markdown, bez komentarzy).
Jeśli nie wiesz, ustaw null i dodaj pole "notes".
"""


def _label_prompt(text: str) -> str:
    return f"""
Przeanalizuj tekst i zwróć JSON o strukturze:

{{
  "language": "pl|en|other|null",
  "stance": {{
    "UE": -2..2|null,
    "NATO": -2..2|null,
    "USA": -2..2|null,
    "Ukraina": -2..2|null,
    "Rosja": -2..2|null,
    "Rzad_Polski": -2..2|null
  }},
  "techniques": [ "string", ... ],
  "evidence": {{
    "UE": ["cytat...", ...],
    "NATO": ["cytat...", ...],
    "USA": ["cytat...", ...],
    "Ukraina": ["cytat...", ...],
    "Rosja": ["cytat...", ...],
    "Rzad_Polski": ["cytat...", ...]
  }},
  "confidence": 0..1|null,
  "notes": "string|null"
}}

Zasady:
- Zwróć WYŁĄCZNIE JSON.
- Skala stance: -2..+2
- Evidence: krótkie cytaty, max 3 per oś

Tekst:
{text}
""".strip()


async def ollama_label(text: str) -> Dict[str, Any]:
    url = cfg.ollama_base_url.rstrip("/") + "/api/chat"
    payload = {
        "model": cfg.ollama_model,
        "stream": False,
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": _label_prompt(text)},
        ],
        "options": {"temperature": 0.2},
    }

    async with httpx.AsyncClient(timeout=cfg.ollama_timeout_s) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        data = r.json()

    content = (data.get("message") or {}).get("content") or ""
    content = content.strip()
    return _extract_json_object(content)


def _extract_json_object(s: str) -> Dict[str, Any]:
    s = s.strip()
    try:
        return json.loads(s)
    except Exception:
        pass

    i = s.find("{")
    j = s.rfind("}")
    if i != -1 and j != -1 and j > i:
        chunk = s[i : j + 1]
        try:
            return json.loads(chunk)
        except Exception:
            return {"notes": "Could not parse JSON chunk", "raw": s, "chunk": chunk}

    return {"notes": "Could not parse JSON", "raw": s}
