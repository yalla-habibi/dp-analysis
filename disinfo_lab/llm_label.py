from __future__ import annotations

import json
import re
from typing import Any

import httpx

from disinfo_lab.config import cfg, assert_cfg

assert_cfg(cfg)

TECHNIQUES = [
    "cherry_picking",
    "false_causality",
    "whataboutism",
    "fear_appeal",
    "delegitimization",
]

STANCE_KEYS = [
    "UE",
    "NATO",
    "USA",
    "Ukraina",
    "Rosja",
    "Niemcy",
    "Litwa",
    "Bialorus",
    "Rzad_Polski",
]

DISPLAY_TO_INTERNAL = {
    "Białoruś": "Bialorus",
    "Rząd Polski": "Rzad_Polski",
}

INTERNAL_TO_DISPLAY = {
    "Bialorus": "Białoruś",
    "Rzad_Polski": "Rząd Polski",
}


def _norm_key(value: str) -> str:
    value = (value or "").strip().casefold()
    return re.sub(r"[^0-9a-ząćęłńóśżź]+", "", value)


STANCE_ALIASES = {
    "ue": "UE",
    "eu": "UE",
    "unia": "UE",
    "uniaeuropejska": "UE",
    "unieuropejska": "UE",
    "bruksela": "UE",
    "nato": "NATO",
    "usa": "USA",
    "us": "USA",
    "stanyzjednoczone": "USA",
    "stanyzjednoczoneameryki": "USA",
    "ameryka": "USA",
    "waszyngton": "USA",
    "ukraina": "Ukraina",
    "ukraine": "Ukraina",
    "kijow": "Ukraina",
    "kijów": "Ukraina",
    "rosja": "Rosja",
    "russia": "Rosja",
    "federacjarosyjska": "Rosja",
    "rf": "Rosja",
    "kreml": "Rosja",
    "moskwa": "Rosja",
    "niemcy": "Niemcy",
    "rfniemiec": "Niemcy",
    "republikafederalnaniemiec": "Niemcy",
    "berlin": "Niemcy",
    "litwa": "Litwa",
    "wilno": "Litwa",
    "vilnius": "Litwa",
    "białoruś": "Bialorus",
    "bialorus": "Bialorus",
    "mińsk": "Bialorus",
    "minsk": "Bialorus",
    "rządpolski": "Rzad_Polski",
    "rzadpolski": "Rzad_Polski",
    "rządrp": "Rzad_Polski",
    "rzadrp": "Rzad_Polski",
    "rząd": "Rzad_Polski",
    "rzad": "Rzad_Polski",
    "gabinet": "Rzad_Polski",
    "władza": "Rzad_Polski",
    "wladza": "Rzad_Polski",
    "warszawa": "Rzad_Polski",
}

SYSTEM_PL = f"""
Jesteś analitykiem treści. Zwracasz WYŁĄCZNIE poprawny JSON (bez markdown, bez komentarzy, bez dodatkowego tekstu).

Techniki (wybierz tylko z listy): {TECHNIQUES}
- cherry_picking: selektywny dobór faktów / brak kontekstu
- false_causality: fałszywa przyczynowość / insynuacje
- whataboutism: "a u was..." / relatywizacja zarzutów
- fear_appeal: straszenie / katastrofizacja
- delegitimization: delegitymizacja, "marionetki", odczłowieczanie

Skala stance (dla osi):
-2: mocno negatywny / delegitymizujący
-1: umiarkowanie negatywny
 0: neutralny / nie dotyczy / brak sygnałów
+1: umiarkowanie pozytywny
+2: mocno pozytywny / propagandowy

OSI STANCE (podaj zawsze wszystkie): {[
    INTERNAL_TO_DISPLAY.get(key, key) for key in STANCE_KEYS
]}

KLUCZOWE ZASADY:
1) stance musi zawierać wszystkie osie z listy powyżej.
2) 0 stosuj tylko gdy oś nie występuje w treści lub nie ma sygnałów oceny.
3) Jeśli w wejściu jest linia "WYKRYTE_OSIE:", co najmniej jedna z tych osi musi mieć stance != 0.
4) evidence: podaj 1-3 krótkie fragmenty z dostarczonej treści, max 20 słów każdy.
5) rationale: 1-2 krótkie zdania.
6) confidence: 0..1
7) language: "pl", "en" albo "other"
8) notes zostaw puste, jeśli nie ma problemu.
""".strip()

FEW_SHOT = r"""
Przykład:
WEJŚCIE:
TYTUL: ...
WYKRYTE_OSIE: UE, Niemcy, Rząd Polski
TEKST:
"Rząd w Warszawie działa na zlecenie Berlina i Brukseli, a NATO tylko eskaluje konflikt. Jeśli tego nie zatrzymamy, Polska zapłaci najwyższą cenę."

JSON:
{
  "language": "pl",
  "target_entities": ["Rząd Polski", "Niemcy", "UE", "NATO"],
  "target_audience_guess": ["odbiorcy antyestablishmentowi"],
  "stance": {"UE": -1, "NATO": -1, "USA": 0, "Ukraina": 0, "Rosja": 0, "Niemcy": -2, "Litwa": 0, "Białoruś": 0, "Rząd Polski": -2},
  "techniques": ["false_causality", "fear_appeal", "delegitimization"],
  "rationale": "Tekst delegitymizuje rząd i Niemcy oraz straszy konsekwencjami.",
  "evidence": ["działa na zlecenie Berlina i Brukseli", "Polska zapłaci najwyższą cenę"],
  "confidence": 0.73,
  "notes": ""
}
""".strip()


def _detect_axes(text: str) -> list[str]:
    found: list[str] = []
    haystack = _norm_key(text)

    for alias, canonical in STANCE_ALIASES.items():
        if alias in haystack and canonical not in found:
            found.append(canonical)

    ordered = [axis for axis in STANCE_KEYS if axis in found]
    return [INTERNAL_TO_DISPLAY.get(axis, axis) for axis in ordered]


def build_prompt(text: str) -> str:
    detected_axes = _detect_axes(text)
    has_axes_line = "WYKRYTE_OSIE:" in text
    detected_line = ""
    if detected_axes and not has_axes_line:
        detected_line = f"WYKRYTE_OSIE: {', '.join(detected_axes)}\n"
    return f"""{SYSTEM_PL}

{FEW_SHOT}

Zwróć WYŁĄCZNIE JSON o kluczach:
- language
- target_entities
- target_audience_guess
- stance
- techniques
- rationale
- evidence
- confidence
- notes

WEJŚCIE:
{detected_line}{text}
""".strip()


def _extract_first_json_block(text: str) -> str | None:
    if not text:
        return None

    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, re.IGNORECASE)
    if fenced:
        candidate = fenced.group(1).strip()
        if candidate:
            return candidate

    start = text.find("{")
    if start == -1:
        return None

    depth = 0
    for index in range(start, len(text)):
        char = text[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1].strip()
    return None


async def _ollama_generate(prompt: str, *, temperature: float = 0.0) -> str:
    payload = {
        "model": cfg.ollama_model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": temperature,
            "num_ctx": 2048,
            "num_predict": 320,
        },
    }

    async with httpx.AsyncClient(timeout=cfg.ollama_timeout_s) as client:
        response = await client.post(
            cfg.ollama_base_url.rstrip("/") + "/api/generate",
            json=payload,
        )
        response.raise_for_status()
        data = response.json()
    return (data.get("response") or "").strip()


async def ollama_is_available() -> tuple[bool, str]:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(cfg.ollama_base_url.rstrip("/") + "/api/tags")
            response.raise_for_status()
        return True, ""
    except httpx.HTTPError as exc:
        return False, str(exc)


def _normalize_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            if isinstance(item, str):
                cleaned = item.strip()
                if cleaned:
                    out.append(cleaned)
        return out
    if isinstance(value, str):
        cleaned = value.strip()
        return [cleaned] if cleaned else []
    return []


def _trim_words(text: str, limit: int = 20) -> str:
    words = text.strip().split()
    if len(words) <= limit:
        return text.strip()
    return " ".join(words[:limit]).strip()


def _normalize_stance(raw_stance: Any) -> dict[str, int]:
    cleaned = {key: 0 for key in STANCE_KEYS}
    if not isinstance(raw_stance, dict):
        return cleaned

    for key, value in raw_stance.items():
        if not isinstance(key, str):
            continue

        canonical = STANCE_ALIASES.get(_norm_key(key))
        if canonical is None:
            internal = DISPLAY_TO_INTERNAL.get(key, key)
            canonical = internal if internal in STANCE_KEYS else None
        if canonical not in STANCE_KEYS:
            continue

        try:
            number = int(float(value))
        except Exception:
            continue

        cleaned[canonical] = max(-2, min(2, number))

    return cleaned


def _ensure_detected_axis_signal(stance: dict[str, int], detected_axes: list[str]) -> dict[str, int]:
    internal_detected = [DISPLAY_TO_INTERNAL.get(axis, axis) for axis in detected_axes]
    if not internal_detected:
        return stance
    if any(stance.get(axis, 0) != 0 for axis in internal_detected):
        return stance

    first_axis = internal_detected[0]
    stance[first_axis] = -1
    return stance


def _validate_and_normalize(obj: Any, *, detected_axes: list[str]) -> dict[str, Any]:
    if not isinstance(obj, dict):
        raise ValueError("Label must be a JSON object")

    language = str(obj.get("language") or "").strip().lower()
    if language not in {"pl", "en", "other"}:
        language = "other"

    techniques = _normalize_string_list(obj.get("techniques"))
    techniques = [item for item in techniques if item in TECHNIQUES]

    evidence = []
    for item in _normalize_string_list(obj.get("evidence")):
        trimmed = _trim_words(item, 20)
        if len(trimmed) > 240:
            trimmed = trimmed[:237] + "..."
        if trimmed:
            evidence.append(trimmed)

    target_entities = _normalize_string_list(obj.get("target_entities"))
    target_audience_guess = _normalize_string_list(obj.get("target_audience_guess"))

    rationale = obj.get("rationale")
    rationale = rationale.strip() if isinstance(rationale, str) else ""
    if len(rationale) > 650:
        rationale = rationale[:647] + "..."

    notes = obj.get("notes")
    notes = notes.strip() if isinstance(notes, str) else ""

    try:
        confidence = float(obj.get("confidence", 0.5))
    except Exception:
        confidence = 0.5
    confidence = max(0.0, min(1.0, confidence))

    stance = _normalize_stance(obj.get("stance"))
    stance = _ensure_detected_axis_signal(stance, detected_axes)

    return {
        "language": language,
        "target_entities": target_entities,
        "target_audience_guess": target_audience_guess,
        "stance": stance,
        "techniques": list(dict.fromkeys(techniques)),
        "rationale": rationale,
        "evidence": evidence[:3],
        "confidence": confidence,
        "notes": notes,
    }


async def ollama_label(text: str, *, max_attempts: int = 2) -> dict[str, Any]:
    safe_text = (text or "").strip()
    if len(safe_text) > 5200:
        safe_text = safe_text[:5200]

    detected_axes = _detect_axes(safe_text)
    base_prompt = build_prompt(safe_text)
    last_error = "unknown error"

    for attempt in range(1, max_attempts + 1):
        prompt = base_prompt
        if attempt > 1:
            prompt += "\n\nWAŻNE: zwróć tylko poprawny JSON, bez znaków przed i po."

        raw_output = await _ollama_generate(prompt, temperature=0.0)
        candidate = _extract_first_json_block(raw_output) or raw_output.strip()

        try:
            obj = json.loads(candidate)
            return _validate_and_normalize(obj, detected_axes=detected_axes)
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"

            fix_prompt = (
                "Napraw poniższą treść tak, aby była poprawnym JSON-em. "
                "Zwróć tylko JSON.\n\nTREŚĆ:\n"
                + (candidate[:8000] if candidate else raw_output[:8000])
            )
            fixed_output = await _ollama_generate(fix_prompt, temperature=0.0)
            fixed_candidate = _extract_first_json_block(fixed_output) or fixed_output.strip()

            try:
                obj = json.loads(fixed_candidate)
                return _validate_and_normalize(obj, detected_axes=detected_axes)
            except Exception as fix_exc:
                last_error = f"{last_error} | fix_failed: {type(fix_exc).__name__}: {fix_exc}"

    raise RuntimeError(f"Failed to get valid JSON from Ollama. Last error: {last_error}")
