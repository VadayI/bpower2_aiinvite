# chatgpt_client.py
from __future__ import annotations
from functools import lru_cache
from typing import Any, Dict, Optional, Literal
import json

from django.db.models import QuerySet
from openai import OpenAI

from dataset.models import Dictionary, DictionaryKind, DictionaryValue


# ==============================
#   KATALOG SŁOWNIKÓW Z BAZY
# ==============================

def _codes_and_ids(qs: QuerySet) -> tuple[list[str], dict[str, int]]:
    rows = list(qs.values("id", "code", "description"))

    codes = sorted(r["code"] for r in rows)
    code_to_id = {}
    dictionary_kind_desc = {}
    for r in rows:
        code_to_id[r["code"]] = r["id"]
        dictionary_kind_desc[r["code"]] = r["description"]

    return codes, code_to_id, dictionary_kind_desc


def _dictionary_filter(
    *, dictionary_code: Optional[str], version: Optional[str], locale: Optional[str]
) -> Optional[int]:
    """
    Zwróć ID konkretnego zestawu (Dictionary) jeżeli podano code/version/locale; wpp. None.
    """
    if not dictionary_code:
        return None
    qs = Dictionary.objects.filter(code=dictionary_code)
    if version:
        qs = qs.filter(version=version)
    if locale:
        qs = qs.filter(locale=locale)
    obj = qs.order_by("-is_active").first()
    return obj.id if obj else None


@lru_cache(maxsize=16)
def load_value_enums(
    *, dictionary_code: Optional[str] = None, version: Optional[str] = None, locale: Optional[str] = None
) -> Dict[str, Any]:
    """
    Zwraca dynamiczny katalog rodzajów i wartości.
    Jeśli podasz dictionary_code/version/locale — ograniczy wartości do danego zestawu.
    Struktura:
    {
      "kinds": {"style": kind_id, ...},
      "values_by_kind": {
        "style": {"codes": [...], "code_to_id": {...}},
        ...
      },
      "dictionary_id": 123 or None
    }
    """
    kinds_qs = DictionaryKind.objects.all().values("id", "code", "description")
    kinds = {row["code"]: {"id": row["id"], "description": row["description"]} for row in kinds_qs}

    dictionary_id = _dictionary_filter(
        dictionary_code=dictionary_code, version=version, locale=locale
    )

    values_by_kind: dict[str, dict[str, Any]] = {}
    for code, kind_id in kinds.items():
        qs = DictionaryValue.objects.filter(kind_id=kind_id["id"], is_active=True)
        if dictionary_id is not None:
            qs = qs.filter(kind__dictionary_id=dictionary_id)
        codes, code_to_id, value_descriptions = _codes_and_ids(qs)
        values_by_kind[code] = {
            "codes": codes, 
            "code_to_id": code_to_id,
            "value_descriptions": value_descriptions
        }

    return {"kinds": kinds, "values_by_kind": values_by_kind, "dictionary_id": dictionary_id}


def build_kind_catalog_text(enums: Dict[str, Any]) -> str:
    """
    Tekstowy katalog dla LLM: lista rodzajów + dostępnych kodów wartości (per rodzaj).
    """
    lines = ["Dostępne rodzaje i wartości (używaj pola 'code' dla wartości):"]
    for kind_code, data in enums["values_by_kind"].items():
        codes_preview = ", ".join(data["codes"]) if data["codes"] else "(brak wartości)"
        value_descriptions_preview = "\n".join([f'-- {k}: {data["value_descriptions"][k]}' for k in data["value_descriptions"]]) if data["value_descriptions"] else "(brak wartości)"

        lines.append(f"{kind_code} - {enums['kinds'][kind_code]['description']}")
        lines.append(f"- {kind_code}: {codes_preview}")
        lines.append(value_descriptions_preview)
    
    return "\n".join(lines)


def build_tool_schema_dynamic(enums: Dict[str, Any]) -> Dict[str, Any]:
    """
    Dynamiczny tool: lista etykiet (kind, value, opcjonalnie snippet/rationale).
    """
    kind_enum = sorted(enums["kinds"].keys())

    return {
        "type": "function",
        "function": {
            "name": "set_labels",
            "description": (
                "Zwróć listę etykiet. Każda etykieta musi zawierać 'kind' (kod rodzaju) i 'value' (kod wartości), "
                "opcjonalnie 'snippet' (fragment treści uzasadniający wybór)."
            ),
            "parameters": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "labels": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "kind": {"type": "string", "enum": kind_enum},
                                "value": {"type": "string"},
                                "snippet": {"type": "string"},
                            },
                            "required": ["kind", "value"],
                        },
                    },
                },
                "required": ["labels"],
            },
        },
    }


# ==============================
#   WYWOŁANIE OPENAI
# ==============================

def label_email_with_openai(
    *,
    email_text: str,
    model_openai: str,
    openai_api_key: str,
    subject: Optional[str] = None,
    direction: Optional[Literal["received", "sent"]] = None,
    dictionary_code: Optional[str],
    dictionary_version: Optional[str],
    dictionary_locale: Optional[str],
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Zwraca: (raw_args, enums)
      raw_args = {"labels":[{"kind","value","snippet"?}, ...]}
      enums    = wynik load_value_enums(...) (zawiera mapy kod->id)
    """
    enums = load_value_enums(
        dictionary_code=dictionary_code, version=dictionary_version, locale=dictionary_locale
    )
    client = OpenAI(api_key=openai_api_key)

    sys_msg = (
        "Jesteś klasyfikatorem e-maili. ZAWSZE zwróć rezultat wyłącznie "
        "jako wywołanie funkcji set_labels (narzędzie). Nie dodawaj żadnej innej treści."
    )
    parts = []
    if subject:
        parts.append(f"Temat: {subject}")
    if direction:
        parts.append(f"Kierunek: {direction}")

    parts.append(build_kind_catalog_text(enums))  # informacja o dostępnych kodach per rodzaj
    parts.append("Wybieraj wartości po 'code'. Jeśli to możliwe, zwróć także 'snippet' (fragment, który uzasadnia etykietę).")
    parts.append(f"Treść e-maila:\n{email_text}")
    user_msg = "\n".join(parts)

    completion = client.chat.completions.create(
        model=model_openai,
        messages=[
            {"role": "system", "content": sys_msg},
            {"role": "user", "content": user_msg},
        ],
        tools=[build_tool_schema_dynamic(enums)],
        tool_choice={"type": "function", "function": {"name": "set_labels"}},
    )


    choice = completion.choices[0].message

    if not getattr(choice, "tool_calls", None):
        raise RuntimeError("Model nie zwrócił wywołania funkcji (tool_calls).")

    args = json.loads(choice.tool_calls[0].function.arguments)
    return args, enums


# ==============================
#   MAPOWANIE
# ==============================

def to_label_rows(raw: Dict[str, Any], enums: Dict[str, Any]) -> list[Dict[str, Any]]:
    """
    Konwersja dynamicznego wejścia na wiersze do utworzenia w Label:
      raw = {"labels":[{"kind":"emotion","value":"joy","snippet":"...","rationale":"..."}, ...],
             "global_rationale":"..."}
    Zwraca listę: [{"kind_id":..., "value_id":..., "snippet":"..."}, ...]
    """
    kinds = enums["kinds"]
    vals = enums["values_by_kind"]

    rows: list[Dict[str, Any]] = []
    labels = raw.get("labels", [])

    for item in labels:
        kind_code = item.get("kind")
        value_code = item.get("value")
        snippet = item.get("snippet", "") or ""

        if not kind_code or kind_code not in kinds:
            raise ValueError(f"Nieznany rodzaj etykiety: {kind_code!r}")

        try:
            value_id = vals[kind_code]["code_to_id"][value_code]
        except KeyError:
            raise ValueError(f"Nieznany kod wartości '{value_code}' dla rodzaju '{kind_code}'.")

        rows.append({
            "kind_id": kinds[kind_code].get('id'),
            "value_id": value_id,
            "snippet": snippet,
        })

    return rows
