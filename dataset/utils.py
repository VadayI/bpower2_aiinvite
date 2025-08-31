from typing import List
from dataset.models import Dictionary, DictionaryKind, DictionaryValue


def render_dictionary_tree(dictionary: Dictionary) -> str:
    """
    Zwraca czytelny string przedstawiający zestaw słowników w formie drzewa:
    Dictionary -> Kinds -> Values.
    """
    lines: List[str] = []
    lines.append(f"Zestaw: {dictionary.name} ({dictionary.code} {dictionary.version}/{dictionary.locale})")
    lines.append(f"Opis: {dictionary.description.strip()}")
    lines.append("")

    # pobieramy wszystkie rodzaje + ich wartości
    kinds = (
        DictionaryKind.objects.all()
        .order_by("name")
        .prefetch_related("values")
    )

    for kind in kinds:
        values = (
            DictionaryValue.objects.filter(kind=kind, is_active=True)
            .order_by("sort_order", "name")
        )
        if not values.exists():
            continue
        lines.append(f"├─ Rodzaj: {kind.name} ({kind.code})")
        lines.append(f"│  Opis: {kind.description.strip()}")
        for val in values:
            lines.append(f"│    • {val.name} [{val.code}] — {val.description.strip()}")
        lines.append("")

    return "\n".join(lines)
