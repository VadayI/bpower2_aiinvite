# dataset/services.py
from typing import Dict, List
from .models import DictionaryKind

def run_openai_classification(
    text: str,
    kinds: List[DictionaryKind],
    topk: int,
    model_name: str,
    model_version: str,
    lang: str,
) -> Dict[int, List[dict]]:
    """
    Zwraca: { kind.id: [ {"value_code": str, "proba": float, "evidence_snippet": str}, ... ] }
    Tutaj podłącz właściwe wywołanie OpenAI (prompt per kind lub multi-task).
    """
    # TODO: implementacja prawdziwego calla do OpenAI
    # Poniżej przykładowa „fałszywka” pod strukturę odpowiedzi:
    results: Dict[int, List[dict]] = {}
    for k in kinds:
        # przykładowy dummy wynik
        results[k.id] = [{
            "value_code": "joy" if k.code == "emotion" else "TY",
            "proba": 0.9,
            "evidence_snippet": text[:140]
        }][:topk]
    return results
