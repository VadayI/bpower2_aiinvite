# dataset/management/commands/seed_dictionaries.py
from __future__ import annotations
from typing import Any
from django.core.management.base import BaseCommand
from django.db import transaction
from django.conf import settings

from dataset.models import Dictionary, DictionaryKind, DictionaryValue


NAME_SET = settings.DICTIONARY_NAME_SET
CODE_SET = settings.DICTIONARY_CODE_SET
DEFAULT_PREPROCESS_VERSION = settings.DEFAULT_PREPROCESS_VERSION
DEFAULT_PREPROCESS_LOCALE = settings.DEFAULT_PREPROCESS_LOCALE
DESC_SET = settings.DICTIONARY_DESC_SET


class Command(BaseCommand):
    help = "Seeduje słowniki etykiet (Dictionary, DictionaryKind, DictionaryValue) dla zestawu 'aiinvite'."

    @transaction.atomic
    def handle(self, *args: Any, **options: Any):
        self.stdout.write(self.style.MIGRATE_HEADING("Seeding dictionaries…"))

        # 1) Dictionary (zestaw)
        dictionary, _ = Dictionary.objects.get_or_create(
            code=CODE_SET,
            version=DEFAULT_PREPROCESS_VERSION,
            locale=DEFAULT_PREPROCESS_LOCALE,
            defaults={
                "name": NAME_SET,
                "description": DESC_SET,
                "is_active": True,
            },
        )
        dictionary.name = NAME_SET
        dictionary.description = DESC_SET
        dictionary.is_active = True
        dictionary.save()

        # 2) DictionaryKind
        kinds_spec = [
            ("style", "Styl komunikacji", "Forma adresatywna w korespondencji."),
            ("emotion", "Emocje / ton / sentyment", "Nacechowanie emocjonalne wypowiedzi."),
            ("urgency", "Pilność", "Poziom pilności komunikatu."),
            ("politeness", "Uprzejmość", "Poziom grzeczności i uprzejmości."),
            ("role", "Rola hierarchiczna", "Rola rozmówcy względem użytkownika."),
            ("business_type", "Typ relacji biznesowej", "Relacja biznesowa."),
            ("trust_integrity", "Zaufanie — Integralność", "Ocena uczciwości, spójności z wartościami."),
            ("trust_intentions", "Zaufanie — Intencje", "Ocena motywacji, dobrych zamiarów."),
            ("trust_skills", "Zaufanie — Umiejętności", "Ocena kompetencji i profesjonalizmu."),
            ("trust_results", "Zaufanie — Wyniki", "Ocena realizacji celów, skuteczności."),
        ]
        kinds: dict[str, DictionaryKind] = {}
        for code, name, desc in kinds_spec:
            kind, _ = DictionaryKind.objects.get_or_create(dictionary=dictionary, code=code, defaults={"name": name, "description": desc})
            kind.name = name
            kind.description = desc
            kind.save()
            kinds[code] = kind

        # 3) Values
        def ensure_value(kind_code: str, code: str, name: str, description: str, order: int):
            val, _ = DictionaryValue.objects.get_or_create(
                kind=kinds[kind_code],
                code=code,
                defaults={
                    "name": name,
                    "description": description,
                    "sort_order": order,
                    "is_active": True,
                },
            )
            changed = False
            if val.name != name:
                val.name = name; changed = True
            if val.description != description:
                val.description = description; changed = True
            if val.sort_order != order:
                val.sort_order = order; changed = True
            if not val.is_active:
                val.is_active = True; changed = True
            if changed:
                val.save(update_fields=["name", "description", "sort_order", "is_active"])
            return val

        # STYLE
        ensure_value("style", "ambiguous", "Niejednoznaczne", "Mieszane lub trudne do rozpoznania.", 0)
        ensure_value("style", "sir_madam", "Pan/Pani", "Użycie form grzecznościowych „Pan/Pani”.", 1)
        ensure_value("style", "you", "Ty", "Bezpośrednia forma „na Ty”.", 2)

        # EMOTION
        ensure_value("emotion", "ambiguous", "Niejednoznaczny", "Niejednoznaczny ton.", 0)
        ensure_value("emotion", "frustration", "Frustracja", "Ton nacechowany zdenerwowaniem, presją czasu, irytacją.", 1)
        ensure_value("emotion", "negative", "Negatywny", "Krytyczny, chłodny ton.", 2)
        ensure_value("emotion", "neutral", "Neutralny", "Ton rzeczowy, bez nacechowania.", 3)
        ensure_value("emotion", "positive", "Pozytywny", "Uprzejmy, życzliwy ton.", 4)
        ensure_value("emotion", "joy", "Radość", "Entuzjastyczna komunikacja, często z humorem.", 5)

        # URGENCY
        ensure_value("urgency", "ambiguous", "Niejednoznaczny", "Brak możliwości oceny pilności.", 0)
        ensure_value("urgency", "low", "Niski", "Niski poziom pilności.", 1)
        ensure_value("urgency", "medium", "Średni", "Średni poziom pilności.", 2)
        ensure_value("urgency", "high", "Wysoki", "Wysoka pilność, szybka reakcja wymagana.", 3)

        # POLITENESS
        ensure_value("politeness", "ambiguous", "Niejednoznaczny", "Brak możliwości oceny uprzejmości.", 0)
        ensure_value("politeness", "low", "Niski", "Niski poziom grzeczności.", 1)
        ensure_value("politeness", "medium", "Średni", "Umiarkowana grzeczność.", 2)
        ensure_value("politeness", "high", "Wysoki", "Wysoka uprzejmość, bardzo grzeczny ton.", 3)

        # ROLE
        ensure_value("role", "ambiguous", "Niejednoznaczny", "Brak możliwości oceny roli hierarchicznej.", 0)
        ensure_value("role", "peer", "Kolega z zespołu", "Osoba na równym szczeblu w zespole.", 1)
        ensure_value("role", "external_person", "Osoba spoza organizacji", "Rozmówca spoza firmy/organizacji.", 2)
        ensure_value("role", "subordinate", "Podwładny", "Osoba podlegająca użytkownikowi.", 3)
        ensure_value("role", "manager", "Przełożony", "Osoba wyżej w hierarchii.", 4)
        ensure_value("role", "internal_employee", "Wewnętrzny pracownik", "Osoba z tej samej organizacji.", 5)

        # BUSINESS TYPE
        ensure_value("business_type", "ambiguous", "Niejednoznaczny", "Brak możliwości oceny typu relacji.", 0)
        ensure_value("business_type", "vendor", "Dostawca", "Podmiot dostarczający produkty/usługi.", 1)
        ensure_value("business_type", "investor", "Inwestor", "Osoba/firma zapewniająca finansowanie.", 2)
        ensure_value("business_type", "client", "Klient", "Odbiorca usług/produktów.", 3)
        ensure_value("business_type", "contractor", "Kooperant", "Podmiot realizujący prace na zlecenie.", 4)
        ensure_value("business_type", "partner", "Partner", "Organizacja/osoba w równorzędnej współpracy.", 5)
        ensure_value("business_type", "recruitment", "Rekruter/Kandydat", "Relacja rekrutacyjna.", 6)

        # TRUST: INTEGRITY
        ensure_value("trust_integrity", "ambiguous", "Niejednoznaczny", "Brak możliwości oceny integralność.", 0)
        ensure_value("trust_integrity", "lies_consciously", "Kłamie świadomie", "Świadome mijanie się z prawdą.", 1)
        ensure_value("trust_integrity", "often_insincere", "Często nieszczera", "Częsty brak szczerości.", 2)
        ensure_value("trust_integrity", "unclear_posture", "Nieczytelna postawa", "Brak spójności deklaracji i działań.", 3)
        ensure_value("trust_integrity", "mostly_honest", "Raczej uczciwa", "Na ogół uczciwa i spójna.", 4)
        ensure_value("trust_integrity", "fully_fair", "Całkowicie fair", "Wysoka integralność i uczciwość.", 5)

        # TRUST: INTENTIONS
        ensure_value("trust_intentions", "ambiguous", "Niejednoznaczny", "Brak możliwości oceny intencji.", 0)
        ensure_value("trust_intentions", "selfish", "Egoistyczna postawa", "Skupienie na własnych korzyściach.", 1)
        ensure_value("trust_intentions", "ignores_others", "Ignoruje innych", "Brak troski o innych.", 2)
        ensure_value("trust_intentions", "unclear_motives", "Motywy nieczytelne", "Niejasne zamiary.", 3)
        ensure_value("trust_intentions", "mostly_sincere", "Przeważnie szczera", "Zazwyczaj dobre zamiary.", 4)
        ensure_value("trust_intentions", "fully_benevolent", "W pełni życzliwa", "Transparentne i dobre intencje.", 5)

        # TRUST: SKILLS
        ensure_value("trust_skills", "ambiguous", "Niejednoznaczny", "Brak możliwości oceny umiejętności.", 0)
        ensure_value("trust_skills", "lacks_competence", "Brakuje kompetencji", "Wyraźne braki w umiejętnościach.", 1)
        ensure_value("trust_skills", "many_errors", "Dużo błędów", "Częste błędy, niska skuteczność.", 2)
        ensure_value("trust_skills", "sometimes_ineffective", "Czasem nieskuteczna", "Nierówna jakość pracy.", 3)
        ensure_value("trust_skills", "usually_effective", "Zazwyczaj sprawna", "Dobra sprawność i fachowość.", 4)
        ensure_value("trust_skills", "high_proficiency", "Wysoka biegłość", "Wysokie kompetencje i profesjonalizm.", 5)

        # TRUST: RESULTS
        ensure_value("trust_results", "ambiguous", "Niejednoznaczny", "Brak możliwości oceny wyników.", 0)
        ensure_value("trust_results", "always_fails", "Zawsze zawodzi", "Trwale nieosiąga celów.", 1)
        ensure_value("trust_results", "often_ineffective", "Często nieskuteczna", "Często nie dostarcza wyników.", 2)
        ensure_value("trust_results", "uneven_results", "Wyniki nierówne", "Zmienna skuteczność.", 3)
        ensure_value("trust_results", "mostly_effective", "Najczęściej skuteczna", "Z reguły realizuje cele.", 4)
        ensure_value("trust_results", "always_delivers", "Zawsze dowozi", "Wysoka przewidywalność i skuteczność.", 5)

        self.stdout.write(self.style.SUCCESS("✓ Słowniki i wartości utworzone/zaktualizowane"))
