from __future__ import annotations

from django.conf import settings
import hashlib
import logging
from typing import Optional

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Q, F, QuerySet

from ingestion.models import EmailMessage, MessageRecipient, Thread, Person
from dataset.models import (
    DatasetSample, Dictionary, DictionaryKind, DictionaryValue, ModelPrediction,
    _normalize_content,  # używamy identycznej normalizacji jak w DatasetSample
)
from api.views import (
    THREAD_MSG_SEPARATOR,  # zachowujemy spójność sposobu składania wątków
)
from dataset.chatgpt_client import (
    label_email_with_openai, to_label_rows,
)

logger = logging.getLogger(__name__)

# ====== stałe spójne z widokami / środowiskiem ======
OPENAI_MODEL_NAME = settings.OPENAI_MODEL_NAME
OPENAI_MODEL_VERSION = settings.OPENAI_MODEL_VERSION
DEFAULT_PREPROCESS_VERSION = settings.DEFAULT_PREPROCESS_VERSION
DEFAULT_LANG = settings.DEFAULT_PREPROCESS_LOCALE
MAX_CHARS_THREAD_DEFAULT = int(settings.MAX_CHARS_THREAD)
OPENAI_API_KEY = settings.OPENAI_API_KEY

# ======================= HELPERY =======================

def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _find_dictionary(code: Optional[str], version: Optional[str], locale: Optional[str]) -> Optional[Dictionary]:
    if not code:
        return None
    qs = Dictionary.objects.filter(code=code)
    if version:
        qs = qs.filter(version=version)
    if locale:
        qs = qs.filter(locale=locale)
    return qs.order_by("-is_active").first()


def _resolve_kinds(input_kinds: list[str] | None) -> QuerySet[DictionaryKind]:
    qs = DictionaryKind.objects.all()
    if not input_kinds:
        return qs.filter(is_active=True)
    ids = [k for k in input_kinds if isinstance(k, int) or (isinstance(k, str) and k.isdigit())]
    codes = [k for k in input_kinds if isinstance(k, str) and not k.isdigit()]
    q = qs.none()
    if ids:
        q = q.union(qs.filter(pk__in=[int(i) for i in ids]))
    if codes:
        q = q.union(qs.filter(code__in=codes))
    return q.distinct()


def _build_values_map(kinds_qs: QuerySet[DictionaryKind], dictionary_id: Optional[int]):
    qs = DictionaryValue.objects.filter(kind__in=kinds_qs, is_active=True)
    if dictionary_id:
        qs = qs.filter(kind__dictionary_id=dictionary_id)
    out = {}
    for v in qs.only("id", "code", "kind_id"):
        out.setdefault(v.kind_id, {})[v.code] = v.id
    return out


def _emails_between_people(a_id: int, b_id: int) -> QuerySet[EmailMessage]:
    """
    Wiadomości, w których uczestniczą A i B wg reguł:
    - zliczamy relacje: from_person -> TO, oraz delivered_to <-> from_person
    - NIE zliczamy TO <-> TO
    - wykluczamy from_person == delivered_to
    """
    return (
        EmailMessage.objects
        .exclude(Q(from_person=F("delivered_to")) & Q(recipients__isnull=True))
        .filter(
            Q(from_person_id=a_id) & (
                Q(delivered_to_id=b_id) |
                Q(message_recipients__person_id=b_id,
                  message_recipients__kind=MessageRecipient.Kind.TO)
            )
            |
            Q(from_person_id=b_id) & (
                Q(delivered_to_id=a_id) |
                Q(message_recipients__person_id=a_id,
                  message_recipients__kind=MessageRecipient.Kind.TO)
            )
        )
        .distinct()
        .only("id", "subject", "direction", "text_processed", "sent_at", "received_at")
        .order_by("id")
    )


def _threads_between_people(a: Person, b: Person) -> QuerySet[Thread]:
    msgs_a = EmailMessage.objects.filter(
        Q(from_person=a) |
        Q(delivered_to=a) |
        Q(message_recipients__person=a, message_recipients__kind=MessageRecipient.Kind.TO)
    ).values("thread_id")

    msgs_b = EmailMessage.objects.filter(
        Q(from_person=b) |
        Q(delivered_to=b) |
        Q(message_recipients__person=b, message_recipients__kind=MessageRecipient.Kind.TO)
    ).values("thread_id")

    return Thread.objects.filter(id__in=msgs_a).filter(id__in=msgs_b).distinct()


def _compose_thread_text(thread: Thread, *, max_chars: int) -> str:
    msgs = (
        EmailMessage.objects
        .filter(thread=thread)
        .only("id", "subject", "direction", "text_processed", "sent_at", "received_at")
        .order_by("sent_at", "received_at", "id")
    )
    parts: list[str] = []
    for m in msgs:
        header = f"[{m.direction.upper()}] {m.subject or ''}".strip()
        body = (m.text_processed or "") or (m.subject or "")
        parts.append((header + "\n" + body).strip())
    text = THREAD_MSG_SEPARATOR.join(parts)
    if len(text) > max_chars:
        text = text[-max_chars:]  # nowsza część dialogu
    return text


def _needs_prediction(sample: DatasetSample,
                      kinds_by_id: dict[int, DictionaryKind],
                      dictionary: Optional[Dictionary]) -> list[int]:
    qs = ModelPrediction.objects.filter(
        sample=sample,
        kind_id__in=list(kinds_by_id.keys()),
        model_name=OPENAI_MODEL_NAME,
        model_version=OPENAI_MODEL_VERSION,
    )
    if hasattr(ModelPrediction, "dictionary_id") and dictionary is not None:
        qs = qs.filter(dictionary=dictionary)
    have = set(qs.values_list("kind_id", flat=True))
    return [kid for kid in kinds_by_id.keys() if kid not in have]


def _upsert_missing_preds(*,
                          email_text: str,
                          subject: Optional[str],
                          direction: Optional[str],
                          kinds_qs: QuerySet[DictionaryKind],
                          need_kind_ids: list[int],
                          kinds_by_id: dict[int, DictionaryKind],
                          dictionary_code: str,
                          dictionary_version: str,
                          dictionary_locale: str,
                          sample: DatasetSample,
                          dictionary: Optional[Dictionary]) -> tuple[int, bool]:
    """
    Jedno wywołanie LLM i zapis brakujących ModelPrediction dla danej próbki.
    Zwraca: (liczba_zapisanych, czy_zapisano_cokolwiek)
    """
    raw_args, enums = label_email_with_openai(
        model_openai=OPENAI_MODEL_VERSION,
        openai_api_key=OPENAI_API_KEY,
        email_text=email_text,
        subject=subject,
        direction=direction,
        dictionary_code=dictionary_code,
        dictionary_version=dictionary_version,
        dictionary_locale=dictionary_locale,
    )
    rows = to_label_rows(raw_args, enums)  # [{"kind_id","value_id","snippet","proba"?}...]
    dictionary_id = enums.get("dictionary_id")
    values_map = _build_values_map(kinds_qs, dictionary_id)
    need_set = set(need_kind_ids)

    saved = 0
    for r in rows:
        r_kind_id = r.get("kind_id")
        if r_kind_id not in need_set:
            continue

        value_id = r.get("value_id")
        if not value_id and r.get("value_code"):
            value_id = values_map.get(r_kind_id, {}).get(r["value_code"])
            if not value_id:
                logger.warning("Brak mapy value_id dla kind_id=%s value_code=%r", r_kind_id, r.get("value_code"))
                continue

        defaults = {
            "value_id": value_id,
            "proba": float(r.get("proba", 0.0)) if r.get("proba") is not None else 0.0,
            "evidence_snippet": (r.get("snippet") or "").strip(),
        }
        pred_kwargs = dict(
            sample=sample,
            kind_id=r_kind_id,
            model_name=OPENAI_MODEL_NAME,
            model_version=OPENAI_MODEL_VERSION,
        )
        if hasattr(ModelPrediction, "dictionary_id") and dictionary_id:
            pred_kwargs["dictionary_id"] = dictionary_id

        ModelPrediction.objects.update_or_create(**pred_kwargs, defaults=defaults)
        saved += 1

    return saved, saved > 0


# ======================= KOMENDA =======================

class Command(BaseCommand):
    help = (
        "Etykietuje za pomocą OpenAI WSZYSTKIE e-maile i wątki między dwiema wskazanymi osobami (po PK). "
        "Zapisuje ModelPrediction (cache) z evidence_snippet. Wymaga --people-pk."
    )

    def add_arguments(self, parser):
        parser.add_argument("--people-pk", nargs=2, type=int, metavar=("PK_A", "PK_B"), required=True,
                            help="Dwie osoby Person po PK, np. --people-pk 12 34")
        parser.add_argument("--kinds", type=str, default="",
                            help="Lista rodzajów po kodach/ID, np. 'emotion,style,relation'. Puste = wszystkie aktywne.")
        parser.add_argument("--dictionary-code", type=str, default="aiinvite")
        parser.add_argument("--dictionary-version", type=str, default="v1")
        parser.add_argument("--dictionary-locale", type=str, default="pl")
        parser.add_argument("--limit", type=int, default=0,
                            help="Limit elementów do przetworzenia na każdej liście (0 = bez limitu).")
        parser.add_argument("--dry-run", action="store_true",
                            help="Nie zapisuj do bazy – tylko symulacja.")
        parser.add_argument("--max-chars-thread", type=int, default=MAX_CHARS_THREAD_DEFAULT,
                            help=f"Maksymalny rozmiar złożonego tekstu wątku (domyślnie {MAX_CHARS_THREAD_DEFAULT}).")

    def handle(self, *args, **opts):
        pk_a, pk_b = opts["people_pk"]
        limit = int(opts["limit"] or 0)
        dry_run = bool(opts["dry_run"])
        max_chars_thread = int(opts["max_chars_thread"] or MAX_CHARS_THREAD_DEFAULT)

        # KINDS & DICTIONARY
        input_kinds = [k.strip() for k in (opts["kinds"] or "").split(",") if k.strip()]
        kinds_qs = _resolve_kinds(input_kinds)
        if not kinds_qs.exists():
            raise CommandError("Brak zdefiniowanych rodzajów (kinds).")
        kinds_by_id = {k.id: k for k in kinds_qs}

        dictionary = _find_dictionary(opts["dictionary_code"], opts["dictionary_version"], opts["dictionary_locale"])

        # Osoby
        try:
            a = Person.objects.get(pk=pk_a)
            b = Person.objects.get(pk=pk_b)
        except Person.DoesNotExist:
            raise CommandError("Nie znaleziono jednej z osób (pk_a/pk_b).")

        self.stdout.write(self.style.MIGRATE_HEADING(
            f"==> Etykietowanie OpenAI dla pary: {a} (pk={a.pk}) ↔ {b} (pk={b.pk})"
        ))

        # ===== E-MAILE A↔B =====
        emails_qs = _emails_between_people(a.pk, b.pk)
        total_emails = emails_qs.count()
        self.stdout.write(self.style.NOTICE(f"E-maile do rozważenia: {total_emails}"))

        processed_emails = 0
        saved_preds_emails = 0

        for msg in emails_qs.iterator(chunk_size=500):
            if limit and processed_emails >= limit:
                break

            email_text = (msg.text_processed or "") or (msg.subject or "")
            norm = _normalize_content(email_text)
            if not norm:
                processed_emails += 1
                continue

            # próbka (email)
            sample, _ = DatasetSample.objects.get_or_create_from_text(
                norm, preprocess_version=DEFAULT_PREPROCESS_VERSION, lang=DEFAULT_LANG, source="email"
            )

            need_kind_ids = _needs_prediction(sample, kinds_by_id, dictionary)
            if not need_kind_ids:
                processed_emails += 1
                continue

            if dry_run:
                self.stdout.write(f"[DRY] email#{msg.id} -> missing kinds: {len(need_kind_ids)}")
                processed_emails += 1
                continue

            with transaction.atomic():
                try:
                    n_saved, _ = _upsert_missing_preds(
                        email_text=norm,
                        subject=(msg.subject or None),
                        direction=msg.direction,
                        kinds_qs=kinds_qs,
                        need_kind_ids=need_kind_ids,
                        kinds_by_id=kinds_by_id,
                        dictionary_code=opts["dictionary_code"],
                        dictionary_version=opts["dictionary_version"],
                        dictionary_locale=opts["dictionary_locale"],
                        sample=sample,
                        dictionary=dictionary,
                    )
                except Exception as e:
                    self.stderr.write(self.style.ERROR(f"OpenAI error (email#{msg.id}): {e}"))
                    processed_emails += 1
                    continue

            saved_preds_emails += n_saved
            self.stdout.write(self.style.SUCCESS(f"email#{msg.id}: zapisano {n_saved} predykcji"))
            processed_emails += 1

        # ===== WĄTKI A↔B =====
        threads_qs = _threads_between_people(a, b).order_by("id")
        total_threads = threads_qs.count()
        self.stdout.write(self.style.NOTICE(f"Wątki do rozważenia: {total_threads}"))

        processed_threads = 0
        saved_preds_threads = 0

        for th in threads_qs.iterator(chunk_size=200):
            if limit and processed_threads >= limit:
                break

            thread_text = _compose_thread_text(th, max_chars=max_chars_thread)
            norm_thread = _normalize_content(thread_text)
            if not norm_thread:
                processed_threads += 1
                continue

            # próbka (thread)
            sample, _ = DatasetSample.objects.get_or_create_from_text(
                norm_thread, preprocess_version=DEFAULT_PREPROCESS_VERSION, lang=DEFAULT_LANG, source="thread"
            )

            need_kind_ids = _needs_prediction(sample, kinds_by_id, dictionary)
            if not need_kind_ids:
                processed_threads += 1
                continue

            if dry_run:
                self.stdout.write(f"[DRY] thread#{th.id} -> missing kinds: {len(need_kind_ids)}")
                processed_threads += 1
                continue

            with transaction.atomic():
                try:
                    n_saved, _ = _upsert_missing_preds(
                        email_text=norm_thread,
                        subject=None,
                        direction=None,
                        kinds_qs=kinds_qs,
                        need_kind_ids=need_kind_ids,
                        kinds_by_id=kinds_by_id,
                        dictionary_code=opts["dictionary_code"],
                        dictionary_version=opts["dictionary_version"],
                        dictionary_locale=opts["dictionary_locale"],
                        sample=sample,
                        dictionary=dictionary,
                    )
                except Exception as e:
                    self.stderr.write(self.style.ERROR(f"OpenAI error (thread#{th.id}): {e}"))
                    processed_threads += 1
                    continue

            saved_preds_threads += n_saved
            self.stdout.write(self.style.SUCCESS(f"thread#{th.id}: zapisano {n_saved} predykcji"))
            processed_threads += 1

        # ===== Podsumowanie =====
        self.stdout.write(self.style.SUCCESS(
            f"OK. E-maile: {processed_emails} (zapisane predykcje: {saved_preds_emails}); "
            f"Wątki: {processed_threads} (zapisane predykcje: {saved_preds_threads})."
        ))
