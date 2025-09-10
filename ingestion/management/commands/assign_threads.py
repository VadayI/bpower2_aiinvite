# assign_threads.py
import re
from typing import Optional, Sequence

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Q, QuerySet, Exists, OuterRef
from django.utils.text import slugify

from ingestion.models import EmailMessage, Thread, Person


# ===== Helpers =====

_SUBJECT_PREFIXES = (
    "re:", "fw:", "fwd:", "odp:", "od:", "sv:", "aw:", "wg:", "ref:",
)


def normalize_subject(subject: str) -> str:
    """Usuwa typowe prefiksy odpowiedzi/przekazań i normalizuje temat."""
    if not subject:
        return ""
    s = subject.strip()
    s_low = s.lower()
    changed = True
    # wycinamy powtarzające się prefiksy (np. Re: Re: Fwd:)
    while changed and s_low:
        changed = False
        for p in _SUBJECT_PREFIXES:
            if s_low.startswith(p):
                s = s[len(p):].lstrip(" \t-:[]")
                s_low = s.lower()
                changed = True
                break
    return s


def extract_references(ref_header: str) -> Sequence[str]:
    """Zwraca listę Message-ID z nagłówka References (bez nawiasów <>)."""
    if not ref_header:
        return []
    in_brackets = re.findall(r"<([^>]+)>", ref_header)
    if in_brackets:
        return [x.strip() for x in in_brackets if x.strip()]
    return [t.strip("<> ,;\t") for t in ref_header.split() if t.strip("<> ,;\t")]


def find_parent_thread(msg: EmailMessage) -> Optional[Thread]:
    """Próbuje znaleźć wątek po In-Reply-To lub References."""
    # 1) Po In-Reply-To (najbardziej wiarygodne)
    if msg.in_reply_to_header:
        parent = (
            EmailMessage.objects
            .filter(message_id_header=msg.in_reply_to_header)
            .select_related("thread")
            .first()
        )
        if parent and parent.thread:
            return parent.thread

    # 2) Po References (pierwsza dopasowana wiadomość z wątkiem)
    if msg.references_header:
        refs = extract_references(msg.references_header)
        if refs:
            parent = (
                EmailMessage.objects
                .filter(message_id_header__in=refs, thread__isnull=False)
                .select_related("thread")
                .order_by("-id")
                .first()
            )
            if parent:
                return parent.thread

    return None


def get_or_create_subject_thread(msg: EmailMessage) -> Thread:
    """Fallback do tematu: tworzy/znajduje Thread po znormalizowanym temacie."""
    subject_norm = normalize_subject(msg.subject or "")
    if subject_norm:
        thread_key = f"subj:{slugify(subject_norm)[:200]}"
        thread, _ = Thread.objects.get_or_create(
            thread_key=thread_key,
            defaults={"subject_norm": subject_norm[:500]},
        )
        return thread
    # Ostateczny fallback – stabilny klucz na bazie Message-ID / external_message_id / pk
    key = f"msgid:{msg.message_id_header or msg.external_message_id or msg.pk}"
    thread, _ = Thread.objects.get_or_create(thread_key=key)
    if not thread.subject_norm and msg.subject:
        thread.subject_norm = normalize_subject(msg.subject)[:500]
        thread.save(update_fields=["subject_norm"])
    return thread


def assign_thread_for_message(msg: EmailMessage, allow_subject_fallback: bool = True) -> Thread:
    """
    Przypisuje (albo tworzy) Thread dla pojedynczej wiadomości.
    Zwraca docelowy Thread (nic nie robi jeśli thread już istnieje).
    """
    if msg.thread_id:
        return msg.thread  # już przypisane

    # 1) Po nagłówkach
    parent_thread = find_parent_thread(msg)
    if parent_thread:
        msg.thread = parent_thread
        msg.save(update_fields=["thread"])
        return parent_thread

    # 2) Fallback po temacie (opcjonalny)
    if allow_subject_fallback:
        thread = get_or_create_subject_thread(msg)
        msg.thread = thread
        msg.save(update_fields=["thread"])
        return thread

    # 3) Brak fallbacku — tworzymy własny wątek po Message-ID
    thread = get_or_create_subject_thread(msg)
    msg.thread = thread
    msg.save(update_fields=["thread"])
    return thread


# ===== Filtry osób (A i/lub B) =====

def _exists_recipient(person_id: int):
    from ingestion.models import MessageRecipient  # lokalny import, by uniknąć cykli
    return Exists(
        MessageRecipient.objects.filter(message=OuterRef("pk"), person_id=person_id)
    )

def q_involves_person(person_id: int) -> Q:
    """
    Czy wiadomość angażuje daną osobę (nadawca, adresat, delivered_to)?
    """
    return Q(from_person_id=person_id) | Q(delivered_to_id=person_id) | _exists_recipient(person_id)

def q_between(person_a_id: int, person_b_id: int) -> Q:
    """
    Czy wiadomość angażuje OBU uczestników (A i B) — w dowolnych rolach.
    """
    return q_involves_person(person_a_id) & q_involves_person(person_b_id)


# ===== Management Command =====

class Command(BaseCommand):
    help = (
        "Przypisuje wątki (Thread) dla wiadomości EmailMessage na podstawie "
        "In-Reply-To, References i (opcjonalnie) znormalizowanego tematu. "
        "Można ograniczyć do wiadomości z udziałem wskazanej osoby (--person) "
        "lub wyłącznie rozmów między dwiema osobami (--person oraz --with-person --only-between)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--all",
            action="store_true",
            help="Przetwarzaj wszystkie wiadomości (także te z już przypisanym wątkiem).",
        )
        parser.add_argument(
            "--since",
            type=str,
            help="Filtruj wiadomości od daty (YYYY-MM-DD) po sent_at/received_at.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=0,
            help="Maksymalna liczba wiadomości do przetworzenia (0 = bez limitu).",
        )
        parser.add_argument(
            "--no-subject-fallback",
            action="store_true",
            help="Wyłącz fallback do tematu; używaj wyłącznie nagłówków RFC.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Tryb podglądu — nic nie zapisuje, tylko pokazuje planowane działania.",
        )
        # --- NOWE ARGUMENTY ---
        parser.add_argument(
            "--person",
            type=int,
            help="ID osoby (Person.id), której wiadomości mają być brane pod uwagę.",
        )
        parser.add_argument(
            "--with-person",
            type=int,
            help="ID rozmówcy (Person.id). Użyj z --person; w połączeniu z --only-between ograniczy do wiadomości angażujących OBU.",
        )
        parser.add_argument(
            "--only-between",
            action="store_true",
            help="Wymagaj współwystąpienia obu osób (--person i --with-person) w tej samej wiadomości.",
        )

    def _base_queryset(
        self,
        process_all: bool,
        since: Optional[str],
        person_id: Optional[int],
        with_person_id: Optional[int],
        only_between: bool,
    ) -> QuerySet[EmailMessage]:
        qs = EmailMessage.objects.all().order_by("id")
        if not process_all:
            qs = qs.filter(thread__isnull=True)
        if since:
            qs = qs.filter(Q(sent_at__date__gte=since) | Q(received_at__date__gte=since))

        # --- Filtry po osobach ---
        if person_id and with_person_id and only_between:
            qs = qs.filter(q_between(person_id, with_person_id))
        elif person_id and with_person_id:
            # dowolna wiadomość z udziałem którejkolwiek z tych dwóch osób
            qs = qs.filter(q_involves_person(person_id) | q_involves_person(with_person_id))
        elif person_id:
            qs = qs.filter(q_involves_person(person_id))
        elif with_person_id:
            qs = qs.filter(q_involves_person(with_person_id))

        return qs.select_related("thread").only(
            "id", "thread_id", "subject",
            "message_id_header", "external_message_id",
            "in_reply_to_header", "references_header",
            "sent_at", "received_at",
        ).distinct()

    def handle(self, *args, **opts):
        process_all = bool(opts.get("all"))
        since = opts.get("since")
        limit = int(opts.get("limit") or 0)
        dry_run = bool(opts.get("dry_run"))
        allow_subject_fallback = not bool(opts.get("no_subject_fallback"))

        person_id = opts.get("person")
        with_person_id = opts.get("with_person")
        only_between = bool(opts.get("only_between"))

        # walidacja pary
        if only_between and not (person_id and with_person_id):
            raise CommandError("--only-between wymaga podania obu: --person oraz --with-person.")

        # (opcjonalne) sprawdzenie, czy osoby istnieją
        def _check_person(pid):
            try:
                return Person.objects.only("id", "email", "display_name").get(pk=pid)
            except Person.DoesNotExist:
                raise CommandError(f"Person.id={pid} nie istnieje.")

        p_a = _check_person(person_id) if person_id else None
        p_b = _check_person(with_person_id) if with_person_id else None

        qs = self._base_queryset(process_all, since, person_id, with_person_id, only_between)
        total = qs.count() if limit == 0 else min(limit, qs.count())

        if total == 0:
            self.stdout.write(self.style.WARNING("Brak wiadomości do przetworzenia."))
            return

        filt_info = []
        if p_a:
            filt_info.append(f"person={p_a.id}<{p_a.display_name or p_a.email}>")
        if p_b:
            filt_info.append(f"with={p_b.id}<{p_b.display_name or p_b.email}>")
        if only_between and (p_a and p_b):
            filt_info.append("mode=ONLY_BETWEEN")

        self.stdout.write(
            self.style.MIGRATE_HEADING(
                f"Przypisywanie wątków: {'WSZYSTKIE' if process_all else 'bez wątku'}; "
                f"since={since or '-'}; limit={limit or '∞'}; "
                f"subject_fallback={'TAK' if allow_subject_fallback else 'NIE'}; "
                f"{' | '.join(filt_info) if filt_info else 'brak filtrów osób'}; "
                f"dry_run={'TAK' if dry_run else 'NIE'}"
            )
        )

        processed = 0
        created_threads = 0
        reused_threads = 0

        for msg in qs.iterator(chunk_size=1000):
            if limit and processed >= limit:
                break

            before_thread_id = msg.thread_id

            if dry_run:
                simulated = find_parent_thread(msg)
                if simulated:
                    action = f"-> thread(parent) {simulated.thread_key}"
                elif allow_subject_fallback:
                    subj = normalize_subject(msg.subject or "")
                    key = f"subj:{slugify(subj)[:200]}" if subj else f"msgid:{msg.message_id_header or msg.external_message_id or msg.pk}"
                    action = f"-> thread(fallback) {key}"
                else:
                    key = f"msgid:{msg.message_id_header or msg.external_message_id or msg.pk}"
                    action = f"-> thread(msgid) {key}"
                self.stdout.write(f"[DRY] msg#{msg.id} {action}")
                processed += 1
                continue

            with transaction.atomic():
                thread = assign_thread_for_message(msg, allow_subject_fallback=allow_subject_fallback)

            processed += 1
            if before_thread_id:
                reused_threads += 1
            else:
                if thread and thread.messages.count() <= 1:
                    created_threads += 1
                else:
                    reused_threads += 1

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(f"Gotowe. Przetworzono: {processed}"))
        self.stdout.write(self.style.SUCCESS(f"Użyte/istniejące wątki: {reused_threads}"))
        self.stdout.write(self.style.SUCCESS(f"Nowe wątki: {created_threads}"))
