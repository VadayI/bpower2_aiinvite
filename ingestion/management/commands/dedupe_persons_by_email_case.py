from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, List, Tuple

from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction, IntegrityError
from django.db.models import Count, F, Q
from django.db.models.functions import Lower

from ingestion.models import Person, EmailMessage, MessageRecipient


def normalize_domain(email: str) -> str:
    return email.split("@")[-1] if "@" in email else ""


@dataclass
class PersonStats:
    pk: int
    email: str
    display_name: str
    domain: str
    sent: int
    delivered: int
    m2m: int

    @property
    def total_links(self) -> int:
        return self.sent + self.delivered + self.m2m


def pick_canonical(candidates: List[PersonStats]) -> PersonStats:
    """
    Wybierz rekord kanoniczny:
    1) najwięcej powiązań (sent+delivered+m2m)
    2) potem najstarszy (najmniejsze PK)
    """
    candidates = sorted(candidates, key=lambda c: (-c.total_links, c.pk))
    return candidates[0]


class Command(BaseCommand):
    help = "Scala rekordy Person różniące się jedynie wielkością liter w polu email; aktualizuje FK/M2M i usuwa duplikaty."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--dry-run", action="store_true", help="Tylko pokaż plan, bez modyfikacji.")
        parser.add_argument("--chunk", type=int, default=500, help="Wielkość batcha dla iteracji.")

    def handle(self, *args, **opts):
        dry_run = opts["dry_run"]
        chunk = opts["chunk"]

        # 0) Zbierz grupy po lower(email)
        groups = (
            Person.objects
            .annotate(lower_email=Lower("email"))
            .values("lower_email")
            .annotate(cnt=Count("id"))
            .filter(cnt__gt=1)
            .values_list("lower_email", flat=True)
        )

        if not groups:
            self.stdout.write(self.style.WARNING("Brak duplikatów wg lower(email). Nic do zrobienia."))
            # mimo to warto znormalizować singletony do lowercase:
            self._normalize_singletons(dry_run)
            return

        self.stdout.write(self.style.NOTICE(f"Znaleziono grup duplikatów: {len(groups)}"))

        total_merged = 0
        total_deleted = 0
        total_updated_msgs = 0
        total_updated_recips = 0

        for lower_email in groups:
            with transaction.atomic():
                # Kandydaci w tej grupie
                persons = list(
                    Person.objects
                    .filter(email__iexact=lower_email)
                    .annotate(
                        _sent=Count("sent_messages", distinct=True),
                        _deliv=Count("delivered_messages", distinct=True),
                        _m2m=Count("message_recipient_links", distinct=True),
                    )
                )

                if len(persons) < 2:
                    continue  # wyścig? ktoś już scalił

                stats = [
                    PersonStats(
                        pk=p.id,
                        email=p.email,
                        display_name=p.display_name or "",
                        domain=p.domain or "",
                        sent=p._sent,
                        delivered=p._deliv,
                        m2m=p._m2m,
                    )
                    for p in persons
                ]

                canonical_stats = pick_canonical(stats)
                canonical = next(p for p in persons if p.id == canonical_stats.pk)
                others = [p for p in persons if p.id != canonical.id]

                self.stdout.write(f"\nGrupa '{lower_email}': wybieram kanoniczny #{canonical.id} <{canonical.email}>")

                # 1) Uzupełnij pola kanonicznego: email lower, domain, display_name (jeśli puste)
                changed_fields = []
                if canonical.email != canonical.email.lower():
                    canonical.email = canonical.email.lower()
                    changed_fields.append("email")
                if not canonical.domain:
                    canonical.domain = normalize_domain(canonical.email)
                    changed_fields.append("domain")
                if not canonical.display_name:
                    # wybierz najdłuższy niepusty display_name z duplikatów
                    candidates_dn = [o.display_name for o in others if (o.display_name or "").strip()]
                    if candidates_dn:
                        canonical.display_name = max(candidates_dn, key=len)
                        changed_fields.append("display_name")
                if changed_fields and not dry_run:
                    canonical.save(update_fields=changed_fields)

                # 2) Przepisz powiązania z others -> canonical
                for dup in others:
                    # EmailMessage.from_person
                    qs1 = EmailMessage.objects.filter(from_person=dup)
                    # EmailMessage.delivered_to
                    qs2 = EmailMessage.objects.filter(delivered_to=dup)

                    # MessageRecipient.person (uwaga na unique_together)
                    qs3 = MessageRecipient.objects.filter(person=dup)

                    c1 = qs1.count()
                    c2 = qs2.count()
                    c3 = qs3.count()

                    self.stdout.write(f"- scalanie #{dup.id} <{dup.email}>: sent={c1}, delivered={c2}, m2m={c3}")

                    if not dry_run:
                        total_updated_msgs += c1 + c2
                        qs1.update(from_person=canonical)
                        qs2.update(delivered_to=canonical)

                        # M2M: mogą powstać duplikaty (message, canonical, kind)
                        for mr in qs3.iterator(chunk_size=chunk):
                            try:
                                mr.person = canonical
                                mr.save(update_fields=["person"])
                            except IntegrityError:
                                # istnieje już taki link — usuń bieżący
                                MessageRecipient.objects.filter(pk=mr.pk).delete()
                            total_updated_recips += 1

                        # Usuń duplikata
                        dup_pk = dup.pk
                        dup.delete()
                        total_deleted += 1

                total_merged += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"\nSkończone. Grupy scalone: {total_merged}, usuniętych Person: {total_deleted}, "
                f"zaktualizowane powiązania: EmailMessage={total_updated_msgs}, MessageRecipient={total_updated_recips}"
            )
        )

        # 3) Po scaleniu — znormalizuj singletony do lower-case
        self._normalize_singletons(dry_run)

        self.stdout.write(self.style.SUCCESS("Dedup zakończony."))

    def _normalize_singletons(self, dry_run: bool):
        """
        Upewnij się, że wszystkie istniejące Person mają email w lowercase oraz poprawną domenę.
        """
        singles = Person.objects.exclude(email=Lower("email")).only("id", "email", "domain")
        n = singles.count()
        if n:
            self.stdout.write(self.style.NOTICE(f"Normalizacja e-maili do lowercase: {n} rekordów"))
        if not dry_run:
            for p in singles.iterator(chunk_size=500):
                p.email = p.email.lower()
                if not p.domain:
                    p.domain = normalize_domain(p.email)
                p.save(update_fields=["email", "domain"])
