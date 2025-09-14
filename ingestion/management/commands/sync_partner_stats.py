from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Tuple, Iterable, Optional

from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction
from django.db.models import F, Q, Max
from django.db.models.functions import Greatest

from ingestion.models import (
    EmailMessage, MessageRecipient, PartnerStat
)


# --- helpers -----------------------------------------------------------------

def _canon_pair(a_id: int, b_id: int) -> Tuple[int, int]:
    return (a_id, b_id) if a_id < b_id else (b_id, a_id)

def _recipient_ids(message_id: int, kinds: tuple[str, ...]) -> Iterable[int]:
    # tylko wskazane rodzaje adresatów (domyślnie TO)
    return MessageRecipient.objects.filter(
        message_id=message_id,
        kind__in=[k.lower() for k in kinds],
    ).values_list("person_id", flat=True)

@dataclass
class Counters:
    # to tylko tymczasowy zestaw par do przeliczenia; liczby wyliczymy kanonicznie
    has_any: bool = False
    
def qs_emails_between(a_id: int, b_id: int, *, kinds=("to",), extra_filter: Q | None = None):
    """
    Kanoniczny QS wiadomości, w których uczestniczą A i B wg zasad:
    - from_person -> delivered_to  OR  from_person -> recipient(kind in kinds)
    - NIE liczymy recipient <-> recipient
    - wykluczamy self-mail bez recipients
    """
    kinds = tuple(k.lower() for k in kinds)
    base = EmailMessage.objects.exclude(
        Q(from_person=F("delivered_to")) & Q(recipients__isnull=True)
    )
    if extra_filter is not None:
        base = base.filter(extra_filter)

    pair_q = (
        Q(from_person_id=a_id) & (
            Q(delivered_to_id=b_id) |
            Q(message_recipients__person_id=b_id,
              message_recipients__kind__in=kinds)
        )
    ) | (
        Q(from_person_id=b_id) & (
            Q(delivered_to_id=a_id) |
            Q(message_recipients__person_id=a_id,
              message_recipients__kind__in=kinds)
        )
    )

    return (base.filter(pair_q)
                .order_by("id")
                .distinct())


class Command(BaseCommand):
    help = (
        "Przebudowuje PartnerStat zgodnie z kanonicznym predykatem (from→delivered_to | from→TO), "
        "wykluczając self-mail bez recipients. Wynik jest zbieżny z ręcznym filtrowaniem."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--batch-size", type=int, default=2000,
                            help="Ile wiadomości przetwarzać w jednej paczce.")
        parser.add_argument("--since-id", type=int, default=None,
                            help="Przetwarzaj tylko EmailMessage o id > since_id.")
        parser.add_argument("--dry-run", action="store_true", default=False,
                            help="Nie zapisuj do bazy – tylko policz i wypisz statystyki.")
        parser.add_argument("--filter-q", dest="filter_q", default=None,
                            help="Dodatkowy filter, np. \"direction='received'\".")
        parser.add_argument("--kinds", type=str, default="TO",
                            help="Rodzaje adresatów, CSV (domyślnie: TO). Przykład: TO,CC")

    def handle(self, *args, **opts):
        batch_size: int = opts["batch_size"]
        since_id: Optional[int] = opts["since_id"]
        dry_run: bool = opts["dry_run"]
        filter_q_raw: Optional[str] = opts.get("filter_q")
        kinds_csv: str = opts["kinds"]
        kinds = tuple(x.strip().lower() for x in kinds_csv.split(",") if x.strip())

        # 1) Baza wiadomości (ta sama ekskluzja co w kanonicznym QS)
        qs = (EmailMessage.objects
              .exclude(Q(from_person=F("delivered_to")) & Q(recipients__isnull=True))
              .only("id", "from_person_id", "delivered_to_id",
                    "sent_at", "received_at", "user_processed", "useless")
              .order_by("id"))

        if since_id:
            qs = qs.filter(id__gt=since_id)

        extra_filter_q = None
        if filter_q_raw:
            try:
                key, val = filter_q_raw.split("=", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                extra_filter_q = Q(**{key: val})
                qs = qs.filter(extra_filter_q)
            except Exception:
                self.stdout.write(self.style.WARNING("Ignoruję --filter-q (nieprawidłowy format)"))
                extra_filter_q = None

        total_msgs = qs.count()
        self.stdout.write(self.style.NOTICE(f"Do przetworzenia: {total_msgs} wiadomości"))

        if not dry_run:
            self.stdout.write(self.style.WARNING("Kasuję PartnerStat..."))
            PartnerStat.objects.all().delete()

        # 2) Zbieramy kandydackie pary z przeglądu wiadomości (szybko)
        agg: Dict[Tuple[int, int], Counters] = defaultdict(Counters)

        start = 0
        processed_msgs = 0

        def flush_to_db():
            """
            Dla zebranych par wykonaj *kanoniczny* przelicznik:
            - msg_count: count(qs_emails_between)
            - msg_processed_count: count(qs_emails_between & (user_processed|useless))
            - last_message_at: max(GREATEST(received_at, sent_at))
            """
            if not agg:
                return

            with transaction.atomic():
                for (a_id, b_id) in list(agg.keys()):
                    pair_qs = qs_emails_between(a_id, b_id, kinds=kinds, extra_filter=extra_filter_q)

                    msg_count = pair_qs.values("id").count()
                    msg_processed_count = pair_qs.filter(
                        Q(user_processed=True) | Q(useless=True)
                    ).values("id").count()

                    last_dt = pair_qs.aggregate(
                        last=Greatest(Max("received_at"), Max("sent_at"))
                    )["last"]

                    if dry_run:
                        self.stdout.write(f"(a={a_id}, b={b_id}) -> count={msg_count}, processed={msg_processed_count}, last={last_dt}")
                    else:
                        obj, created = PartnerStat.objects.get_or_create(a_id=min(a_id, b_id),
                                                                         b_id=max(a_id, b_id),
                                                                         defaults={
                                                                             "msg_count": 0,
                                                                             "msg_processed_count": 0,
                                                                             "last_message_at": last_dt,
                                                                         })
                        PartnerStat.objects.filter(pk=obj.pk).update(
                            msg_count=msg_count,
                            msg_processed_count=msg_processed_count,
                            last_message_at=last_dt,
                        )
            agg.clear()

        while True:
            batch = list(qs[start:start + batch_size].iterator())
            if not batch:
                break

            for m in batch:
                # Budujemy pary dokladnie tak, jak w kanonicznym predykacie:
                # from -> delivered_to oraz from -> TO
                if not m.from_person_id:
                    processed_msgs += 1
                    continue

                # delivered_to (o ile różny od from)
                if m.delivered_to_id and m.delivered_to_id != m.from_person_id:
                    a, b = _canon_pair(m.from_person_id, m.delivered_to_id)
                    agg[(a, b)].has_any = True

                # TO recipients wg wybranych kinds
                for rid in _recipient_ids(m.id, kinds=kinds):
                    if rid == m.from_person_id:
                        continue
                    a, b = _canon_pair(m.from_person_id, rid)
                    agg[(a, b)].has_any = True

                processed_msgs += 1

            self.stdout.write(f"Przetworzono: {processed_msgs}/{total_msgs}")
            flush_to_db()
            start += batch_size

        flush_to_db()

        self.stdout.write(self.style.SUCCESS(
            f"Zakończono. Zliczono {processed_msgs} wiadomości. "
            f"{'Dry-run – brak zapisu.' if dry_run else 'Statystyki zapisane.'}"
        ))
