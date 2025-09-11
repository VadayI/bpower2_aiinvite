from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Tuple, Iterable, Optional

from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction
from django.db.models import F
from django.utils.timezone import now

from ingestion.models import (
    EmailMessage, MessageRecipient, PartnerStat
)

# --- lok. helpers ------------------------------------------------------------

def _canon_pair(a_id: int, b_id: int) -> Tuple[int, int]:
    return (a_id, b_id) if a_id < b_id else (b_id, a_id)

def _recipient_ids(message_id: int) -> Iterable[int]:
    # WSZYSTKIE rodzaje adresatów: TO/CC/BCC
    return MessageRecipient.objects.filter(
        message_id=message_id,
        kind__in=[
            MessageRecipient.Kind.TO,
            # MessageRecipient.Kind.CC,
            # MessageRecipient.Kind.BCC,
        ]
    ).values_list("person_id", flat=True)

def _message_dt(sent_at, received_at):
    return received_at or sent_at or now()

@dataclass
class Counters:
    msg_count: int = 0
    msg_processed_count: int = 0
    last_message_at = None

    def add(self, processed: bool, dt):
        self.msg_count += 1
        if processed:
            self.msg_processed_count += 1
        if (self.last_message_at is None) or (dt and dt > self.last_message_at):
            self.last_message_at = dt


class Command(BaseCommand):
    help = (
        "Przebudowuje cache PartnerStat z istniejących EmailMessage (from ↔ all recipients + delivered_to). "
        "Domyślnie kasuje PartnerStat i liczy od zera."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--batch-size", type=int, default=2000,
                            help="Ile wiadomości przetwarzać w jednej paczce.")
        parser.add_argument("--since-id", type=int, default=None,
                            help="Opcjonalnie: przetwarzaj tylko EmailMessage o id > since_id.")
        parser.add_argument("--dry-run", action="store_true", default=False,
                            help="Nie zapisuj do bazy – tylko policz i wypisz statystyki.")
        parser.add_argument("--filter-q", dest="filter_q", default=None,
                            help="Dodatkowy Q filter dla EmailMessage, np. \"direction='received'\".")

    def handle(self, *args, **opts):
        batch_size: int = opts["batch_size"]
        since_id: Optional[int] = opts["since_id"]
        dry_run: bool = opts["dry_run"]
        filter_q_raw: Optional[str] = opts.get("filter_q")

        # 1) Wybór wiadomości do przebudowy
        qs = EmailMessage.objects.exclude(from_person=F("delivered_to")).only(
            "id", "from_person_id", "delivered_to_id",
            "sent_at", "received_at", "user_processed", "useless"
        ).order_by("id")

        if since_id:
            qs = qs.filter(id__gt=since_id)

        if filter_q_raw:
            # Bardzo prosty parser: oczekujemy formatu: pole='wartość'
            # Przykład: direction='sent'
            # Dla większej elastyczności dorób parser wg potrzeb.
            try:
                key, val = filter_q_raw.split("=", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                qs = qs.filter(**{key: val})
            except Exception:
                self.stdout.write(self.style.WARNING("Ignoruję --filter-q (nieprawidłowy format)"))

        total_msgs = qs.count()
        self.stdout.write(self.style.NOTICE(f"Do przetworzenia: {total_msgs} wiadomości"))

        # 2) Opcjonalny reset
        if not dry_run:
            self.stdout.write(self.style.WARNING("Kasuję PartnerStat..."))
            PartnerStat.objects.all().delete()

        # 3) Akumulacja w pamięci + periodyczny zapis
        agg: Dict[Tuple[int, int], Counters] = defaultdict(Counters)
        processed_msgs = 0

        def flush_to_db():
            """Zapisz bieżącą agregację do bazy (bulk upsert via get_or_create+update)."""
            if dry_run or not agg:
                return
            # Minimalnie: użyj transakcji i update per rekord (bez konfliktów)
            with transaction.atomic():
                for (a_id, b_id), c in agg.items():
                    obj, created = PartnerStat.objects.get_or_create(a_id=a_id, b_id=b_id, 
                                    defaults={
                                        "msg_count": 0,
                                        "msg_processed_count": 0,
                                        "last_message_at": c.last_message_at
                                    })

                    # zsumuj
                    new_msg = (obj.msg_count or 0) + c.msg_count
                    new_proc = (obj.msg_processed_count or 0) + c.msg_processed_count
                    new_last = c.last_message_at \
                        if (not obj.last_message_at or (c.last_message_at and c.last_message_at > obj.last_message_at))\
                        else obj.last_message_at
                    PartnerStat.objects.filter(pk=obj.pk).update(
                        msg_count=new_msg,
                        msg_processed_count=new_proc,
                        last_message_at=new_last,
                    )

        # 4) Iteracja po wiadomościach (batched)
        start = 0
        while True:
            batch = list(qs[start:start + batch_size].iterator())
            if not batch:
                break

            for m in batch:
                processed_flag = bool(m.user_processed or m.useless)
                dt = _message_dt(m.sent_at, m.received_at)

                if not m.from_person_id:
                    processed_msgs += 1
                    continue

                # Zbierz wszystkich „partnerów” tej wiadomości względem from_person:
                partner_ids = set(_recipient_ids(m.id))
                if m.delivered_to_id and m.delivered_to_id != m.from_person_id:
                    partner_ids.add(m.delivered_to_id)

                # Każda para jest UNORDERED i liczona maks. raz per wiadomość:
                for pid in partner_ids:
                    if pid == m.from_person_id:
                        continue
                    a, b = _canon_pair(m.from_person_id, pid)
                    agg[(a, b)].add(processed_flag, dt)

                processed_msgs += 1

            # co batch – spłucz do bazy i wyczyść słownik
            self.stdout.write(f"Przetworzono: {processed_msgs}/{total_msgs}")
            flush_to_db()
            agg.clear()
            start += batch_size

        # Końcowy flush (gdyby coś zostało)
        flush_to_db()

        self.stdout.write(self.style.SUCCESS(
            f"Zakończono. Zliczono {processed_msgs} wiadomości. "
            f"{'Dry-run – brak zapisu.' if dry_run else 'Statystyki zapisane.'}"
        ))
