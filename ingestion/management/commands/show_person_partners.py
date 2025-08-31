from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q, Exists, OuterRef
from ingestion.models import Person, EmailMessage, MessageRecipient
from ingestion.services import communication_partners_with_counts


class Command(BaseCommand):
    help = "Pokaż partnerów komunikacji i wiadomości dla wskazanego Person (po e-mailu)."

    def add_arguments(self, parser):
        parser.add_argument(
            "email",
            type=str,
            help="Adres e-mail osoby (Person.email), dla której chcemy zobaczyć partnerów komunikacji.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=5,
            help="Ilość wiadomości do podglądu per partner (domyślnie 5).",
        )

    def handle(self, *args, **options):
        email = options["email"]
        limit = options["limit"]

        try:
            person = Person.objects.get(email__iexact=email)
        except Person.DoesNotExist:
            raise CommandError(f"Nie znaleziono osoby o adresie e-mail: {email}")

        self.stdout.write(self.style.SUCCESS(f"Analiza komunikacji dla: {person} <{person.email}>"))

        partners_qs = communication_partners_with_counts(person)
        if not partners_qs.exists():
            self.stdout.write(self.style.WARNING("Brak komunikacji z innymi osobami."))
            return

        # Helpery do sprawdzania współwystąpienia w wiadomości (Exists dla wydajności)
        def involves(p):
            return (
                Q(from_person=p)
                | Q(delivered_to=p)
                | Exists(MessageRecipient.objects.filter(message=OuterRef("pk"), person=p))
            )

        for partner in partners_qs:
            count = getattr(partner, "msg_count", None)
            suffix = f" ({count} wiadomości)" if count is not None else ""
            self.stdout.write(self.style.MIGRATE_HEADING(f"\nPartner: {partner}{suffix}"))

            # Wiadomości, w których biorą udział *oba* podmioty (niezależnie od kierunku)
            msgs = (
                EmailMessage.objects.filter(involves(person) & involves(partner))
                .select_related("from_person", "delivered_to", "thread")
                .prefetch_related("message_recipients__person")
                .order_by("-received_at", "-sent_at", "-id")
            )[:limit]

            if not msgs:
                self.stdout.write(self.style.WARNING("  (brak wiadomości do podglądu)"))
                continue

            for m in msgs:
                subj = m.subject or "(brak tematu)"
                direction = m.get_direction_display()
                ts = m.sent_at or m.received_at
                self.stdout.write(f"  - {ts}: [{direction}] {subj}")
