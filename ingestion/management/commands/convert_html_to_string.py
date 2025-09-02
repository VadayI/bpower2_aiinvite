from django.core.management.base import BaseCommand

from ingestion.models import EmailMessage
from ingestion.services import html_to_text


class Command(BaseCommand):
    help = "Konwertuje text_html -> text_html_parsed dla wiadomości, które jeszcze tego nie mają."

    def handle(self, *args, **opts):
        qs = EmailMessage.objects.filter(
            text_html__isnull=False,
            text_html__gt="",
            text_html_parsed__isnull=True,
        ).only("id", "text_html")

        total = qs.count()
        if total == 0:
            self.stdout.write(self.style.SUCCESS("Brak rekordów do przetworzenia."))
            return

        updated = 0
        for idx, obj in enumerate(qs.iterator(chunk_size=500), start=1):
            try:
                obj.text_html_parsed = html_to_text(obj.text_html)
                # używamy update_fields, żeby nie dotykać innych pól
                obj.save(update_fields=["text_html_parsed"])
                updated += 1
            except Exception as e:
                # nie przerywaj całego batcha; zaloguj i jedź dalej
                self.stderr.write(f"ID {obj.id}: {e}")

            if idx % 500 == 0:
                remaining = total - idx
                self.stdout.write(f"Przetworzono {idx}/{total} (pozostało {remaining})...")

        self.stdout.write(self.style.SUCCESS(f"Zaktualizowano: {updated}/{total}"))
