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

        updated = 0
        for obj in qs.iterator(chunk_size=500):
            try:
                obj.text_html_parsed = html_to_text(obj.text_html)
                obj.save(update_fields=["text_html_parsed"])
                updated += 1
            except Exception as e:
                # nie przerywaj całego batcha; zaloguj i jedź dalej
                self.stderr.write(f"ID {obj.id}: {e}")
        self.stdout.write(self.style.SUCCESS(f"Zaktualizowano: {updated}"))
