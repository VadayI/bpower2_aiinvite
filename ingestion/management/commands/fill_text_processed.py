import re
from django.core.management.base import BaseCommand
import unicodedata


from ingestion.models import EmailMessage

# zestaw dozwolonych znaków specjalnych
CHARACTERS_ALLOWED = set("?!.,*:@<>/_+-()[]{}=|&%$#^~\"'–")

def clean_for_training(text: str) -> str:
    """Czyści treść maila do postaci przydatnej dla modeli."""
    if not text:
        return ""

    # 1) Zamień wszystkie whitespace na spacje/nowe linie
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\t", " ")

    # 2) Usuń dekoracyjne separatory
    text = re.sub(r"[-=_]{3,}", " ", text)

    # 3) Usuń nadmiarowe spacje
    text = re.sub(r"[ ]{2,}", " ", text)

    # 4) Usuń nadmiarowe nowe linie (max 2 pod rząd)
    text = re.sub(r"\n{3,}", "\n\n", text)

    # 5) Przytnij spacje na końcach linii
    text = "\n".join(line.strip() for line in text.splitlines())

    # 6) Usuń puste linie na początku/końcu
    text = text.strip()

    return text

def check_string_is_correct(s: str):
    """
    Sprawdza czy string jest poprawny.
    Dozwolone: litery, cyfry, spacje, polskie znaki i standardowe symbole.
    Zwraca (bool, lista_błędnych_znaków).
    """
    def is_correct(ch):
        cat = unicodedata.category(ch)
        return (
            cat[0] in ("L", "N")
            or ch.isspace()
            or ch in CHARACTERS_ALLOWED
        )
    
    incorrect = [ch for ch in s if not is_correct(ch)]
    return len(incorrect) == 0


class Command(BaseCommand):
    help = "Uzupełnia pole text_processed na podstawie text_html lub text_plain."

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit", type=int, default=0,
            help="Ile rekordów maksymalnie przetworzyć (0 = wszystkie)"
        )
        parser.add_argument(
            "--dry-run", action="store_true",
            help="Pokaż tylko podgląd, nie zapisuj do bazy"
        )

    def handle(self, *args, **opts):
        limit = opts["limit"]
        dry_run = opts["dry_run"]

        qs = EmailMessage.objects.all().only("id", "text_html", "text_plain")

        if limit:
            qs = qs[:limit]

        updated = 0
        for msg in qs.iterator(chunk_size=200):
            text_html = msg.text_html_parsed or ""
            text_plain = msg.text_plain or ""

            if len(text_html) >= (len(text_plain) * 1.5) and check_string_is_correct(text_html):
                chosen = text_html
            else:
                chosen = text_plain

            processed = clean_for_training(chosen)
            if not processed:
                continue

            if dry_run:
                self.stdout.write(f"[{msg.id}] {processed[:120]}...")
            else:
                msg.text_processed = processed
                msg.save(update_fields=["text_processed"])
                updated += 1

        if not dry_run:
            self.stdout.write(self.style.SUCCESS(f"Zaktualizowano {updated} wiadomości."))
