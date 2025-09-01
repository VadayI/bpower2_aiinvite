# ingestion/management/commands/import_tasklytics.py
from time import sleep
from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction

from ingestion.services_client import TasklyticsClient
from ingestion.services import import_external_messages


class Command(BaseCommand):
    help = "Pobiera e-maile z Tasklytics (INBOX/SENT) i importuje do bazy stronami (page-by-page)."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--base-url", default="https://api.tasklytics.eu", help="Bazowy URL API")
        parser.add_argument("--login", required=True, help="Login do API")
        parser.add_argument("--password", required=True, help="Hasło do API")
        parser.add_argument("--mailbox-id", required=True, type=int, help="mailBoxId")
        parser.add_argument("--folders", default="INBOX,SENT", help="Lista folderów, np. INBOX,SENT")
        parser.add_argument("--msg-per-page", type=int, default=100, help="Rozmiar strony (messagePerPage)")
        parser.add_argument("--page-from", type=int, default=1, help="Strona początkowa (1-indeksowana)")
        parser.add_argument("--page-to", type=int, default=None, help="Strona końcowa (włącznie); jeśli brak → do końca")
        parser.add_argument("--sleep", type=float, default=1.0, help="Odstęp (sekundy) między stronami, by nie zajechać API")

    def handle(self, *args, **opts):
        base_url     = opts["base_url"]
        login        = opts["login"]
        password     = opts["password"]
        mailbox_id   = opts["mailbox_id"]
        folders      = [f.strip().upper() for f in opts["folders"].split(",") if f.strip()]
        msg_per_page = opts["msg_per_page"]
        page_from    = opts["page_from"]
        page_to      = opts.get("page_to")
        pause        = max(0.0, float(opts["sleep"]))

        client = TasklyticsClient(base_url=base_url, login=login, password=password)
        client.authenticate()

        total_created = 0
        total_skipped = 0
        total_pages_processed = 0

        for folder in folders:
            self.stdout.write(self.style.NOTICE(
                f"Folder: {folder} | pages {page_from}..{page_to or '∞'} | size={msg_per_page}"
            ))

            page = page_from
            while True:
                if page_to and page > page_to:
                    break

                # 1) Pobierz ID wiadomości dla TEJ strony
                ids = client.fetch_message_ids_page(
                    mailbox_id, folder,
                    page=page,
                    message_per_page=msg_per_page
                )
                if not ids:
                    self.stdout.write(self.style.WARNING(
                        f"Folder {folder} str.{page}: brak rekordów — kończę folder."
                    ))
                    break

                # 2) Pobierz szczegóły
                page_items = []
                for mid in ids:
                    details = client.fetch_details(mailbox_id, mid)
                    if isinstance(details, dict):
                        details.setdefault("folder", folder)
                        page_items.append(details)

                # 3) Zapisz w transakcji
                with transaction.atomic():
                    result = import_external_messages(page_items)

                total_pages_processed += 1
                total_created += result["created"]
                total_skipped += result["skipped"]

                self.stdout.write(self.style.SUCCESS(
                    f"Folder {folder} str.{page}: utworzono {result['created']}, pominięto {result['skipped']} (IDs: {len(ids)})"
                ))

                page += 1
                if pause:
                    sleep(pause)

        self.stdout.write(self.style.SUCCESS(
            f"Skończone. Stron: {total_pages_processed}, utworzono: {total_created}, pominięto: {total_skipped}"
        ))
