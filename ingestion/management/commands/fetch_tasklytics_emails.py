from django.core.management.base import BaseCommand, CommandParser

from ingestion.services_client import TasklyticsClient
from ingestion.services import import_external_messages


class Command(BaseCommand):
    help = "Pobiera WSZYSTKIE strony e-maili z Tasklytics (INBOX/SENT) i importuje do bazy."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--base-url", default="https://api.tasklytics.eu", help="Bazowy URL API")
        parser.add_argument("--login", required=True, help="Login do API")
        parser.add_argument("--password", required=True, help="Hasło do API")
        parser.add_argument("--mailbox-id", required=True, type=int, help="mailBoxId")
        parser.add_argument("--folders", default="INBOX,SENT", help="Lista folderów, np. INBOX,SENT")
        parser.add_argument("--msg-per-page", type=int, default=100, help="Rozmiar strony (messagePerPage)")
        parser.add_argument("--page-from", type=int, default=100, help="Strona początkowa")
        parser.add_argument("--page-to", type=int, default=100, help="Strona końcowa")


    def handle(self, *args, **opts):
        base_url = opts["base_url"]
        login = opts["login"]
        password = opts["password"]
        mailbox_id = opts["mailbox_id"]
        folders = [f.strip().upper() for f in opts["folders"].split(",") if f.strip()]
        msg_per_page = opts["msg_per_page"]
        page_from = opts["page_from"]
        page_to = opts["page_to"]

        client = TasklyticsClient(base_url=base_url, login=login, password=password)
        client.authenticate()

        total_created = 0
        total_skipped = 0

        for folder in folders:
            self.stdout.write(self.style.NOTICE(f"Pobieram folder: {folder}"))
            items = list(client.iter_details_for_folder(mailbox_id, folder, message_per_page=msg_per_page, page_from=page_from, page_to=page_to))

            result = import_external_messages(items)
            total_created += result["created"]
            total_skipped += result["skipped"]

            self.stdout.write(self.style.SUCCESS(
                f"Folder {folder}: utworzono {result['created']}, pominięto {result['skipped']}"
            ))

        self.stdout.write(self.style.SUCCESS(
            f"Skończone. Łącznie utworzono: {total_created}, pominięto: {total_skipped}"
        ))
