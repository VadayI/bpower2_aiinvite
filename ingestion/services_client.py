import base64
import hashlib
import requests
from typing import Dict, Generator, Mapping, Optional


class TasklyticsClient:
    """
    Minimalny klient do pobierania maili z Tasklytics.
    Obsługuje auto-relogowanie gdy dostanie 401.
    """

    def __init__(self, base_url: str, login: str, password: str, *, session: Optional[requests.Session] = None):
        self.base_url = base_url.rstrip("/")
        self.login = login
        self.password = password
        self.session = session or requests.Session()
        self.token: Optional[str] = None

    # ------------------------------
    # Auth
    # ------------------------------
    def _user_key(self) -> str:
        sha = hashlib.sha256(self.password.encode("utf-8")).hexdigest()
        return base64.b64encode(f"{self.login}:{sha}".encode("utf-8")).decode("utf-8")

    def authenticate(self) -> str:
        url = f"{self.base_url}/app/legacy/login/v3?scope=PRODUCTION"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        resp = self.session.post(url, headers=headers, data=self._user_key(), timeout=30)
        resp.raise_for_status()
        data = resp.json()
        self.token = data[0]["cloudToken"]
        print('Authenticated')
        return self.token

    def _auth_headers(self) -> Dict[str, str]:
        if not self.token:
            self.authenticate()
        return {"Accept": "application/json", "Authorization": self.token}

    # ------------------------------
    # Helpers with auto-retry
    # ------------------------------
    def _get(self, url: str, *, params: Optional[dict] = None, timeout: int = 60) -> requests.Response:
        """GET z auto-relogin przy 401."""
        headers = self._auth_headers()
        resp = self.session.get(url, headers=headers, params=params, timeout=timeout)
        if resp.status_code == 401:
            # token wygasł → zaloguj się ponownie
            self.authenticate()
            headers = self._auth_headers()
            resp = self.session.get(url, headers=headers, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp

    # ------------------------------
    # API calls
    # ------------------------------
    def iter_message_ids(self, mailbox_id: int, folder: str, *, message_per_page: int = 100, page_from: int = 1, page_to: int = 1) -> Generator[str, None, None]:
        url = f"{self.base_url}/app/email/message/mails"
        page = page_from
        while True:
            params = {
                "page": page,
                "messagePerPage": message_per_page,
                "mailBoxId": mailbox_id,
                "folder": folder,
            }
            resp = self._get(url, params=params)
            payload = resp.json()
            rows = (payload.get("default") or {}).get("data") or []
            print(f'Page = {page}, len iter_message_ids = {len(rows)}')
            if not rows:
                break
            for row in rows:
                msg_id = row.get("Id") or row.get("id") or row.get("messageId")
                if msg_id:
                    yield str(msg_id)
            page += 1
            if page > page_to:
                break

    def fetch_details(self, mailbox_id: int, message_id: str) -> Mapping:
        url = f"{self.base_url}/app/email/message/details"
        params = {"mailBoxId": mailbox_id, "messageId": message_id}
        resp = self._get(url, params=params)
        return resp.json()

    def iter_details_for_folder(self, mailbox_id: int, folder: str, *, message_per_page: int = 100, page_from: int = 1, page_to: int = 1):
        for msg_id in self.iter_message_ids(mailbox_id, folder, message_per_page=message_per_page, page_from=page_from, page_to=page_to):
            details = self.fetch_details(mailbox_id, msg_id)
            if isinstance(details, dict):
                details.setdefault("folder", folder)
            yield details
