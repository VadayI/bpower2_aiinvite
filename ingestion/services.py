from django.db import transaction
from django.utils import timezone

import base64
import binascii
import re
from typing import Iterable, Mapping, Optional
from datetime import timezone as tz, timedelta, datetime
from email.utils import parsedate_to_datetime

import html as html_lib
from textwrap import fill
from bs4 import BeautifulSoup, NavigableString, Tag

from .models import EmailMessage, Person, MessageRecipient


# --- Reguły / stałe ----------------------------------------------------------

SUBJECT_PREFIX_RE = re.compile(r"^\s*((re|fw|fwd|odp|wg|sv)\s*:\s*)+", flags=re.I)
META_CHARSET_RE = re.compile(br'charset\s*=\s*["\']?([A-Za-z0-9_\-]+)', re.I)
XML_DECL_RE = re.compile(br'^<\?xml[^>]*encoding=["\']([A-Za-z0-9_\-]+)["\']', re.I)
OFFSET_RE = re.compile(r'([+-])(\d{2})(\d{2})$')

# --- Dekodacja treści (base64 -> tekst) --------------------------------------

def _add_b64_padding(s: str) -> str:
    """Dopasuj padding, gdy API zwróciło base64 bez '='."""
    m = len(s) % 4
    return s + ("=" * (4 - m)) if m else s

def _guess_encoding_from_bytes(b: bytes, is_html: bool) -> Optional[str]:
    """Zgadnij kodowanie z BOM/HTML meta/XML encoding."""
    # 1) BOM
    if b.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    if b.startswith(b"\xff\xfe\x00\x00"):
        return "utf-32le"
    if b.startswith(b"\x00\x00\xfe\xff"):
        return "utf-32be"
    if b.startswith(b"\xff\xfe"):
        return "utf-16le"
    if b.startswith(b"\xfe\xff"):
        return "utf-16be"

    # 2) HTML <meta charset=...> lub XML encoding="..."
    head = b[:2048].lower()
    if is_html:
        m = META_CHARSET_RE.search(head)
        if m:
            return m.group(1).decode("ascii", "ignore")
    else:
        m = XML_DECL_RE.search(head)
        if m:
            return m.group(1).decode("ascii", "ignore")

    return None

def b64_to_text(v: Optional[str], *, is_html: bool) -> str:
    """
    Dekoduje base64 do napisu, wykrywając kodowanie.
    Obsługuje urlsafe/niedopadane base64 i popularne charsety (utf-8/utf-16/utf-32/cp1250/iso-8859-2/latin-1).
    """
    if not v:
        return ""
    try:
        s = v.strip()
        try:
            b = base64.urlsafe_b64decode(_add_b64_padding(s))
        except binascii.Error:
            b = base64.b64decode(_add_b64_padding(s))
    except Exception:
        return ""

    enc = _guess_encoding_from_bytes(b, is_html=is_html)
    candidates = [c for c in [enc, "utf-8", "utf-8-sig", "cp1250", "iso-8859-2", "latin-1"] if c]

    for codec in candidates:
        try:
            return b.decode(codec)
        except Exception:
            continue

    # Ostateczność
    try:
        return b.decode("utf-8", errors="replace")
    except Exception:
        return b.decode("latin-1", errors="replace")


# --- Pomocnicze --------------------------------------------------------------

def normalize_subject(subject: Optional[str]) -> str:
    if not subject:
        return ""
    return SUBJECT_PREFIX_RE.sub("", subject).strip()

def strip_angle(v: str) -> str:
    return v.strip().strip("<>").strip()

def _to_aware(dt: datetime, tz):
    if timezone.is_aware(dt):
        return dt
    # zoneinfo + pytz kompatybilnie:
    try:
        return timezone.make_aware(dt, tz)  # preferowane w Django
    except Exception:
        return dt.replace(tzinfo=tz)

def parse_dt(v: str | None, default_tz=None):
    """
    Zwraca datetime ze strefą:
    - RFC 2822/5322 (np. 'Wed, 19 Feb 2025 12:43:57 +0100' / '... GMT') → używa strefy z pola
    - 'YYYY-MM-DD HH:MM:SS' (bez strefy) → używa default_tz (domyślnie TZ serwera)
    - ISO '2025-02-19T12:43:57Z' / '...+01:00' → używa strefy z pola; gdy brak → default_tz
    """
    if not v:
        return None

    if default_tz is None:
        default_tz = timezone.get_current_timezone()

    s = v.strip()
    try:
        # 1) RFC 2822/5322
        if "," in s or "GMT" in s or "+" in s or "-" in s[10:]:
            dt = parsedate_to_datetime(s)
            return _to_aware(dt, default_tz)

        # 2) 'YYYY-MM-DD HH:MM:SS' (bez strefy)
        if " " in s and ":" in s:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            return _to_aware(dt, default_tz)

        # 3) ISO z T (i ewentualnie Z/+01:00)
        if "T" in s:
            s_iso = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s_iso)
            return _to_aware(dt, default_tz)

        return None
    except Exception:
        return None
    
def extract_tzinfo(s: str):
    """
    Próbuj wydobyć strefę czasową z końcówki napisu daty.
    Obsługuje np. '+0100', '-0230', 'Z', 'GMT'.
    Zwraca tzinfo albo None.
    """
    if not s:
        return None
    s = s.strip()

    if s.endswith("Z"):
        return tz.utc
    if s.endswith("GMT"):
        return tz.utc

    m = OFFSET_RE.search(s)
    if m:
        sign, hh, mm = m.groups()
        offset = int(hh) * 60 + int(mm)
        if sign == "-":
            offset = -offset
        return tz(timedelta(minutes=offset))

    return None

def person_from_addr(address: Optional[str]) -> Person:
    addr = (address or "").strip()
    if "<" in addr and ">" in addr:
        addr = addr.split("<", 1)[1].split(">", 1)[0].strip()
    addr = addr.lower()  # <-- normalizacja
    domain = addr.split("@")[-1] if "@" in addr else ""
    obj, _ = Person.objects.get_or_create(
        email=addr,
        defaults={"display_name": "", "domain": domain},
    )
    return obj

def compute_thread_hint(item: Mapping) -> str:
    in_reply_to = item.get("inReplyTo") or []
    references = item.get("references") or []
    message_id_header = (item.get("messageIdFromHeader") or "").strip()
    subject = item.get("subject") or ""

    if in_reply_to:
        return strip_angle(in_reply_to[0])
    if references:
        return strip_angle(references[0])
    if message_id_header:
        return strip_angle(message_id_header)
    return f"subject:{normalize_subject(subject)[:240]}"


# --- Główny import -----------------------------------------------------------

@transaction.atomic
def import_external_messages(items: Iterable[Mapping]) -> dict:
    """
    Jednorazowy import listy słowników (JSON) ze źródła zewnętrznego.

    Oczekiwane pola m.in.:
    - id, messageId, mailBoxId
    - fromAddress, deliveredTo
    - toAddresses[], ccAddresses[], bccAddresses[]
    - receivedDate, sentDate, subject
    - messageIdFromHeader, inReplyTo[], references[]
    - textPlain (base64), textHtml (base64)
    - folder ("INBOX" / "SENT")  ← używane do ustawienia 'direction'

    Tworzy: Person, EmailMessage, MessageRecipient.
    NIE tworzy Thread — jedynie zapisuje 'thread_hint' do późniejszego spięcia.
    """
    created = 0
    skipped = 0

    for item in items:
        external_id = int(item.get("id"))
        if EmailMessage.objects.filter(external_id=external_id).exists():
            skipped += 1
            continue

        subject = item.get("subject") or ""
        from_p = person_from_addr(item.get("fromAddress"))
        # delivered_to zostawiamy – to identyfikacja skrzynki, ale nie tworzymy M2M z tego pola
        delivered_p = person_from_addr(item.get("deliveredTo")) if item.get("deliveredTo") else None

        folder = (item.get("folder") or "").upper()
        direction = EmailMessage.Direction.SENT if folder == "SENT" else EmailMessage.Direction.RECEIVED

        sent_raw = item.get("sentDate")
        recv_raw = item.get("receivedDate")
        tz_sys = extract_tzinfo(sent_raw) or extract_tzinfo(recv_raw) or timezone.get_current_timezone()

        msg = EmailMessage.objects.create(
            external_id=external_id,
            external_message_id=item.get("messageId") or "",
            mailbox_id=item.get("mailBoxId"),
            thread=None,  # wątek przypiszemy później
            thread_hint=compute_thread_hint(item),
            subject=subject,
            from_person=from_p,
            delivered_to=delivered_p,
            sent_at=parse_dt(sent_raw, default_tz=tz_sys),
            received_at=parse_dt(recv_raw, default_tz=tz_sys),
            direction=direction,
            message_id_header=(item.get("messageIdFromHeader") or "").strip(),
            in_reply_to_header=strip_angle(item["inReplyTo"][0]) if item.get("inReplyTo") else "",
            references_header=";".join([strip_angle(r) for r in (item.get("references") or [])]),
            text_plain=b64_to_text(item.get("textPlain"), is_html=False),
            text_html=b64_to_text(item.get("textHtml"), is_html=True),
            is_unread=item.get("isUnread") if "isUnread" in item else None,
            raw_payload=item,  # zachowujemy pełny JSON (z folderem)
        )

        # ODBIORCY: WYŁĄCZNIE To/CC/BCC)
        for kind in [("toAddresses", MessageRecipient.Kind.TO), ("bccAddresses", MessageRecipient.Kind.BCC), ("ccAddresses", MessageRecipient.Kind.CC)]:
            kind_name = kind[0]
            kind_code = kind[1]
            to_addresses = item.get(kind_name) or []
            for a in to_addresses:
                p = person_from_addr(a)  # <- tworzymy Person tylko dla To
                MessageRecipient.objects.get_or_create(
                    message=msg,
                    person=p,
                    kind=kind_code,
                )
        created += 1

    return {"created": created, "skipped": skipped}

# ----------- Html to string -----------------------------------------

def html_to_text(html_input: str, *, max_width: int | None = 0) -> str:
    """
    Konwertuje HTML na czytelny tekst. Odporna na uszkodzony HTML.
    """
    if not html_input:
        return ""

    soup = BeautifulSoup(html_input, "html.parser")

    # 1) Usuń tagi nienadające się do czytania
    for t in soup.find_all(["script", "style", "noscript", "template"]):
        try:
            t.decompose()
        except Exception:
            pass

    # 2) Znaczniki linii dla bloków
    def mark_block(tag_name, before="\n", after="\n"):
        for t in soup.find_all(tag_name):
            try:
                t.insert_before(NavigableString(before))
                t.insert_after(NavigableString(after))
            except Exception:
                continue

    for br in list(soup.find_all("br")):
        try:
            br.replace_with(NavigableString("\n"))
        except Exception:
            pass

    for name in ["p", "div", "section", "article", "header", "footer"]:
        mark_block(name)

    for h in ["h1", "h2", "h3", "h4", "h5", "h6"]:
        mark_block(h)

    mark_block("tr", after="\n")
    mark_block("table", before="\n", after="\n")
    mark_block("blockquote", before="\n> ", after="\n")
    mark_block("hr", before="\n" + "-" * 40 + "\n", after="\n")

    # 3) Listy
    for ul in list(soup.find_all("ul")):
        for li in ul.find_all("li", recursive=False):
            try:
                li.insert_before(NavigableString("\n• "))
                li.insert_after(NavigableString("\n"))
            except Exception:
                continue

    for ol in list(soup.find_all("ol")):
        i = 1
        for li in ol.find_all("li", recursive=False):
            try:
                li.insert_before(NavigableString(f"\n{i}. "))
                li.insert_after(NavigableString("\n"))
            except Exception:
                pass
            i += 1

    # 4) Linki
    for a in list(soup.find_all("a")):
        if not isinstance(a, Tag):
            continue
        try:
            text = a.get_text(strip=True)
            href = (a.get("href") or "").strip()
            repl = text if text else href
            if href and text and href != text:
                repl = f"{text} ({href})"
            a.replace_with(NavigableString(repl))
        except Exception:
            continue

    # 5) Obrazy
    for img in list(soup.find_all("img")):
        if not isinstance(img, Tag):
            continue
        try:
            alt = (img.get("alt") or "").strip() if hasattr(img, "get") else ""
            if alt:
                img.replace_with(NavigableString(f"[img: {alt}]"))
            else:
                img.decompose()
        except Exception:
            try:
                img.decompose()
            except Exception:
                pass

    # 6) Tabele — separatory, by nie sklejać kolumn
    for td in list(soup.find_all("td")):
        try:
            td.insert_after(NavigableString("\t"))
        except Exception:
            pass
    for th in list(soup.find_all("th")):
        try:
            th.insert_after(NavigableString("\t"))
        except Exception:
            pass

    # 7) Do tekstu
    text = soup.get_text()

    # 8) Encje HTML
    text = html_lib.unescape(text).replace("\xa0", " ")

    # 9) Porządki w białych znakach
    import re as _re
    text = _re.sub(r"[ \t\f\v]+\n", "\n", text)     # trailing spaces
    text = _re.sub(r"\n{3,}", "\n\n", text)         # max 2 pustych linii
    text = "\n".join(line.strip() for line in text.splitlines())
    text = text.strip()

    # 10) Opcjonalne łamanie linii
    if max_width and max_width > 0:
        blocks = []
        for block in text.split("\n\n"):
            new_lines = []
            for ln in block.splitlines():
                if ln.startswith("> "):
                    new_lines.append(ln)
                else:
                    new_lines.append(fill(ln, width=max_width))
            blocks.append("\n".join(new_lines))
        text = "\n\n".join(blocks)

    return text
