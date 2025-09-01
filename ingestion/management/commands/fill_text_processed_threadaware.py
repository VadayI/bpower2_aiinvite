# fill_text_processed_threadaware.py
import re
from typing import Optional

from django.core.management.base import BaseCommand
from django.db.models import Q

from ingestion.models import EmailMessage


# ===== Heurystyki de-quotingu =====

_SUBJECT_SEPARATORS = re.compile(r"[-=_]{3,}")  # -----, ====, ___
_MULTI_SPACES = re.compile(r"[ ]{2,}")
_MULTI_NEWLINES = re.compile(r"\n{3,}")

QUOTE_LINE_PREFIXES = (
    re.compile(r"^>+"),
    re.compile(r"^\|+"),
)

# --- Dotychczasowe "bloki" ---
QUOTE_BLOCK_START = (
    re.compile(r"^-{2,}\s*Original Message", re.I),
    re.compile(r"^\*?\s*From:", re.I),
    re.compile(r"^\*?\s*Od:", re.I),
    re.compile(r"^On .+ wrote:", re.I),
    re.compile(r"^W dniu .+ pisze:", re.I),
    re.compile(r"^________________________________", re.I),
)

# === wykrywanie nagłówków odpowiedzi/forwardów (Outlook/Gmail, PL/EN) ===
STAR_OPT = r"\*?\s*"              # opcjonalne gwiazdki/Markdown
FIELD_SEP = r"\s*:\s*"            # separator po nazwie pola
HEADER_FIELDS = [
    r"(From|Od|De|Von)",
    r"(To|Do|Para)",
    r"(Cc|DW)",
    r"(Subject|Temat|Betreff)",
    r"(Sent|Wysłano|Gesendet|Enviado|Inviato|Date|Data)",
]
REPLY_HEADER_LINE = re.compile(
    rf"^[>\-\s]*\*?\s*(?:{'|'.join(HEADER_FIELDS)}){FIELD_SEP}.+$",
    re.I
)

# "On Tue, Apr ... wrote:" / "W dniu 22 ... pisze:"
ON_WROTE = (
    re.compile(rf"^{STAR_OPT}On .+ wrote:{STAR_OPT}$", re.I),
    re.compile(rf"^{STAR_OPT}W dniu .+ (pisze|napisał|napisała):{STAR_OPT}$", re.I),
)

# Linia-separator używana w wielu klientach
HARD_SEPARATORS = re.compile(r"^[-_*]{5,}$")

# === wykrywanie stopek ===
SIGNATURE_CUES = [
    "pozdrawiam", "z poważaniem", "z wyrazami szacunku",
    "kind regards", "best regards", "regards",
    "mit freundlichen grüßen", "mfg",
]
CONTACT_CUES = (
    re.compile(r"\b(?:tel|phone|mobile|kom|fax)\b", re.I),
    re.compile(r"\b(?:www\.|https?://|linkedin\.com|@)\S+", re.I),
    re.compile(r"\bPL\d{9,}\b"),                     # NIP/REGON etc. (ostrożnie)
    re.compile(r"\b\d{2,4}[-\s]?\d{2,3}[-\s]?\d{2,3}\b"),  # numery tel / lokalne formaty
)

URL_ONLY = re.compile(r"^\s*(https?://\S+|www\.\S+|\S+\.\w{2,})(\s*)$", re.I)

def _normalize_whitespace(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\t", " ")
    text = text.replace("\u00A0", " ")  # NBSP -> zwykła spacja
    text = _SUBJECT_SEPARATORS.sub(" ", text)
    text = _MULTI_SPACES.sub(" ", text)
    text = _MULTI_NEWLINES.sub("\n\n", text)
    text = "\n".join(line.rstrip() for line in text.splitlines())
    return text.strip()

# === twarde cięcie przy „header cluster” (np. *From:* /*Od:*) ===
def _cut_at_reply_headers(text: str) -> str:
    """
    Tnie w miejscu, gdzie zaczyna się blok nagłówków poprzedniej korespondencji.
    - obsługuje *From:*, *Od:*, *Sent/Wysłano:*, *To/Do:*, *Cc/DW:*, *Subject/Temat:*
    - cięcie również na 'On ... wrote:' / 'W dniu ... pisze:'
    - toleruje gwiazdki i linie separatorów
    """
    if not text:
        return ""

    lines = text.splitlines()
    n = len(lines)

    def is_header_line(i: int) -> bool:
        s = lines[i].strip()
        if not s:
            return False
        if HARD_SEPARATORS.match(s):
            return True
        if REPLY_HEADER_LINE.match(s):
            return True
        if any(p.match(s) for p in ON_WROTE):
            return True
        return False

    i = 0
    while i < n:
        if is_header_line(i):
            # Heurystyka: jeśli w najbliższych 6 liniach są >=2 nagłówki pól, uznajemy za klaster
            look_ahead = lines[i:i+6]
            header_hits = sum(1 for l in look_ahead if REPLY_HEADER_LINE.match(l.strip()))
            if header_hits >= 2 or any(p.match(lines[i].strip()) for p in ON_WROTE):
                return "\n".join(lines[:i]).rstrip()
        i += 1
    return text

def strip_quoted(text: str) -> str:
    # Najpierw klasyczne „>”/„Original Message”
    out_lines = []
    for line in text.splitlines():
        s = line.strip()
        if any(p.match(s) for p in QUOTE_LINE_PREFIXES):
            continue
        if any(p.match(s) for p in QUOTE_BLOCK_START):
            break
        out_lines.append(line)
    base = "\n".join(out_lines).strip()

    # mocniejsze cięcie bloków nagłówków (Outlook-style)
    base = _cut_at_reply_headers(base)
    return base

# === usuwanie stopek / banerów na końcu ===
def _strip_signature_and_link_banners(text: str) -> str:
    if not text:
        return ""

    lines = text.splitlines()
    n = len(lines)

    # 1) wytnij "baner linkowy" na końcu (>=2 kolejne linie będące *wyłącznie* URLami/domenami)
    k = n
    consec = 0
    while k > 0:
        if lines[k-1].strip() == "":
            k -= 1
            continue
        if URL_ONLY.match(lines[k-1].strip()):
            consec += 1
            k -= 1
            continue
        break
    if consec >= 2:
        lines = lines[:k]  # obcięcie ogona z linków

    # 2) heurystyczne wycinanie stopek: szukamy *ostatniej* formuły grzecznościowej,
    # która pojawia się w dolnych 40% tekstu i po której w kolejnych liniach są "sygnały kontaktowe"
    low_idx = int(len(lines) * 0.6)
    sig_idx = -1
    for i in range(len(lines)-1, max(low_idx-1, -1), -1):
        s = lines[i].strip().lower()
        if any(cue in s for cue in SIGNATURE_CUES):
            # sprawdź do 6 kolejnych linii
            tail = "\n".join(lines[i:i+7])
            if sum(bool(rx.search(tail)) for rx in CONTACT_CUES) >= 2:
                sig_idx = i
                break
    if sig_idx != -1:
        lines = lines[:sig_idx]

    return "\n".join(lines).rstrip()

def clean_for_training(text: str) -> str:
    if not text:
        return ""
    text = _normalize_whitespace(text)
    text = strip_quoted(text)
    text = _strip_signature_and_link_banners(text)  # NEW
    return text

def trim_repeated_within_thread(msg: EmailMessage, text: str, lookback_messages: int = 3, overlap_chars: int = 300) -> str:
    if not text or not msg.thread_id:
        return text
    prev_qs = (
        EmailMessage.objects.filter(thread_id=msg.thread_id, id__lt=msg.id)
        .exclude(text_processed__isnull=True)
        .only("id", "text_processed")
        .order_by("-id")[:lookback_messages]
    )
    for prev in prev_qs:
        prev_text = (prev.text_processed or "").strip()
        if not prev_text:
            continue
        # === porównujemy po normalizacji i casefold, szukamy *blisko końca* ===
        a = text.casefold()
        b = prev_text.casefold()
        k = min(len(b), overlap_chars)
        if k < 50:
            continue
        suffix = a[-(k+50):] if len(a) > k+50 else a
        if b[:k] in suffix:
            cut_pos = a.rfind(b[:k])
            if cut_pos != -1:
                # przelicz na indeks w oryginalnym tekście (ta sama długość po casefold)
                return text[:cut_pos].rstrip()
    return text

# ===== Management Command =====
class Command(BaseCommand):
    help = (
        "Czyści pole text_processed (lub tworzy je jeśli puste), "
        "usuwając cytaty i powtórzenia w obrębie wątku. "
        "Na końcu zawsze nadpisuje wynik do text_processed."
    )

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0, help="Maksymalna liczba rekordów do przetworzenia (0 = wszystkie).")
        parser.add_argument("--dry-run", action="store_true", help="Podgląd bez zapisu do bazy.")
        parser.add_argument("--since", type=str, help="Filtr daty (YYYY-MM-DD) po sent_at/received_at.")
        parser.add_argument("--lookback", type=int, default=5, help="Ile poprzednich maili z wątku sprawdzać pod kątem powtórek.")
        parser.add_argument("--overlap", type=int, default=500, help="Ile znaków porównywać na końcu/początku.")

    def _base_queryset(self, since: Optional[str]):
        qs = EmailMessage.objects.filter(formatted_text=False)
        if since:
            qs = qs.filter(Q(sent_at__date__gte=since) | Q(received_at__date__gte=since))
        return qs.only("id", "thread_id", "text_processed", "sent_at", "received_at").order_by("id")

    def handle(self, *args, **opts):
        limit = int(opts["limit"] or 0)
        dry = bool(opts["dry_run"])
        since = opts.get("since")
        lookback = int(opts["lookback"])
        overlap = int(opts["overlap"])

        qs = self._base_queryset(since)
        if limit:
            qs = qs[:limit]

        if not qs.exists():
            self.stdout.write(self.style.WARNING("Brak wiadomości do przetworzenia."))
            return

        self.stdout.write(self.style.MIGRATE_HEADING(
            f"fill_text_processed_threadaware: limit={limit or '∞'}, since={since or '-'}, "
            f"lookback={lookback}, overlap={overlap}, dry_run={'TAK' if dry else 'NIE'}"
        ))

        updated = 0
        seen = 0

        for msg in qs.iterator(chunk_size=200):
            seen += 1

            cleaned = clean_for_training(msg.text_processed)
            cleaned = trim_repeated_within_thread(msg, cleaned, lookback_messages=lookback, overlap_chars=overlap)

            if dry:
                sample = cleaned.replace("\n", " ")[:160]
                self.stdout.write(f"[DRY] msg#{msg.id}: {sample}{'…' if len(cleaned) > 160 else ''}")
                continue

            # zawsze nadpisujemy text_processed
            msg.text_processed = cleaned
            msg.formatted_text = True
            msg.save(update_fields=["text_processed", "formatted_text"])
            updated += 1

        if dry:
            self.stdout.write(self.style.SUCCESS(f"[DRY] Przetworzono: {seen}, nadpisanych: {updated}"))
        else:
            self.stdout.write(self.style.SUCCESS(f"Nadpisano text_processed w {updated} z {seen} rekordów."))
