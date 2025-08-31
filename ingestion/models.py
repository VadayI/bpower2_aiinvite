from django.db import models
from django.db.models import UniqueConstraint
from django.db.models.functions import Lower


class Person(models.Model):
    """Reprezentuje uczestnika korespondencji (nadawca/odbiorca)."""
    email = models.EmailField(
        unique=True,
        verbose_name="adres e-mail",
        help_text="Unikalny adres e-mail osoby."
    )
    
    display_name = models.CharField(
        max_length=255, blank=True,
        verbose_name="nazwa wyświetlana",
        help_text="Opcjonalna nazwa wyświetlana (z nagłówków wiadomości)."
    )
    domain = models.CharField(
        max_length=255, blank=True,
        verbose_name="domena",
        help_text="Domena wyprowadzona z adresu e-mail."
    )

    class Meta:
        verbose_name = "Osoba"
        verbose_name_plural = "Osoby"
        constraints = [
            UniqueConstraint(Lower("email"), name="unique_lower_email")
        ]

    def __str__(self) -> str:
        return self.display_name or self.email


class Thread(models.Model):
    """Logiczna grupa wiadomości powiązanych (wątek)."""
    thread_key = models.CharField(
        max_length=512, db_index=True, unique=True,
        verbose_name="klucz wątku",
        help_text="Stabilny identyfikator wątku (np. Message-ID/In-Reply-To/References/temat)."
    )
    subject_norm = models.CharField(
        max_length=500, blank=True,
        verbose_name="temat (oczyszczony)",
        help_text="Temat po usunięciu prefiksów typu RE:, FWD:, Odp:, itp."
    )
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="utworzono",
        help_text="Czas utworzenia wątku w systemie."
    )

    class Meta:
        verbose_name = "Wątek"
        verbose_name_plural = "Wątki"

    def __str__(self) -> str:
        return self.subject_norm or self.thread_key


class EmailMessage(models.Model):
    """Pojedyncza wiadomość e-mail (bez załączników) z treścią tekstową i HTML."""
    class Direction(models.TextChoices):
        RECEIVED = "received", "Odebrana"
        SENT = "sent", "Wysłana"

    external_id = models.BigIntegerField(
        unique=True,
        verbose_name="ID zewnętrzne",
        help_text="Identyfikator rekordu z systemu źródłowego."
    )
    external_message_id = models.CharField(
        max_length=255, blank=True,
        verbose_name="Message-ID (zewn.)",
        help_text="Id wiadomości z systemu źródłowego (jeśli inne niż nagłówek)."
    )
    mailbox_id = models.BigIntegerField(
        null=True, blank=True,
        verbose_name="ID skrzynki",
        help_text="Identyfikator skrzynki/poczty w systemie źródłowym."
    )

    thread = models.ForeignKey(
        Thread, null=True, blank=True, on_delete=models.SET_NULL,
        related_name="messages", verbose_name="wątek",
        help_text="Powiązany wątek; może być puste (zostanie przypisane później)."
    )
    thread_hint = models.CharField(
        max_length=512, blank=True,
        verbose_name="wstępny klucz wątku",
        help_text="Wartość pomocnicza do późniejszego łączenia w wątki."
    )

    subject = models.CharField(
        max_length=1000, blank=True,
        verbose_name="temat",
        help_text="Temat wiadomości."
    )

    from_person = models.ForeignKey(
        "Person", on_delete=models.PROTECT, related_name="sent_messages",
        verbose_name="nadawca",
        help_text="Osoba będąca nadawcą wiadomości."
    )
    delivered_to = models.ForeignKey(
        "Person", on_delete=models.PROTECT, related_name="delivered_messages",
        null=True, blank=True,
        verbose_name="dostarczono do",
        help_text="Adres docelowy skrzynki, do której trafiła wiadomość (jeśli dostępny)."
    )

    recipients = models.ManyToManyField(
        "Person", through="MessageRecipient", related_name="received_messages",
        verbose_name="adresaci",
        help_text="Lista adresatów To/CC/BCC."
    )

    sent_at = models.DateTimeField(
        null=True, blank=True,
        verbose_name="wysłano",
        help_text="Czas wysłania (z nagłówków)."
    )
    received_at = models.DateTimeField(
        null=True, blank=True,
        verbose_name="odebrano",
        help_text="Czas odebrania/otrzymania w skrzynce."
    )

    direction = models.CharField(
        max_length=8, choices=Direction.choices,
        verbose_name="kierunek",
        help_text="Czy wiadomość jest odebrana czy wysłana względem naszej skrzynki."
    )

    message_id_header = models.CharField(
        max_length=255, blank=True,
        verbose_name="Message-ID (nagłówek)",
        help_text="Wartość z nagłówka Message-ID."
    )
    in_reply_to_header = models.CharField(
        max_length=255, blank=True,
        verbose_name="In-Reply-To",
        help_text="Wartość z nagłówka In-Reply-To (jeśli istnieje)."
    )
    references_header = models.TextField(
        blank=True,
        verbose_name="References (lista)",
        help_text="Połączona lista identyfikatorów z nagłówka References."
    )

    text_plain = models.TextField(
        blank=True,
        verbose_name="treść (tekst)",
        help_text="Dekodowana treść text/plain (base64 → UTF-8)."
    )
    text_html = models.TextField(
        blank=True,
        verbose_name="treść (HTML)",
        help_text="Dekodowana treść text/html (base64 → UTF-8)."
    )
    text_html_parsed = models.TextField(
        null=True, blank=True,
        verbose_name="treść (HTML) String",
        help_text="Dekodowana treść text/html (base64 → UTF-8) przekonwertowana w string"
    )

    text_processed = models.TextField(
        null=True, blank=True,
        verbose_name="tekst przygotowany",
        help_text="Przygotowany tekst"
    )


    is_unread = models.BooleanField(
        null=True, blank=True,
        verbose_name="nieprzeczytane",
        help_text="Stan nieprzeczytania; opcjonalny (może być nieznany)."
    )
    raw_payload = models.JSONField(
        blank=True, null=True,
        verbose_name="surowy JSON",
        help_text="Oryginalny obiekt zewnętrzny do celów audytu/diagnozy."
    )

    user_processed = models.BooleanField(
        default=False, db_index=True,
        verbose_name="Etykietowano przez Użytkownika"
    )
    useless = models.BooleanField(
        default=False,
        verbose_name="Nieprzydatny do nauczenia modeli."
    )

    class Meta:
        verbose_name = "Wiadomość e-mail"
        verbose_name_plural = "Wiadomości e-mail"
        ordering = ["-received_at"]

    def __str__(self) -> str:
        return self.subject or "(brak tematu)"


class MessageRecipient(models.Model):
    """Powiązanie wiadomości z osobą jako odbiorcą (To/CC/BCC)."""

    class Kind(models.TextChoices):
        TO = "to", "Do"
        CC = "cc", "DW"
        BCC = "bcc", "UDW"

    message = models.ForeignKey(
        EmailMessage, on_delete=models.CASCADE, related_name="message_recipients",
        verbose_name="wiadomość", help_text="Wiadomość e-mail."
    )
    person = models.ForeignKey(
        Person, on_delete=models.PROTECT, related_name="message_recipient_links",
        verbose_name="osoba", help_text="Osoba będąca adresatem."
    )
    kind = models.CharField(
        max_length=3, choices=Kind.choices,
        verbose_name="rodzaj", help_text="Rodzaj adresata: Do/DW/UDW."
    )

    class Meta:
        verbose_name = "Adresat"
        verbose_name_plural = "Adresaci"
        unique_together = ("message", "person", "kind")

    def __str__(self) -> str:
        return f"{self.person} ({self.get_kind_display()})"


class PartnerStat(models.Model):
    """
    Zliczenia 'komunikacji' między dwiema osobami.
    Para jest UNORDERED – przechowujemy w konwencji a_id < b_id.
    Liczymy WYŁĄCZNIE relacje 'nadawca → TO' oraz 'delivered_to ↔ from_person'.
    NIE liczymy par recipient↔recipient (TO↔TO).
    """
    a = models.ForeignKey(Person, on_delete=models.CASCADE, related_name="+", db_index=True)
    b = models.ForeignKey(Person, on_delete=models.CASCADE, related_name="+", db_index=True)

    msg_count = models.PositiveIntegerField(default=0)
    msg_processed_count = models.PositiveIntegerField(default=0)  # user_processed=True OR useless=True
    last_message_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["a", "b"], name="uniq_partner_pair_ab"),
        ]

    def __str__(self):
        return f"{self.a_id} ↔ {self.b_id}  (msgs={self.msg_count}, processed={self.msg_processed_count})"
    
