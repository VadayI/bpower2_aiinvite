from django.contrib import admin
from django.db.models import Count
from django.utils.html import strip_tags
from django.utils.text import Truncator

from .models import Person, Thread, EmailMessage, MessageRecipient,\
    PartnerStat


# -----------------------------
# Wspólne helpery
# -----------------------------
def _snippet(text: str, *, length: int = 120) -> str:
    if not text:
        return ""
    # Usuwamy HTML i skracamy
    clean = strip_tags(text)
    return Truncator(clean).chars(length)


# -----------------------------
# Inlines
# -----------------------------
class MessageRecipientInline(admin.TabularInline):
    model = MessageRecipient
    extra = 0
    autocomplete_fields = ("person",)
    fields = ("person", "kind")
    show_change_link = True


# -----------------------------
# Filtry
# -----------------------------
class HasThreadFilter(admin.SimpleListFilter):
    title = "przypisany wątek"
    parameter_name = "has_thread"

    def lookups(self, request, model_admin):
        return (("yes", "Tak"), ("no", "Nie"))

    def queryset(self, request, queryset):
        if self.value() == "yes":
            return queryset.filter(thread__isnull=False)
        if self.value() == "no":
            return queryset.filter(thread__isnull=True)
        return queryset


class FromDomainFilter(admin.SimpleListFilter):
    title = "domena nadawcy"
    parameter_name = "from_domain"

    def lookups(self, request, model_admin):
        qs = (
            Person.objects.exclude(domain="")
            .values("domain")
            .annotate(c=Count("id"))
            .order_by("-c")
            .values_list("domain", flat=True)[:20]
        )
        return [(d, d) for d in qs]

    def queryset(self, request, queryset):
        v = self.value()
        return queryset.filter(from_person__domain=v) if v else queryset


class ToDomainFilter(admin.SimpleListFilter):
    title = "domena dostarczenia"
    parameter_name = "to_domain"

    def lookups(self, request, model_admin):
        qs = (
            Person.objects.exclude(domain="")
            .values("domain")
            .annotate(c=Count("id"))
            .order_by("-c")
            .values_list("domain", flat=True)[:20]
        )
        return [(d, d) for d in qs]

    def queryset(self, request, queryset):
        v = self.value()
        return queryset.filter(delivered_to__domain=v) if v else queryset
    
# -----------------------------
# Person
# -----------------------------
@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ("email", "display_name", "domain", "sent_count", "received_count")
    search_fields = ("email", "display_name", "domain")
    list_filter = ("domain",)
    ordering = ("email",)
    readonly_fields = ()
    fieldsets = (
        (None, {"fields": ("email", "display_name", "domain")}),
    )

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        # Anotacje liczników
        return qs.annotate(
            _sent=Count("sent_messages", distinct=True),
            _received=Count("received_messages", distinct=True),
        )

    @admin.display(description="wysłane", ordering="_sent")
    def sent_count(self, obj: Person):
        return obj._sent

    @admin.display(description="otrzymane", ordering="_received")
    def received_count(self, obj: Person):
        return obj._received


# -----------------------------
# Thread
# -----------------------------
@admin.register(Thread)
class ThreadAdmin(admin.ModelAdmin):
    list_display = ("id", "subject_norm", "thread_key", "messages_count", "created_at")
    search_fields = ("subject_norm", "thread_key")
    date_hierarchy = "created_at"
    ordering = ("-created_at",)

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.annotate(_mc=Count("messages", distinct=True))

    @admin.display(description="liczba wiadomości", ordering="_mc")
    def messages_count(self, obj: Thread):
        return obj._mc


# -----------------------------
# EmailMessage
# -----------------------------
@admin.register(EmailMessage)
class EmailMessageAdmin(admin.ModelAdmin):
    inlines = [MessageRecipientInline]

    list_display = (
        "id",
        "external_id",
        "short_subject",
        "direction",
        "from_person",
        "delivered_to",
        "sent_at",
        "received_at",
        "has_thread",
        "recipients_count",
    )
    list_filter = (
        "direction",
        "user_processed",
        "useless",
        HasThreadFilter,
        FromDomainFilter,
        ToDomainFilter,
        ("sent_at", admin.DateFieldListFilter),
        ("received_at", admin.DateFieldListFilter),
    )
    date_hierarchy = "received_at"
    search_fields = (
        "subject",
        "external_message_id",
        "message_id_header",
        "in_reply_to_header",
        "references_header",
        "from_person__email",
        "from_person__display_name",
        "delivered_to__email",
        "delivered_to__display_name",
        "thread__thread_key",
        "thread_hint",
        "text_plain",
    )
    autocomplete_fields = ("from_person", "delivered_to", "thread")
    raw_id_fields = ()
    ordering = ("-received_at", "-sent_at")
    readonly_fields = (
        "external_id",
        "external_message_id",
        "mailbox_id",
        "message_id_header",
        "in_reply_to_header",
        "references_header",
        "preview_text_plain",
        "preview_text_html_parsed", 
        "preview_text_processed",
        "raw_payload_pretty",
    )

    fieldsets = (
        ("Identyfikatory", {
            "fields": (
                "external_id",
                "external_message_id",
                "mailbox_id",
                "direction",
            )
        }),
        ("Statusy Opracowania", {
            "fields": (
                "user_processed",
                "useless",
            )
        }),
        ("Powiązania", {
            "fields": (
                "from_person",
                "delivered_to",
                "thread",
                "thread_hint",
            )
        }),
        ("Czasy", {
            "fields": ("sent_at", "received_at"),
        }),
        ("Nagłówki", {
            "classes": ("collapse",),
            "fields": ("message_id_header", "in_reply_to_header", "references_header"),
        }),
        ("Treść", {
            "fields": ("subject", "preview_text_plain", 
                       "preview_text_html_parsed", "preview_text_processed"),
        }),
        ("Surowy JSON", {
            "classes": ("collapse",),
            "fields": ("raw_payload_pretty",),
        }),
        ("Pełna treść (edytowalne pola)", {
            "classes": ("collapse",),
            "fields": ("text_plain", "text_html"),
        }),
    )

    # ---- Wyświetlacze ----
    @admin.display(description="temat (skrót)")
    def short_subject(self, obj: EmailMessage):
        return Truncator(obj.subject or "").chars(60)

    @admin.display(description="wątek?")
    def has_thread(self, obj: EmailMessage):
        return bool(obj.thread)
    has_thread.boolean = True  # ikonka ✔/✖

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.annotate(_rc=Count("message_recipients", distinct=True))

    @admin.display(description="adresaci", ordering="_rc")
    def recipients_count(self, obj: EmailMessage):
        return obj._rc

    @admin.display(description="podgląd (plain)")
    def preview_text_plain(self, obj: EmailMessage):
        return _snippet(obj.text_plain, length=300)

    @admin.display(description="podgląd (html→tekst)")
    def preview_text_html_parsed(self, obj: EmailMessage):
        return _snippet(obj.text_html_parsed, length=300)

    @admin.display(description="Formatowany tekst")
    def preview_text_processed(self, obj: EmailMessage):
        return _snippet(obj.text_processed, length=300)

    @admin.display(description="surowy JSON")
    def raw_payload_pretty(self, obj: EmailMessage):
        if not obj.raw_payload:
            return ""
        # ładne formatowanie
        import json as _json
        return _json.dumps(obj.raw_payload, ensure_ascii=False, indent=2)

    # ---- Akcje ----
    actions = ("assign_threads_from_hint",)

    @admin.action(description="Przypisz wątki z podpowiedzi (thread_hint)")
    def assign_threads_from_hint(self, request, queryset):
        """
        Dla zaznaczonych wiadomości:
        - tworzy (jeśli trzeba) Thread(thread_key=thread_hint),
        - kopiuje subject_norm ze skrótu tematu,
        - przypisuje do EmailMessage.thread.
        """
        created = 0
        updated = 0
        for msg in queryset:
            hint = (msg.thread_hint or "").strip()
            if not hint:
                continue
            thread, was_created = Thread.objects.get_or_create(
                thread_key=hint,
                defaults={"subject_norm": (msg.subject or "").strip()},
            )
            if was_created:
                created += 1
            if msg.thread_id != thread.id:
                msg.thread = thread
                msg.save(update_fields=["thread"])
                updated += 1
        self.message_user(
            request,
            f"Utworzono wątków: {created}, zaktualizowano przypisań: {updated}",
            level="INFO",
        )


# -----------------------------
# MessageRecipient
# -----------------------------
@admin.register(MessageRecipient)
class MessageRecipientAdmin(admin.ModelAdmin):
    list_display = ("id", "message", "person", "kind")
    list_filter = ("kind",)
    search_fields = (
        "message__subject",
        "message__external_message_id",
        "person__email",
        "person__display_name",
    )
    autocomplete_fields = ("message", "person")
    ordering = ("-id",)



@admin.register(PartnerStat)
class PartnerStatAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "a_email", "b_email",
        "msg_count", "msg_processed_count",
        "last_message_at",
    )
    list_filter = ("last_message_at",)
    search_fields = ("a__email", "b__email")
    ordering = ("-msg_count",)

    def a_email(self, obj):
        return obj.a.email if obj.a else None
    a_email.short_description = "Person A"

    def b_email(self, obj):
        return obj.b.email if obj.b else None
    b_email.short_description = "Person B"