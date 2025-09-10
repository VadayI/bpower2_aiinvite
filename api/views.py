import logging

from django.conf import settings
from django.db.models import (
    Sum, F, Q, Value, IntegerField, OuterRef, Subquery, Max, Case, When, Count
)
from django.db import transaction
from django.db.models.functions import Coalesce

from rest_framework import viewsets, mixins, status, permissions
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.pagination import PageNumberPagination
from rest_framework.authtoken.views import ObtainAuthToken
from rest_framework.authtoken.models import Token
from rest_framework.views import APIView

from ingestion.models import EmailMessage, PartnerStat, Person, MessageRecipient, Thread

# Używamy *dynamicznych* helperów z chatgpt_client:
from dataset.chatgpt_client import (
    label_email_with_openai,
    to_label_rows,                 # zamienia {"labels":[{kind,value,...}]} -> [{"kind_id","value_id","comment"}...]
)

from dataset.models import Dictionary, DictionaryKind, DictionaryValue, DatasetSample, \
    Annotation, LabelFinal, DictionaryKind, DictionaryValue, ModelPrediction
from .serializers import PersonSerializer, EmailMessageSerializer, ThreadSerializer, \
    ThreadSerializer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OPENAI_MODEL_NAME = settings.OPENAI_MODEL_NAME
OPENAI_MODEL_VERSION = settings.OPENAI_MODEL_VERSION
DEFAULT_PREPROCESS_VERSION = settings.DEFAULT_PREPROCESS_VERSION
OPENAI_API_KEY = settings.OPENAI_API_KEY


# --------------- Auth -------------------------------------------------------

class TokenAuthView(ObtainAuthToken):
    """
    POST {username, password} -> {key}
    Zwraca istniejący token lub tworzy nowy.
    """
    def post(self, request, *args, **kwargs):
        serializer = self.serializer_class(data=request.data, context={"request": request})
        serializer.is_valid(raise_exception=True)
        user = serializer.validated_data["user"]
        token, _ = Token.objects.get_or_create(user=user)
        return Response({"key": token.key}, status=status.HTTP_200_OK)


# ------------------- Production ---------------------------------------------

class SmallPage(PageNumberPagination):
    page_size = 25
    page_size_query_param = "page_size"
    max_page_size = 200

class DynamicMaxPage(PageNumberPagination):
    page_size = 25
    page_size_query_param = "page_size"
    max_page_size = 200  # fallback

    def get_paginated_response(self, data):
        response = super().get_paginated_response(data)
        # dołączamy informacyjnie aktualny max_page_size (dynamiczny lub fallback)
        current_max = getattr(self, "dynamic_max_page_size", self.max_page_size)
        response.data["max_page_size"] = current_max
        return response

class PersonViewSet(viewsets.ReadOnlyModelViewSet):
    serializer_class = PersonSerializer
    pagination_class = DynamicMaxPage

    def get_queryset(self):
        # --- Subquery: sumy z PartnerStat, gdy osoba jest po stronie 'a' lub 'b' ---
        sum_as_a = PartnerStat.objects.filter(a=OuterRef("pk")) \
            .values("a") \
            .annotate(s=Sum("msg_count")) \
            .values("s")[:1]

        sum_as_b = PartnerStat.objects.filter(b=OuterRef("pk")) \
            .values("b") \
            .annotate(s=Sum("msg_count")) \
            .values("s")[:1]

        sum_proc_as_a = PartnerStat.objects.filter(a=OuterRef("pk")) \
            .values("a") \
            .annotate(s=Sum("msg_processed_count")) \
            .values("s")[:1]

        sum_proc_as_b = PartnerStat.objects.filter(b=OuterRef("pk")) \
            .values("b") \
            .annotate(s=Sum("msg_processed_count")) \
            .values("s")[:1]

        qs = (
            Person.objects
            .annotate(
                total_from_a=Coalesce(Subquery(sum_as_a, output_field=IntegerField()), Value(0)),
                total_from_b=Coalesce(Subquery(sum_as_b, output_field=IntegerField()), Value(0)),
                total_proc_from_a=Coalesce(Subquery(sum_proc_as_a, output_field=IntegerField()), Value(0)),
                total_proc_from_b=Coalesce(Subquery(sum_proc_as_b, output_field=IntegerField()), Value(0)),
            )
            .annotate(
                total_messages=F("total_from_a") + F("total_from_b"),
                total_processed=F("total_proc_from_a") + F("total_proc_from_b"),
            )
            .order_by("-total_messages", "email")
        )
        return qs

    def list(self, request, *args, **kwargs):
        qs = self.get_queryset()
        aggregate = qs.aggregate(s=Sum("total_messages"))
        max_total = aggregate["s"] or 0

        if self.paginator:
            self.paginator.max_page_size = max_total or self.paginator.max_page_size
            setattr(self.paginator, "dynamic_max_page_size", self.paginator.max_page_size)

        return super().list(request, *args, **kwargs)

    @action(detail=True, methods=["get"])
    def partners(self, request, pk=None):
        person = self.get_object()

        stats = (
            PartnerStat.objects
            .exclude(a=F("b"))  # para musi być nieuporządkowana, ale nie a==b
            .filter(Q(a=person) | Q(b=person))
            .annotate(
                partner_id=Case(
                    When(a_id=person.id, then=F("b_id")),
                    default=F("a_id"),
                    output_field=IntegerField(),
                )
            )
            .order_by("-msg_count", "-last_message_at")
        )

        partner_ids = list(stats.values_list("partner_id", flat=True))
        partners = {p.id: p for p in Person.objects.filter(id__in=partner_ids)}

        data = []
        for s in stats:
            p = partners.get(s.partner_id)
            if not p:
                continue
            data.append({
                "id": p.id,
                "email": p.email,
                "display_name": p.display_name,
                "domain": p.domain,
                "msg_count": s.msg_count,
                "msg_processed_count": s.msg_processed_count,
                "last_message_at": s.last_message_at,
            })
        return Response(data)

    @action(detail=True, methods=["get"])
    def conversations(self, request, pk=None):
        """
        Zwraca LISTĘ wątków między 'person' i 'with' (partner_id),
        posortowaną po ostatniej aktywności. Jeżeli brak 'with', zwracamy wszystkie wiadomości w parach (bez grupowania).
        """
        person = self.get_object()
        partner_id = request.query_params.get("with")
        if not partner_id:
            return Response({"detail": "Missing 'with' (partner id)."}, status=400)

        partner = Person.objects.get(pk=partner_id)

        from django.db.models import Exists, OuterRef  # upewnij się, że masz import
        def involves(p):
            return Q(from_person=p) | Q(delivered_to=p) | Exists(
                MessageRecipient.objects.filter(message=OuterRef("pk"), person=p)
            )

        msgs = EmailMessage.objects.filter(involves(person) & involves(partner), thread__isnull=False)

        threads = (
            Thread.objects.filter(messages__in=msgs)
            .annotate(
                message_count=Count("messages", filter=Q(messages__in=msgs), distinct=True),
                last_activity_at=Max("messages__received_at")
            )
            .order_by("-last_activity_at","-id")
        )
        return Response(ThreadSerializer(threads, many=True).data)
    

def _get_bool(request, name: str, default: bool) -> bool:
    val = request.query_params.get(name, None)
    if val is None:
        return default
    return str(val).lower() in {"1", "true", "yes", "y", "t"}

def _parse_kinds(param: str | None):
    # Domyślnie tylko TO – zgodnie z sync_partner_stats.py
    kinds_csv = (param or "TO").upper()
    allowed = {
        "TO": MessageRecipient.Kind.TO,
        "CC": MessageRecipient.Kind.CC,
        "BCC": MessageRecipient.Kind.BCC,
    }
    return [allowed[k.strip()] for k in kinds_csv.split(",") if k.strip()]

class MessageViewSet(viewsets.ReadOnlyModelViewSet):
    """
    GET /api/messages?person=<id>&with=<id>
        [&thread=<id>]
        [&only_useless=true|false]         (domyślnie false)
        [&with_useless=true|false]         (domyślnie true)
        [&only_user_processed=true|false]  (domyślnie false)
        [&with_user_processed=true|false]  (domyślnie true)
        [&kinds=TO|TO,CC,BCC]              (domyślnie TO – tak jak w sync_partner_stats.py)
    """
    serializer_class = EmailMessageSerializer
    pagination_class = SmallPage
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        params = self.request.query_params
        person_id = params.get("person")
        with_id = params.get("with")
        thread_id = params.get("thread")

        only_useless = _get_bool(self.request, "only_useless", False)
        with_useless = _get_bool(self.request, "with_useless", True)
        only_user_processed = _get_bool(self.request, "only_user_processed", False)
        with_user_processed = _get_bool(self.request, "with_user_processed", True)

        try:
            kind_values = _parse_kinds(params.get("kinds"))
        except KeyError:
            return EmailMessage.objects.none()

        qs = EmailMessage.objects.exclude(Q(from_person=F("delivered_to")) | Q(text_processed=False))

        # useless
        if only_useless:
            qs = qs.filter(useless=True)
        elif not with_useless:
            qs = qs.exclude(useless=True)

        # user_processed
        if only_user_processed:
            qs = qs.filter(user_processed=True)
        elif not with_user_processed:
            qs = qs.exclude(user_processed=True)

        if person_id and with_id:
            try:
                a_id = int(person_id)
                b_id = int(with_id)
            except (TypeError, ValueError):
                return EmailMessage.objects.none()

            # Sprawdzenie w PartnerStat (a<b)
            x, y = (a_id, b_id) if a_id < b_id else (b_id, a_id)
            if not PartnerStat.objects.filter(a_id=x, b_id=y).exists():
                return EmailMessage.objects.none()

            # Dokładnie ta sama reguła, co w sync_partner_stats.py:
            # from_person ↔ (delivered_to ∪ recipients[kinds]) – w obu kierunkach
            pair_q = (
                Q(from_person_id=a_id) & (
                    Q(delivered_to_id=b_id) |
                    Q(message_recipients__person_id=b_id,
                      message_recipients__kind__in=kind_values)
                )
            ) | (
                Q(from_person_id=b_id) & (
                    Q(delivered_to_id=a_id) |
                    Q(message_recipients__person_id=a_id,
                      message_recipients__kind__in=kind_values)
                )
            )
            qs = qs.filter(pair_q).distinct()

        if thread_id:
            qs = qs.filter(thread_id=thread_id)

        return qs.select_related("from_person", "delivered_to", "thread") \
                 .order_by("-received_at", "-sent_at", "-id")

    # ------- AKCJE ZMIENIAJĄCE POLE 'useless' -------

    @action(detail=True, methods=["post"], url_path="mark-useless")
    def mark_useless(self, request, pk=None):
        """
        POST /api/messages/{id}/mark-useless/
        """
        msg = self.get_object()
        if not msg.useless:
            msg.useless = True
            msg.save(update_fields=["useless"])
        return Response({"id": msg.id, "useless": True}, status=status.HTTP_200_OK)

    # ------- AKCJE ZMIENIAJĄCE POLE 'user_processed' -------

    @action(detail=True, methods=["post"], url_path="mark-processed")
    def mark_user_processed(self, request, pk=None):
        """
        POST /api/messages/{id}/mark-processed/
        """
        msg = self.get_object()
        if not msg.user_processed:
            msg.user_processed = True
            msg.save(update_fields=["user_processed"])
        return Response({"id": msg.id, "user_processed": True}, status=status.HTTP_200_OK)


class ThreadViewSet(viewsets.ReadOnlyModelViewSet):
    """
    GET /api/threads?person=<id>&with=<id>
        [&only_useless=true|false]         (domyślnie false)
        [&with_useless=true|false]         (domyślnie true)
        [&only_user_processed=true|false]  (domyślnie false)
        [&with_user_processed=true|false]  (domyślnie true)
        [&kinds=TO|TO,CC,BCC]              (domyślnie TO – tak jak w MessageViewSet)
        [&since=YYYY-MM-DD]                (opcjonalnie ograniczenie czasu na bazie sent_at/received_at)

    Zwraca wątki, które mają co najmniej jedną wiadomość spełniającą regułę:
      from_person ↔ (delivered_to ∪ recipients[kinds]) – w obu kierunkach, między person i with.
    """
    serializer_class = ThreadSerializer
    pagination_class = SmallPage
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        params = self.request.query_params
        person_id = params.get("person")
        with_id = params.get("with")
        since = params.get("since")

        if not person_id or not with_id:
            # wymagamy obu identyfikatorów, bo to 'wątki między wskazanymi osobami'
            return Thread.objects.none()
        try:
            a_id = int(person_id)
            b_id = int(with_id)
        except (TypeError, ValueError):
            return Thread.objects.none()

        # kinds (TO/CC/BCC)
        try:
            kind_values = _parse_kinds(params.get("kinds"))
        except KeyError:
            return Thread.objects.none()

        only_useless = _get_bool(self.request, "only_useless", False)
        with_useless = _get_bool(self.request, "with_useless", True)
        only_user_processed = _get_bool(self.request, "only_user_processed", False)
        with_user_processed = _get_bool(self.request, "with_user_processed", True)

        # Mikro-optymalizacja: sprawdź w PartnerStat, czy para w ogóle istnieje
        x, y = (a_id, b_id) if a_id < b_id else (b_id, a_id)
        if not PartnerStat.objects.filter(a_id=x, b_id=y).exists():
            return Thread.objects.none()

        # Bazowe QS po wiadomościach – *tylko* te, które pasują do filtrów "useless/user_processed/since"
        msgs = EmailMessage.objects.all()

        # useless
        if only_useless:
            msgs = msgs.filter(useless=True)
        elif not with_useless:
            msgs = msgs.exclude(useless=True)

        # user_processed
        if only_user_processed:
            msgs = msgs.filter(user_processed=True)
        elif not with_user_processed:
            msgs = msgs.exclude(user_processed=True)

        # since (po sent_at/received_at – którakolwiek z dat musi spełniać warunek)
        if since:
            msgs = msgs.filter(Q(sent_at__date__gte=since) | Q(received_at__date__gte=since))

        # Reguła pary, identyczna jak w MessageViewSet:
        pair_q = (
            Q(from_person_id=a_id) & (
                Q(delivered_to_id=b_id) |
                Q(message_recipients__person_id=b_id,
                  message_recipients__kind__in=kind_values)
            )
        ) | (
            Q(from_person_id=b_id) & (
                Q(delivered_to_id=a_id) |
                Q(message_recipients__person_id=a_id,
                  message_recipients__kind__in=kind_values)
            )
        )
        msgs = msgs.filter(pair_q)

        # Z tego subzapytania budujemy QS wątków:
        #  - tylko wątki, które mają co najmniej jedną taką wiadomość,
        #  - adnotujemy liczbę takich wiadomości i "ostatnią aktywność" (max z sent_at/received_at).
        threads = (
            Thread.objects
            .filter(messages__in=msgs)
            .annotate(
                matched_messages=Count("messages", filter=Q(messages__in=msgs), distinct=True),
                last_activity=Max(Coalesce("messages__received_at", "messages__sent_at")),
            )
            .order_by("-last_activity", "-id")
            .distinct()
        )

        # Dodatkowo możesz chcieć prefetch/only, ale to zależy od Twojego ThreadSerializer
        return threads

# ------------------- SŁOWNIKI (dynamicznie) ---------------------------------

class DictionaryViewSet(viewsets.ReadOnlyModelViewSet):
    """
    GET /api/dictionaries/?active=true|false
    """
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = None

    def get_queryset(self):
        qs = Dictionary.objects.all().order_by("-is_active", "code", "version", "locale")
        active = self.request.query_params.get("active")
        if active is not None:
            flag = str(active).lower() in {"1","true","yes","y","t"}
            qs = qs.filter(is_active=flag)
        return qs

    def list(self, request, *args, **kwargs):
        data = [{
            "id": d.id,
            "code": d.code,
            "name": d.name,
            "description": d.description,
            "version": d.version,
            "locale": d.locale,
            "is_active": d.is_active,
        } for d in self.get_queryset()]
        return Response(data)


class DictionaryKindViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Rodzaje etykiet dla zadanego zestawu lub globalnie.
    GET /api/dict-kinds/?dictionary=<id>  (opcjonalnie)
    """
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = None

    def get_queryset(self):
        qs = DictionaryKind.objects.all().order_by("name")
        dictionary_id = self.request.query_params.get("dictionary")
        if dictionary_id:
            qs = qs.filter(dictionary_id=dictionary_id)
        return qs.select_related("dictionary")

    def list(self, request, *args, **kwargs):
        data = [{
            "id": k.id,
            "code": k.code,
            "name": k.name,
            "description": k.description,
            "dictionary": {
                "id": k.dictionary_id,
                "code": k.dictionary.code,
                "version": k.dictionary.version,
                "locale": k.dictionary.locale,
                "is_active": k.dictionary.is_active,
            },
        } for k in self.get_queryset()]
        return Response(data)


class DictionaryValueViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Wartości etykiet (filtrowalne po rodzaju i/lub zestawie).
    GET /api/dict-values/?kind=<code>&dictionary=<id>
    """
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = None

    def get_queryset(self):
        qs = DictionaryValue.objects.all().order_by("kind__name", "sort_order", "name")
        kind_code = self.request.query_params.get("kind")
        dictionary_id = self.request.query_params.get("dictionary")

        if kind_code:
            qs = qs.filter(kind__code=kind_code)
        if dictionary_id:
            qs = qs.filter(kind__dictionary_id=dictionary_id)

        return qs.select_related("kind", "kind__dictionary")

    def list(self, request, *args, **kwargs):
        data = [{
            "id": v.id,
            "code": v.code,
            "name": v.name,
            "description": v.description,
            "sort_order": v.sort_order,
            "is_active": v.is_active,
            "kind": {
                "id": v.kind_id,
                "code": v.kind.code,
                "name": v.kind.name,
            },
            "dictionary": {
                "id": v.kind.dictionary_id,
                "code": v.kind.dictionary.code,
                "version": v.kind.dictionary.version,
                "locale": v.kind.dictionary.locale,
            },
            "label_count": v.final_for_samples.count()
        } for v in self.get_queryset()]
        return Response(data)


# ---------------------- ETYKIETY (GENERYCZNE, dynamicznie) ------------------

class LabelViewSet(mixins.CreateModelMixin,
                   mixins.RetrieveModelMixin,
                   mixins.UpdateModelMixin,
                   viewsets.GenericViewSet):
    """
    Upsert etykiety użytkownika bez duplikowania treści.

    POST /api/labels/upsert
    Body (pojedynczy obiekt lub lista obiektów):
    {
      "content": "pełny tekst maila (oczyszczony)",
      "kind":  "ID rodzaju (DictionaryKind.pk)",
      "value": "ID wartości (DictionaryValue.pk)",
      "snippet": "fragment uzasadniający etykietę",   # zapisujemy jako evidence_snippet
      "comment": "uwagi użytkownika",                 # zapisujemy w Annotation i Final
      "preprocess_version": "v1",                     # opcjonalnie; default 'v1'
      "lang": "pl"                                    # opcjonalnie
    }

    Zwraca listę wpisów z informacją o utworzonych/zmodyfikowanych rekordach:
    [
      {
        "sample_id": 123,
        "annotation_id": 456,
        "final_label_id": 789,
        "kind": {"id": 1, "code": "emotion", "name": "Emocja"},
        "value": {"id": 10, "code": "joy", "name": "Radość"},
        "evidence_snippet": "...",
        "comment": "..."
      },
      ...
    ]
    """
    permission_classes = [permissions.IsAuthenticated]
    queryset = LabelFinal.objects.all()  # nie jest używane przez @action, ale wymagane przez ViewSet

    @action(detail=False, methods=["post"])
    @transaction.atomic
    def upsert(self, request):
        payload = request.data
        # pozwalamy na pojedynczy obiekt lub listę
        items = payload if isinstance(payload, list) else [payload]

        if not items:
            return Response({"detail": "Brak danych."}, status=400)

        # 1) Walidacja referencji kind/value (drugi przebieg użyje już obiektów)
        kinds_cache = {}
        values_cache = {}
        for i, item in enumerate(items, start=1):
            content = (item.get("content") or "").strip()
            kind_id = item.get("kind")
            value_id = item.get("value")
            if not content or not kind_id or not value_id:
                return Response(
                    {"detail": f"[{i}] Wymagane pola: content, kind, value."},
                    status=400,
                )
            try:
                kind = kinds_cache[kind_id]
            except KeyError:
                try:
                    kind = DictionaryKind.objects.get(pk=kind_id)
                except DictionaryKind.DoesNotExist:
                    return Response({"detail": f"[{i}] Nieznany rodzaj: {kind_id!r}."}, status=404)
                kinds_cache[kind_id] = kind

            try:
                value = values_cache[(kind_id, value_id)]
            except KeyError:
                try:
                    value = DictionaryValue.objects.get(kind=kind, pk=value_id, is_active=True)
                except DictionaryValue.DoesNotExist:
                    return Response(
                        {"detail": f"[{i}] Nieznana wartość: {value_id!r} dla rodzaju {kind.code!r}."},
                        status=404,
                    )
                values_cache[(kind_id, value_id)] = value

        # 2) Przetwarzanie: Sample -> Annotation -> LabelFinal
        resp = []
        user = request.user
        for item in items:
            content = (item.get("content") or "").strip()
            preprocess_version = (item.get("preprocess_version") or "v1").strip()
            lang = (item.get("lang") or "").strip()

            snippet = (item.get("snippet") or "").strip()
            comment = (item.get("comment") or "").strip()

            kind = kinds_cache[item["kind"]]
            value = values_cache[(item["kind"], item["value"])]

            # a) deduplikacja treści – DatasetSample
            sample, _created = DatasetSample.objects.get_or_create_from_text(
                content, preprocess_version=preprocess_version, lang=lang or "pl"
            )

            # b) adnotacja użytkownika (z evidence_snippet + comment)
            ann = Annotation.objects.create(
                sample=sample,
                kind=kind,
                value=value,
                annotator=user,
                confidence=1.0,
                evidence_snippet=snippet,  # <-- kluczowa zmiana
                comment=comment,
            )

            # c) finalna etykieta (1 per sample/kind) – upsert
            final_obj, _ = LabelFinal.objects.update_or_create(
                sample=sample,
                kind=kind,
                defaults={
                    "value": value,
                    "evidence_snippet": snippet,  # utrwalamy w finalu również
                    "comment": comment,
                }
            )

            resp.append({
                "sample_id": sample.id,
                "annotation_id": ann.id,
                "final_label_id": final_obj.id,
                "kind": {"id": kind.id, "code": kind.code, "name": kind.name},
                "value": {"id": value.id, "code": value.code, "name": value.name},
                "evidence_snippet": final_obj.evidence_snippet,
                "comment": final_obj.comment,
            })

        return Response(resp, status=200)
    
# ---------------------- Etykietowanie OpenAI (dynamicznie) ------------------

class LabelPreviewView(APIView):
    """
    POST /api/label/preview
    Body:
    {
      "message_id": 123,
      "kinds": ["emotion","style","relation"],   # opcjonalnie: lista kodów lub ID
      "dictionary_code": "aiinvite",             # opcjonalnie (domyślnie: aiinvite)
      "dictionary_version": "v1",                # opcjonalnie
      "dictionary_locale": "pl"                  # opcjonalnie
    }

    Zwraca:
    {
      "labels": [
        {"kind_code":"emotion","kind_id":7,"value_code":"joy","value_id":42,"snippet":"..."},
        ...
      ],
      "cached": true|false
    }
    """
    permission_classes = [permissions.IsAuthenticated]

    @transaction.atomic
    def post(self, request, *args, **kwargs):
        message_id = request.data.get("message_id")
        if not message_id:
            return Response({"detail": "Brak message_id."}, status=400)

        # --- 1) Wiadomość i tekst ---
        try:
            msg = (
                EmailMessage.objects
                .only("id","subject","direction","text_processed")
                .get(pk=message_id)
            )
        except EmailMessage.DoesNotExist:
            return Response({"detail": "EmailMessage not found."}, status=404)

        email_text = (msg.text_processed or "") or (msg.subject or "")
        if not email_text.strip():
            return Response({"detail": "Brak treści/tematu do klasyfikacji."}, status=400)

        # --- 2) Rodzaje (kinds) ---
        input_kinds = request.data.get("kinds") or []
        kinds_qs = self._resolve_kinds(input_kinds)
        if not kinds_qs.exists():
            return Response({"detail": "Brak zdefiniowanych rodzajów (kinds)."}, status=400)

        kinds_by_id = {k.id: k for k in kinds_qs}

        # --- 3) Zestaw słowników (code/version/locale) ---
        dictionary_code = request.data.get("dictionary_code") or "aiinvite"
        dictionary_version = request.data.get("dictionary_version") or "v1"
        dictionary_locale = request.data.get("dictionary_locale") or "pl"

        # --- 3a) Próbka datasetowa (NIE powiązana FK z ingestion) ---
        # deduplikacja po (content_hash, preprocess_version, source)
        sample, _created = DatasetSample.objects.get_or_create_from_text(
            email_text,
            preprocess_version=DEFAULT_PREPROCESS_VERSION,
            lang="pl",
            source="email",  # <--- kluczowe: dataset niezależny; tylko metadana 'source'
        )

        # --- 4) Sprawdź cache (ModelPrediction) ---
        preds_qs = (
            ModelPrediction.objects
            .filter(
                sample=sample,
                kind_id__in=list(kinds_by_id.keys()),
                model_name=OPENAI_MODEL_NAME,
                model_version=OPENAI_MODEL_VERSION,
            )
            .select_related("kind","value")
        )

        # Jeśli masz pole 'dictionary' w ModelPrediction: filtruj pod konkretny zestaw
        dictionary = self._find_dictionary(dictionary_code, dictionary_version, dictionary_locale)
        if hasattr(ModelPrediction, "dictionary_id") and dictionary is not None:
            preds_qs = preds_qs.filter(dictionary=dictionary)

        preds_by_kind_id = {p.kind_id: p for p in preds_qs}
        need_kind_ids = [kid for kid in kinds_by_id.keys() if kid not in preds_by_kind_id]
        cached_fully = len(need_kind_ids) == 0

        result_rows = []

        if cached_fully:
            for p in preds_qs:
                result_rows.append({
                    "kind_id": p.kind_id,
                    "kind_code": p.kind.code,
                    "value_id": p.value_id,
                    "value_code": p.value.code,
                    "snippet": p.evidence_snippet or "",
                })
            return Response({"labels": self._sort_rows(result_rows, kinds_qs), "cached": True}, status=200)

        # --- 5) Braki -> wołamy OpenAI (1 call) ---
        try:
            raw_args, enums = label_email_with_openai(
                email_text=email_text,
                model_openai=OPENAI_MODEL_VERSION,
                openai_api_key=OPENAI_API_KEY,
                subject=(msg.subject or None),
                direction=msg.direction,
                dictionary_code=dictionary_code,
                dictionary_version=dictionary_version,
                dictionary_locale=dictionary_locale,
            )
            rows = to_label_rows(raw_args, enums)  # [{"kind_id","value_id","snippet"}...]
            dictionary_id = enums.get("dictionary_id")  # może być None
        except ValueError as e:
            return Response({"detail": str(e)}, status=422)
        except Exception as e:
            logger.exception("OpenAI error")
            return Response({"detail": f"OpenAI error: {e}"}, status=502)

        # mapy pomocnicze
        values_map = self._build_values_map(kinds_qs, dictionary_id)

        saved_any = False

        for r in rows:
            r_kind_id = r.get("kind_id")
            r_value_id = r.get("value_id")
            snippet = (r.get("snippet") or "").strip()

            # jeżeli to_label_rows zwraca kind_id/value_id już lokalne – OK,
            # ale upewnijmy się, że to rodzaj z 'need_kind_ids'
            if r_kind_id not in need_kind_ids:
                continue

            # safety: jeżeli value_id brak – spróbuj zmapować po kodzie
            if not r_value_id and r.get("value_code"):
                local_map = values_map.get(r_kind_id, {})
                r_value_id = local_map.get(r["value_code"])
                if not r_value_id:
                    logger.warning("Brak mapy value_id dla kind_id=%s value_code=%r", r_kind_id, r.get("value_code"))
                    continue

            defaults = {
                "value_id": r_value_id,
                "proba": float(r.get("proba", 0.0)) if r.get("proba") is not None else 0.0,
                "evidence_snippet": snippet,  # <-- zapisujemy dowód
            }
            # jeżeli mamy pole 'dictionary' – dołóżmy je do upsertu
            if hasattr(ModelPrediction, "dictionary_id") and dictionary_id:
                defaults["dictionary_id"] = dictionary_id

            pred, created_pred = ModelPrediction.objects.update_or_create(
                sample=sample,
                kind_id=r_kind_id,
                model_name=OPENAI_MODEL_NAME,
                model_version=OPENAI_MODEL_VERSION,
                # jeżeli masz kolumnę 'dictionary', dorzuć też ją do klucza wyszukiwania:
                **({"defaults": defaults} if not hasattr(ModelPrediction, "dictionary_id") else {}),
                **({} if not hasattr(ModelPrediction, "dictionary_id") else {
                    "dictionary_id": dictionary_id,
                    "defaults": defaults
                }),
            )
            saved_any = saved_any or created_pred

            result_rows.append({
                "kind_id": pred.kind_id,
                "kind_code": kinds_by_id[pred.kind_id].code,
                "value_id": pred.value_id,
                "value_code": pred.value.code,
                "snippet": pred.evidence_snippet or "",
            })

        # dołącz zcache’owane (żeby zwrócić komplet)
        for kid, pred in preds_by_kind_id.items():
            result_rows.append({
                "kind_id": pred.kind_id,
                "kind_code": pred.kind.code,
                "value_id": pred.value_id,
                "value_code": pred.value.code,
                "snippet": pred.evidence_snippet or "",
            })

        return Response({"labels": self._sort_rows(result_rows, kinds_qs), "cached": not saved_any}, status=200)

    # -------------- helpers --------------

    def _resolve_kinds(self, input_kinds):
        qs = DictionaryKind.objects.all()
        if not input_kinds:
            return qs.filter(is_active=True)
        ids = [k for k in input_kinds if isinstance(k, int) or (isinstance(k, str) and k.isdigit())]
        codes = [k for k in input_kinds if isinstance(k, str) and not k.isdigit()]
        q = qs.none()
        if ids:
            q = q.union(qs.filter(pk__in=[int(i) for i in ids]))
        if codes:
            q = q.union(qs.filter(code__in=codes))
        return q.distinct()

    def _build_values_map(self, kinds_qs, dictionary_id):
        qs = DictionaryValue.objects.filter(kind__in=kinds_qs, is_active=True)
        if dictionary_id:
            qs = qs.filter(kind__dictionary_id=dictionary_id)
        out = {}
        for v in qs.only("id","code","kind_id"):
            out.setdefault(v.kind_id, {})[v.code] = v.id
        return out

    def _sort_rows(self, rows, kinds_qs):
        order = {k.id: idx for idx, k in enumerate(kinds_qs.order_by("name","code"), start=1)}
        return sorted(rows, key=lambda r: (order.get(r["kind_id"], 9999), r.get("value_code","")))

    def _find_dictionary(self, code, version, locale):
        if not code:
            return None
        qs = Dictionary.objects.filter(code=code)
        if version:
            qs = qs.filter(version=version)
        if locale:
            qs = qs.filter(locale=locale)
        return qs.order_by("-is_active").first()
