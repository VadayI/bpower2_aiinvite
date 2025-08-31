# api/serializers.py
from rest_framework import serializers

from ingestion.models import Person, EmailMessage, Thread
from dataset.models import Dictionary, DictionaryKind, DictionaryValue


# ---- Osoby / wątki / partnerzy ----

class PersonSerializer(serializers.ModelSerializer):
    class Meta:
        model = Person
        fields = ("id", "email", "display_name", "domain")


class PartnerWithCountSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    email = serializers.EmailField()
    display_name = serializers.CharField(allow_blank=True)
    domain = serializers.CharField(allow_blank=True)
    msg_count = serializers.IntegerField()
    msg_processed_count = serializers.IntegerField()


class ThreadSerializer(serializers.ModelSerializer):
    message_count = serializers.IntegerField()
    last_activity_at = serializers.DateTimeField()

    class Meta:
        model = Thread
        fields = ("id", "thread_key", "subject_norm", "message_count", "last_activity_at")


# ---- Słowniki (zestaw / rodzaje / wartości) ----

class DictionarySerializer(serializers.ModelSerializer):
    class Meta:
        model = Dictionary
        fields = ("id", "code", "name", "description", "version", "locale", "is_active", "created_at", "updated_at")


class DictionaryKindSerializer(serializers.ModelSerializer):
    dictionary = serializers.SerializerMethodField()

    class Meta:
        model = DictionaryKind
        fields = ("id", "code", "name", "description", "dictionary")

    def get_dictionary(self, obj: DictionaryKind):
        d = obj.dictionary
        return {
            "id": d.id,
            "code": d.code,
            "name": d.name,
            "version": d.version,
            "locale": d.locale,
            "is_active": d.is_active,
        }


class DictionaryValueSerializer(serializers.ModelSerializer):
    kind = serializers.SerializerMethodField()
    dictionary = serializers.SerializerMethodField()

    class Meta:
        model = DictionaryValue
        fields = ("id", "code", "name", "description", "sort_order", "is_active", "kind", "dictionary")

    def get_kind(self, obj: DictionaryValue):
        k = obj.kind
        return {"id": k.id, "code": k.code, "name": k.name}

    def get_dictionary(self, obj: DictionaryValue):
        d = obj.kind.dictionary
        return {"id": d.id, "code": d.code, "version": d.version, "locale": d.locale}


# ---- OpenAI: preview (dynamiczne) ----

class LabelPreviewInputSerializer(serializers.Serializer):
    message_id = serializers.IntegerField(required=True)
    # opcjonalne override'y (jeśli chcesz ich używać po stronie klienta LLM):
    model = serializers.CharField(required=False, allow_blank=True)
    temperature = serializers.FloatField(required=False)


class LabelPreviewLabelSerializer(serializers.Serializer):
    kind_id = serializers.IntegerField()
    kind_code = serializers.CharField()
    value_id = serializers.IntegerField()
    value_code = serializers.CharField(allow_blank=True, required=False)
    comment = serializers.CharField(allow_blank=True, required=False)


class LabelPreviewOutputSerializer(serializers.Serializer):
    labels = LabelPreviewLabelSerializer(many=True)
    global_rationale = serializers.CharField(allow_blank=True, required=False)


# ---- Wiadomości e-mail ----

class EmailMessageSerializer(serializers.ModelSerializer):
    from_person = PersonSerializer(read_only=True)
    delivered_to = PersonSerializer(read_only=True)
    recipients = PersonSerializer(read_only=True, many=True)

    class Meta:
        model = EmailMessage
        fields = (
            "id",
            "subject",
            "direction",
            "sent_at",
            "received_at",
            "from_person",
            "delivered_to",
            "recipients",
            "thread_id",
            "text_processed",
            "useless",
            "text_html_parsed",
            "text_plain"
        )
