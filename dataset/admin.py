# dataset/admin.py
from django import forms
from django.contrib import admin
from django.utils.translation import gettext_lazy as _

from .models import (
    Dictionary,
    DictionaryKind,
    DictionaryValue,
    DatasetSample,
    Annotation,
    LabelFinal,
    ModelPrediction
)

# ===========================
#   FORMS
# ===========================

class LabelFinalAdminForm(forms.ModelForm):
    """
    Ogranicza 'value' do wartości pasujących do wybranego 'kind'
    (i opcjonalnie do wybranego zestawu słownikowego — przez DictionaryFilter).
    """
    class Meta:
        model = LabelFinal
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        dictionary_id = kwargs.pop("dictionary_id", None)
        super().__init__(*args, **kwargs)

        kind = None
        if "kind" in self.data:
            try:
                kind = DictionaryKind.objects.get(pk=self.data.get("kind"))
            except DictionaryKind.DoesNotExist:
                kind = None
        if not kind and self.instance and self.instance.pk:
            kind = self.instance.kind

        qs = DictionaryValue.objects.none()
        if kind:
            qs = DictionaryValue.objects.filter(kind=kind, is_active=True)
            if dictionary_id:
                qs = qs.filter(kind__dictionary_id=dictionary_id)

        self.fields["value"].queryset = qs
        self.fields["value"].help_text = _(
            "Wartości filtrowane automatycznie po wybranym rodzaju (i opcjonalnie po zestawie)."
        )


class AnnotationAdminForm(forms.ModelForm):
    """
    Adnotacja użytkownika: 'value' ograniczone do wybranego 'kind'
    (i opcjonalnie do wybranego zestawu słownikowego).
    """
    class Meta:
        model = Annotation
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        dictionary_id = kwargs.pop("dictionary_id", None)
        super().__init__(*args, **kwargs)

        kind = None
        if "kind" in self.data:
            try:
                kind = DictionaryKind.objects.get(pk=self.data.get("kind"))
            except DictionaryKind.DoesNotExist:
                kind = None
        if not kind and self.instance and self.instance.pk:
            kind = self.instance.kind

        qs = DictionaryValue.objects.none()
        if kind:
            qs = DictionaryValue.objects.filter(kind=kind, is_active=True)
            if dictionary_id:
                qs = qs.filter(kind__dictionary_id=dictionary_id)

        self.fields["value"].queryset = qs
        self.fields["value"].help_text = _(
            "Wartości filtrowane automatycznie po wybranym rodzaju (i opcjonalnie po zestawie)."
        )

# ===========================
#   INLINES
# ===========================

class DictionaryKindInline(admin.TabularInline):
    """Rodzaje etykiet podpięte do konkretnego zestawu (Dictionary)."""
    model = DictionaryKind
    extra = 0
    fields = ("code", "name", "description")
    show_change_link = True
    ordering = ("name",)


class DictionaryValueInline(admin.TabularInline):
    """Wartości podpinamy pod DictionaryKind (NIE pod Dictionary)."""
    model = DictionaryValue
    extra = 0
    fields = ("code", "name", "description", "sort_order", "is_active")
    ordering = ("sort_order", "name")
    show_change_link = True


class AnnotationInline(admin.TabularInline):
    """Adnotacje użytkowników dla próbki (DatasetSample)."""
    model = Annotation
    form = AnnotationAdminForm
    extra = 0
    fields = (
        "annotator", "kind", "value",
        "evidence_snippet", "comment",
        "is_gold", "created_at",
    )
    readonly_fields = ("created_at",)
    autocomplete_fields = ("kind", "value", "annotator")


class LabelFinalInline(admin.TabularInline):
    """Finalne etykiety (po jednej na sample/kind) – przy DatasetSample."""
    model = LabelFinal
    form = LabelFinalAdminForm
    extra = 0
    fields = ("kind", "value", "evidence_snippet", "comment", "created_at")
    readonly_fields = ("created_at",)
    autocomplete_fields = ("kind", "value")


class ModelPredictionInline(admin.TabularInline):
    """Zapisane predykcje modeli (cache) dla próbki."""
    model = ModelPrediction
    extra = 0
    fields = ("kind", "value", "proba", "model_name", "model_version", "evidence_snippet", "created_at", "dictionary")
    readonly_fields = ("created_at",)
    autocomplete_fields = ("kind", "value", "dictionary")
    ordering = ("-created_at",)

# ===========================
#   LIST FILTERS
# ===========================

class DictionaryFilter(admin.SimpleListFilter):
    title = _("zestaw etykiet")
    parameter_name = "dictionary_id"

    def lookups(self, request, model_admin):
        qs = Dictionary.objects.order_by("-is_active", "code", "version", "locale").values(
            "id", "code", "version", "locale", "name"
        )
        return [(r["id"], f'{r["name"]} ({r["code"]} {r["version"]}/{r["locale"]})') for r in qs]

    def queryset(self, request, queryset):
        val = self.value()
        if not val:
            return queryset

        model = queryset.model
        if model is DictionaryValue:
            return queryset.filter(kind__dictionary_id=val)
        if model is LabelFinal:
            return queryset.filter(kind__dictionary_id=val)
        if model is Annotation:
            return queryset.filter(kind__dictionary_id=val)
        if model is DictionaryKind:
            return queryset.filter(dictionary_id=val)
        if model is Dictionary:
            return queryset.filter(id=val)
        if model is ModelPrediction:
            # predykcja nie ma własnego 'dictionary' obowiązkowo, ale filtr po kind.dictionary ma sens
            return queryset.filter(kind__dictionary_id=val)
        return queryset


class DictionaryKindCodeFilter(admin.SimpleListFilter):
    title = _("rodzaj (kod)")
    parameter_name = "kind_code"

    def lookups(self, request, model_admin):
        return [(k["code"], k["code"]) for k in DictionaryKind.objects.values("code").order_by("code")]

    def queryset(self, request, queryset):
        code = self.value()
        if not code:
            return queryset

        model = queryset.model
        if model is DictionaryValue:
            return queryset.filter(kind__code=code)
        if model is LabelFinal:
            return queryset.filter(kind__code=code)
        if model is Annotation:
            return queryset.filter(kind__code=code)
        if model is DictionaryKind:
            return queryset.filter(code=code)
        if model is ModelPrediction:
            return queryset.filter(kind__code=code)
        return queryset


class SampleSourceFilter(admin.SimpleListFilter):
    title = _("źródło próbki")
    parameter_name = "source"

    def lookups(self, request, model_admin):
        sources = DatasetSample.objects.values_list("source", flat=True).distinct().order_by("source")
        return [(s, s) for s in sources if s]

    def queryset(self, request, queryset):
        val = self.value()
        if not val:
            return queryset
        if queryset.model is DatasetSample:
            return queryset.filter(source=val)
        # inne modele przez FK do sample
        if hasattr(queryset.model, "sample"):
            return queryset.filter(sample__source=val)
        return queryset

# ===========================
#   UTIL
# ===========================

def short(text: str, n: int = 80) -> str:
    if not text:
        return ""
    text = text.strip().split("\n", 1)[0]
    return (text[: n - 1] + "…") if len(text) > n else text

# ===========================
#   ADMINS
# ===========================

@admin.register(Dictionary)
class DictionaryAdmin(admin.ModelAdmin):
    list_display = ("name", "code", "version", "locale", "is_active", "kinds_count", "values_count")
    list_filter = ("is_active", "locale")
    search_fields = ("name", "code", "description", "version", "locale")
    ordering = ("-is_active", "code", "version", "locale")
    inlines = [DictionaryKindInline]

    @admin.display(description=_("liczba rodzajów"))
    def kinds_count(self, obj: Dictionary) -> int:
        return DictionaryKind.objects.filter(dictionary=obj).count()

    @admin.display(description=_("liczba wartości"))
    def values_count(self, obj: Dictionary) -> int:
        return DictionaryValue.objects.filter(kind__dictionary=obj).count()


@admin.register(DictionaryKind)
class DictionaryKindAdmin(admin.ModelAdmin):
    list_display = ("name", "code", "dictionary", "description", "values_count")
    list_filter = (DictionaryFilter,)
    search_fields = ("name", "code", "description", "dictionary__name", "dictionary__code")
    ordering = ("dictionary__code", "name")
    inlines = [DictionaryValueInline]
    autocomplete_fields = ("dictionary",)

    @admin.display(description=_("liczba wartości"))
    def values_count(self, obj: DictionaryKind) -> int:
        return obj.values.count()


@admin.register(DictionaryValue)
class DictionaryValueAdmin(admin.ModelAdmin):
    list_display = ("name", "code", "kind", "dictionary_display", "sort_order", "is_active")
    list_filter = (DictionaryFilter, DictionaryKindCodeFilter, "is_active")
    search_fields = (
        "name", "code", "description",
        "kind__name", "kind__code",
        "kind__dictionary__name", "kind__dictionary__code",
    )
    ordering = ("kind__dictionary__code", "kind__dictionary__version", "kind__name", "sort_order", "name")
    autocomplete_fields = ("kind",)

    @admin.display(description=_("zestaw"))
    def dictionary_display(self, obj: DictionaryValue):
        d = obj.kind.dictionary
        return f"{d.name} ({d.code} {d.version}/{d.locale})" if d else "—"


@admin.register(DatasetSample)
class DatasetSampleAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "source",
        "lang",
        "preprocess_version",
        "content_short",
        "content_hash",
        "created_at",
        "labels_final_count",
        "annotations_count",
        "predictions_count",
    )
    list_filter = (SampleSourceFilter, "preprocess_version", "lang")
    search_fields = ("content", "content_hash")
    readonly_fields = ("content_hash", "created_at")
    ordering = ("-created_at", "-id")
    inlines = [LabelFinalInline, AnnotationInline, ModelPredictionInline]

    @admin.display(description=_("treść"))
    def content_short(self, obj: DatasetSample):
        return short(obj.content, 120)

    @admin.display(description=_("finalne etykiety"))
    def labels_final_count(self, obj: DatasetSample):
        return obj.labels_final.count()

    @admin.display(description=_("adnotacje"))
    def annotations_count(self, obj: DatasetSample):
        return obj.annotations.count()

    @admin.display(description=_("predykcje"))
    def predictions_count(self, obj: DatasetSample):
        return obj.predictions.count()


@admin.register(LabelFinal)
class LabelFinalAdmin(admin.ModelAdmin):
    form = LabelFinalAdminForm

    list_display = (
        "sample",
        "kind",
        "value",
        "dictionary_display",
        "evidence_short",
        "comment_short",
        "created_at",
    )
    list_filter = (
        DictionaryFilter,           # po dictionary
        DictionaryKindCodeFilter,   # po kind.code
        SampleSourceFilter,         # po sample.source
    )
    search_fields = (
        "evidence_snippet",
        "comment",
        "value__name",
        "value__code",
        "kind__name",
        "kind__code",
        "sample__content",
        "sample__content_hash",
    )
    date_hierarchy = "created_at"
    ordering = ("-created_at", "-id")
    autocomplete_fields = ("sample", "kind", "value",)
    readonly_fields = ("created_at",)

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.select_related("kind", "value", "kind__dictionary", "sample")

    @admin.display(description=_("zestaw"))
    def dictionary_display(self, obj: LabelFinal):
        d = obj.kind.dictionary
        return f"{d.name} ({d.code} {d.version}/{d.locale})" if d else "—"

    @admin.display(description=_("fragment"))
    def evidence_short(self, obj: LabelFinal):
        return short(obj.evidence_snippet, 100)

    @admin.display(description=_("komentarz"))
    def comment_short(self, obj: LabelFinal):
        return short(obj.comment, 80)


@admin.register(Annotation)
class AnnotationAdmin(admin.ModelAdmin):
    form = AnnotationAdminForm

    list_display = (
        "sample",
        "annotator",
        "kind",
        "value",
        "dictionary_display",
        "evidence_short",
        "comment_short",
        "is_gold",
        "created_at",
    )
    list_filter = (
        DictionaryFilter,
        DictionaryKindCodeFilter,
        SampleSourceFilter,
        "is_gold",
    )
    search_fields = (
        "evidence_snippet",
        "comment",
        "value__name",
        "value__code",
        "kind__name",
        "kind__code",
        "annotator__username",
        "sample__content",
        "sample__content_hash",
    )
    date_hierarchy = "created_at"
    ordering = ("-created_at", "-id")
    autocomplete_fields = ("sample", "kind", "value", "annotator",)
    readonly_fields = ("created_at",)

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.select_related("kind", "value", "kind__dictionary", "sample", "annotator")

    @admin.display(description=_("zestaw"))
    def dictionary_display(self, obj: Annotation):
        d = obj.kind.dictionary
        return f"{d.name} ({d.code} {d.version}/{d.locale})" if d else "—"

    @admin.display(description=_("fragment"))
    def evidence_short(self, obj: Annotation):
        return short(obj.evidence_snippet, 100)

    @admin.display(description=_("komentarz"))
    def comment_short(self, obj: Annotation):
        return short(obj.comment, 80)


@admin.register(ModelPrediction)
class ModelPredictionAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "sample",
        "kind",
        "value",
        "proba",
        "model_name",
        "model_version",
        "dictionary",
        "snippet_short",
        "created_at",
    )
    list_filter = (
        "model_name",
        "model_version",
        DictionaryKindCodeFilter,
        DictionaryFilter,
        SampleSourceFilter,
    )
    search_fields = (
        "sample__content",
        "sample__content_hash",
        "value__code",
        "value__name",
        "kind__code",
        "kind__name",
    )
    autocomplete_fields = ("sample", "kind", "value", "dictionary")
    readonly_fields = ("created_at",)
    ordering = ("-created_at", "-id")

    @admin.display(description=_("fragment"))
    def snippet_short(self, obj: ModelPrediction):
        return short(obj.evidence_snippet, 100)
