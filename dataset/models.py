# dataset/models.py
from __future__ import annotations

import hashlib
import re

from django.db import models
from django.utils import timezone



# ==========================
#   ZESTAW ETYKIET (WERSJA)
# ==========================

class Dictionary(models.Model):
    code = models.SlugField(max_length=64, verbose_name="kod zestawu")
    name = models.CharField(max_length=160, verbose_name="nazwa zestawu")
    description = models.TextField(blank=True, default="", verbose_name="opis zestawu")

    version = models.CharField(max_length=32, default="v1", verbose_name="wersja")
    locale = models.CharField(max_length=16, default="pl", verbose_name="język/locale")
    is_active = models.BooleanField(default=True, verbose_name="aktywny")

    created_at = models.DateTimeField(auto_now_add=True, verbose_name="utworzono")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="zaktualizowano")

    class Meta:
        verbose_name = "Zestaw etykiet"
        verbose_name_plural = "Zestawy etykiet"
        ordering = ["-is_active", "code", "version", "locale"]
        unique_together = [("code", "version", "locale")]
        indexes = [
            models.Index(fields=["code", "version", "locale"]),
            models.Index(fields=["is_active"]),
        ]

    def __str__(self) -> str:
        return f"{self.name} ({self.code} {self.version}/{self.locale})"


# ==========================
#   RODZAJE ETYKIET
# ==========================

class DictionaryKind(models.Model):
    dictionary = models.ForeignKey(
        Dictionary, on_delete=models.CASCADE, related_name="kinds",
        verbose_name="zestaw"
    )
    code = models.SlugField(max_length=64, unique=True, verbose_name="kod rodzaju")
    name = models.CharField(max_length=160, verbose_name="nazwa rodzaju")
    is_active = models.BooleanField(default=True, verbose_name="aktywna")
    description = models.TextField(blank=True, default="", verbose_name="opis rodzaju")

    class Meta:
        verbose_name = "Rodzaj etykiety"
        verbose_name_plural = "Rodzaje etykiet"
        ordering = ["name"]
        indexes = [models.Index(fields=["code"])]

    def __str__(self) -> str:
        return self.name


# ==========================
#   WARTOŚCI (w zestawie/rodzaju)
# ==========================

class DictionaryValue(models.Model):
    kind = models.ForeignKey(
        DictionaryKind, on_delete=models.CASCADE, related_name="values",
        verbose_name="rodzaj"
    )

    code = models.SlugField(max_length=64, verbose_name="kod wartości")
    name = models.CharField(max_length=160, verbose_name="nazwa wartości")
    description = models.TextField(blank=True, default="", verbose_name="opis wartości")

    sort_order = models.IntegerField(default=0, verbose_name="kolejność")
    is_active = models.BooleanField(default=True, verbose_name="aktywna")

    class Meta:
        verbose_name = "Wartość etykiety"
        verbose_name_plural = "Wartości etykiet"
        ordering = ["kind__name", "sort_order", "name"]
        indexes = [
            models.Index(fields=["kind", "sort_order"]),
        ]

    def __str__(self) -> str:
        return f"{self.name} [{self.kind.code} @ {self.kind.dictionary.code}/{self.kind.dictionary.version}]"


# ==========================
#   ETYKIETA (GENERYCZNA)
# ==========================

# ---------------- utils ----------------
def _normalize_content(text: str) -> str:
    """
    Normalizacja treści na potrzeby stabilnego skrótu:
    - normalizacja końców linii do LF,
    - redukcja nadmiarowych białych znaków,
    - trim.
    """
    if text is None:
        return ""
    
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t\f\v]+", " ", text)
    return text.strip()

def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# --------------- Dataset ---------------

class DatasetSampleQuerySet(models.QuerySet):
    pass

class DatasetSampleManager(models.Manager):
    def get_queryset(self):
        return DatasetSampleQuerySet(self.model, using=self._db)

    def get_or_create_from_text(
        self,
        text: str,
        *,
        preprocess_version: str = "v1",
        lang: str = "",
        source: str = "generic",
    ):
        """
        Utwórz/pobierz próbkę na podstawie treści.
        Deduplikacja po (content_hash, preprocess_version, source).
        """
        norm = _normalize_content(text or "")
        h = _sha256(norm)
        obj, created = self.get_or_create(
            content_hash=h,
            preprocess_version=preprocess_version,
            source=source,
            defaults={"content": norm, "lang": lang or ""},
        )
        return obj, created

class DatasetSample(models.Model):
    """
    Kanoniczna próbka tekstu.
    Treść może pochodzić z różnych źródeł (e-mail, czat, formularz, www, ...).
    Link do świata zewnętrznego wyłącznie przez skrót treści (sha256).
    """

    # UWAGA: BRAK powiązań z ingestion; przechowujemy wyłącznie treść i metadane datasetowe.

    content = models.TextField(
        verbose_name="treść (kanoniczna)",
        help_text="Znormalizowana treść próbki używana do trenowania i etykietowania."
    )
    content_hash = models.CharField(
        max_length=64, db_index=True, editable=False,
        verbose_name="skrót treści (sha256)",
        help_text="Skrót SHA-256 znormalizowanej treści; służy do deduplikacji i linkowania."
    )
    preprocess_version = models.CharField(
        max_length=16, default="v1", db_index=True,
        verbose_name="wersja preprocessingu",
        help_text="Wersja pipeline’u przygotowania tekstu (np. reguły czyszczenia)."
    )
    lang = models.CharField(
        max_length=8, blank=True, default="",
        verbose_name="język",
        help_text="Kod języka próbki (np. 'pl')."
    )
    source = models.CharField(
        max_length=64, default="generic", db_index=True, unique=False,
        verbose_name="źródło",
        help_text="Identyfikator źródła pochodzenia treści (np. 'email', 'chat', 'webform', 'www', 'generic')."
    )
    created_at = models.DateTimeField(
        default=timezone.now,
        verbose_name="utworzono",
        help_text="Znacznik czasu utworzenia próbki."
    )

    objects = DatasetSampleManager()

    class Meta:
        verbose_name = "Próbka danych"
        verbose_name_plural = "Próbki danych"
        # Deduplikacja: ta sama treść może wystąpić w różnych źródłach lub przy innej wersji preprocessingu.
        constraints = [
            models.UniqueConstraint(
                fields=["content_hash", "preprocess_version", "source"],
                name="uniq_sample_by_hash_preprocess_source"
            )
        ]
        indexes = [
            models.Index(fields=["source", "preprocess_version", "content_hash"]),
            models.Index(fields=["lang"]),
        ]
        ordering = ["-created_at", "-id"]

    def save(self, *args, **kwargs):
        norm = _normalize_content(self.content or "")
        self.content_hash = _sha256(norm)
        super().save(*args, **kwargs)

    def __str__(self):
        title = (self.content or "").split("\n", 1)[0]
        if len(title) > 60:
            title = title[:57] + "..."
        return f"#{self.pk} [{self.source}/{self.lang or 'unk'}/{self.preprocess_version}] {title}"


# --------------- Labeling ---------------

class LabelFinal(models.Model):
    """
    Finalna (zaakceptowana) etykieta *per rodzaj* dla danej próbki.
    Jedna finalna etykieta na (sample, kind).
    """
    sample = models.ForeignKey(
        DatasetSample, on_delete=models.CASCADE, related_name="labels_final",
        verbose_name="próbka", help_text="Próbka danych, której dotyczy etykieta."
    )
    kind = models.ForeignKey(
        "dataset.DictionaryKind", on_delete=models.PROTECT, related_name="+",
        verbose_name="rodzaj", help_text="Rodzaj etykiety (np. 'emotion', 'style')."
    )
    value = models.ForeignKey(
        "dataset.DictionaryValue", on_delete=models.PROTECT, related_name="final_for_samples",
        verbose_name="wartość", help_text="Wybrana wartość słownikowa dla tego rodzaju."
    )
    evidence_snippet = models.TextField(
        blank=True, default="",
        verbose_name="snippet (dowód)",
        help_text="Fragment treści uzasadniający wybór etykiety."
    )
    comment = models.TextField(
        blank=True, default="",
        verbose_name="komentarz",
        help_text="Uwagi użytkownika do etykiety finalnej."
    )
    created_at = models.DateTimeField(
        default=timezone.now,
        verbose_name="utworzono",
        help_text="Znacznik czasu utworzenia/aktualizacji etykiety finalnej."
    )

    class Meta:
        verbose_name = "Etykieta finalna"
        verbose_name_plural = "Etykiety finalne"
        constraints = [
            models.UniqueConstraint(fields=["sample", "kind"], name="uniq_final_label_per_sample_kind"),
        ]
        indexes = [
            models.Index(fields=["kind", "value"]),
        ]
        ordering = ["-created_at", "kind_id"]

    def __str__(self):
        return f"[FINAL] kind#{self.kind_id} → value#{self.value_id} @ sample#{self.sample_id}"


class Annotation(models.Model):
    """
    Adnotacja człowieka – pojedynczy głos/ocena dla (sample, kind).
    Może istnieć wiele adnotacji dla tej samej próbki i rodzaju (konsensus/QA).
    """
    sample = models.ForeignKey(
        DatasetSample, on_delete=models.CASCADE, related_name="annotations",
        verbose_name="próbka", help_text="Próbka, którą adnotujemy."
    )
    kind = models.ForeignKey(
        "dataset.DictionaryKind", on_delete=models.PROTECT, related_name="+",
        verbose_name="rodzaj", help_text="Rodzaj etykiety (np. 'emotion', 'style')."
    )
    value = models.ForeignKey(
        "dataset.DictionaryValue", on_delete=models.PROTECT, related_name="annotations",
        verbose_name="wartość", help_text="Proponowana wartość słownikowa."
    )
    annotator = models.ForeignKey(
        "auth.User", on_delete=models.PROTECT, related_name="annotations",
        verbose_name="adnotator", help_text="Użytkownik, który wprowadził adnotację."
    )
    confidence = models.FloatField(
        default=1.0,
        verbose_name="pewność",
        help_text="Deklarowana pewność adnotatora w skali 0–1."
    )
    evidence_snippet = models.TextField(
        blank=True, default="",
        verbose_name="snippet (dowód)",
        help_text="Fragment treści uzasadniający wybór."
    )
    comment = models.TextField(
        blank=True, default="",
        verbose_name="komentarz",
        help_text="Uwagi adnotatora do tej etykiety."
    )
    created_at = models.DateTimeField(
        default=timezone.now,
        verbose_name="utworzono",
        help_text="Znacznik czasu dodania adnotacji."
    )
    is_gold = models.BooleanField(
        default=False,
        verbose_name="gold",
        help_text="Flaga oznaczająca adnotację jako 'złotą' (referencyjną)."
    )

    class Meta:
        verbose_name = "Adnotacja"
        verbose_name_plural = "Adnotacje"
        indexes = [
            models.Index(fields=["kind", "value"]),
            models.Index(fields=["annotator"]),
        ]
        ordering = ["-created_at", "-id"]

    def __str__(self):
        user = getattr(self.annotator, "username", self.annotator_id)
        return f"[ANN] {user}: kind#{self.kind_id} → value#{self.value_id} @ sample#{self.sample_id}"


class ModelPrediction(models.Model):
    """
    Surowa predykcja modelu (wersjonowana).
    Utrwalamy wynik inference (wartość, proba, snippet) dla danej próbki i rodzaju.
    """
    sample = models.ForeignKey(
        DatasetSample, on_delete=models.CASCADE, related_name="predictions",
        verbose_name="próbka", help_text="Próbka, dla której zapisano predykcję."
    )
    kind = models.ForeignKey(
        "dataset.DictionaryKind", on_delete=models.PROTECT, related_name="+",
        verbose_name="rodzaj", help_text="Rodzaj etykiety, którego dotyczy predykcja."
    )
    value = models.ForeignKey(
        "dataset.DictionaryValue", on_delete=models.PROTECT, related_name="predictions",
        verbose_name="wartość", help_text="Wartość przewidziana przez model."
    )
    proba = models.FloatField(
        default=0.0,
        verbose_name="prawdopodobieństwo",
        help_text="Pewność modelu (0–1)."
    )
    model_name = models.CharField(
        max_length=64,
        verbose_name="nazwa modelu",
        help_text="Identyfikator/dostawca modelu (np. 'openai')."
    )
    model_version = models.CharField(
        max_length=32,
        verbose_name="wersja modelu",
        help_text="Wersja/model użyty do inferencji (np. 'gpt-4o-mini')."
    )
    created_at = models.DateTimeField(
        default=timezone.now,
        verbose_name="utworzono",
        help_text="Znacznik czasu zapisu predykcji."
    )
    evidence_snippet = models.TextField(
        blank=True, default="",
        verbose_name="snippet (dowód)",
        help_text="Fragment treści wskazany przez model jako uzasadnienie."
    )
    dictionary = models.ForeignKey(
        "dataset.Dictionary",
        on_delete=models.PROTECT, null=True, blank=True,
        verbose_name="zestaw słownikowy",
        help_text="Konkretna wersja słownika, w ramach której wybrano wartość (opcjonalnie)."
    )

    class Meta:
        verbose_name = "Predykcja modelu"
        verbose_name_plural = "Predykcje modeli"
        indexes = [
            models.Index(fields=["kind", "model_name", "model_version"]),
        ]
        ordering = ["-created_at", "-id"]

    def __str__(self):
        dict_info = f"/dict#{self.dictionary_id}" if self.dictionary_id else ""
        return f"[PRED] {self.model_name}:{self.model_version}{dict_info} kind#{self.kind_id}→value#{self.value_id} @ sample#{self.sample_id}"
