from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    PersonViewSet,
    MessageViewSet,
    DictionaryViewSet,
    DictionaryKindViewSet,
    DictionaryValueViewSet,
    LabelViewSet,
    TokenAuthView,
    LabelPreviewView,
    ThreadViewSet
)

router = DefaultRouter()
router.register(r"people", PersonViewSet, basename="people")
router.register(r"messages", MessageViewSet, basename="messages")
router.register(r"dictionaries", DictionaryViewSet, basename="dictionaries")
router.register(r"dict-kinds", DictionaryKindViewSet, basename="dict-kinds")
router.register(r"dict-values", DictionaryValueViewSet, basename="dict-values")
router.register(r"labels", LabelViewSet, basename="labels")
router.register(r"threads", ThreadViewSet, basename="threads")


urlpatterns = [
    path("token-auth/", TokenAuthView.as_view(), name="token-auth"),  # <-- /api/token-auth/
    path("", include(router.urls)),                                   # Pozostałe endpointy
    path("api-auth/", include("rest_framework.urls")),                # sesyjne logowanie /api-auth/login/
    path("label/preview", LabelPreviewView.as_view(), name="label-preview"),
]
