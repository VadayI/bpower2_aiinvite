# ingestion/apps.py
from django.apps import AppConfig

class IngestionConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "ingestion"

    def ready(self):
        from . import signals
