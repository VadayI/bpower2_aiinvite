# ingestion/signals.py
from typing import Iterable, Tuple, Set
from django.db.models import Q, Exists, OuterRef, Count, Max
from django.db.models.signals import post_save, post_delete, pre_save
from django.db.models.functions import Coalesce
from django.dispatch import receiver

from .models import EmailMessage, MessageRecipient, PartnerStat

# ---- helpers ---------------------------------------------------------------

def _canon_pair(x: int, y: int) -> Tuple[int, int]:
    """Zwraca uporządkowaną parę (a_id < b_id)."""
    return (x, y) if x < y else (y, x)

# ---- EmailMessage lifecycle -----------------------------------------------

def _message_pairs(msg: EmailMessage) -> Set[Tuple[int, int]]:
    """
    Pary do liczenia wg reguł:
      - (from_person, każdy TO)
      - (delivered_to, from_person) jeśli obie strony istnieją
    """
    pairs: Set[Tuple[int, int]] = set()
    if msg.from_person_id:
        to_ids = MessageRecipient.objects.filter(
            message_id=msg.id, kind=MessageRecipient.Kind.TO
        ).values_list("person_id", flat=True)
        for rid in to_ids:
            pairs.add(_canon_pair(msg.from_person_id, rid))
    if msg.delivered_to_id and msg.from_person_id:
        pairs.add(_canon_pair(msg.delivered_to_id, msg.from_person_id))
    return pairs

def _message_pairs_from_fields(*, from_person_id, delivered_to_id, to_ids: Iterable[int]) -> Set[Tuple[int, int]]:
    pairs: Set[Tuple[int, int]] = set()
    if from_person_id:
        for rid in to_ids:
            pairs.add(_canon_pair(from_person_id, rid))
    if delivered_to_id and from_person_id:
        pairs.add(_canon_pair(delivered_to_id, from_person_id))
    return pairs

def _qualifying_messages_qs(a_id: int, b_id: int):
    """
    Zwraca QS EmailMessage spełniających ANY z warunków dla pary (a,b):
      - from=a AND TO contains b
      - from=b AND TO contains a
      - delivered_to=a AND from=b
      - delivered_to=b AND from=a
    DISTINCT po id (ważne!).
    """
    to_has_a = Exists(MessageRecipient.objects.filter(message=OuterRef("pk"),
                                                      kind=MessageRecipient.Kind.TO,
                                                      person_id=a_id))
    to_has_b = Exists(MessageRecipient.objects.filter(message=OuterRef("pk"),
                                                      kind=MessageRecipient.Kind.TO,
                                                      person_id=b_id))

    cond = (
        (Q(from_person_id=a_id) & to_has_b) |
        (Q(from_person_id=b_id) & to_has_a) |
        (Q(delivered_to_id=a_id) & Q(from_person_id=b_id)) |
        (Q(delivered_to_id=b_id) & Q(from_person_id=a_id))
    )

    return (
        EmailMessage.objects
        .filter(cond)
        .distinct()  # DISTINCT po id
    )

def recompute_partner_stats_for_pairs(pairs: Iterable[Tuple[int, int]]) -> None:
    """
    Dla każdej pary (a,b) przelicz PartnerStat w oparciu o aktualną zawartość EmailMessage.
    """
    for a_id, b_id in set(pairs):
        base_qs = _qualifying_messages_qs(a_id, b_id)

        # Liczenie:
        #  - msg_count: count distinct id
        #  - msg_processed_count: count distinct id z processed
        #  - last_message_at: max(coalesce(received_at, sent_at))
        processed_filter = Q(user_processed=True) | Q(useless=True)

        agg = base_qs.aggregate(
            msg_count=Count("id", distinct=True),
            msg_processed_count=Count("id", distinct=True, filter=processed_filter),
            last_message_at=Max(Coalesce("received_at", "sent_at")),
        )

        # Upsert do PartnerStat
        obj, _ = PartnerStat.objects.get_or_create(a_id=a_id, b_id=b_id)
        PartnerStat.objects.filter(pk=obj.pk).update(
            msg_count=agg["msg_count"] or 0,
            msg_processed_count=agg["msg_processed_count"] or 0,
            last_message_at=agg["last_message_at"],
        )

@receiver(pre_save, sender=EmailMessage)
def email_pre_save_capture_pairs(sender, instance: EmailMessage, **kwargs):
    """
    Zapamiętaj pary 'przed' – potrzebne, gdy zmienią się from_person/delivered_to.
    (TO nie ruszamy tutaj; TO obsługują sygnały MessageRecipient.)
    """
    if not instance.pk:
        instance._old_pairs = set()
        return
    prev = EmailMessage.objects.only("from_person_id", "delivered_to_id").get(pk=instance.pk)
    # Pobierz TO z poprzedniego stanu
    prev_to_ids = list(
        MessageRecipient.objects.filter(message_id=instance.pk, kind=MessageRecipient.Kind.TO)
        .values_list("person_id", flat=True)
    )
    instance._old_pairs = _message_pairs_from_fields(
        from_person_id=prev.from_person_id,
        delivered_to_id=prev.delivered_to_id,
        to_ids=prev_to_ids,
    )

@receiver(post_save, sender=EmailMessage)
def email_post_save_recompute(sender, instance: EmailMessage, created, **kwargs):
    """
    Po każdej operacji na EmailMessage:
      - przelicz PartnerStat dla par: (stare ∪ nowe)
    """
    new_pairs = _message_pairs(instance)
    old_pairs = getattr(instance, "_old_pairs", set())
    affected = old_pairs.union(new_pairs)
    if affected:
        recompute_partner_stats_for_pairs(affected)

@receiver(post_delete, sender=EmailMessage)
def email_post_delete_recompute(sender, instance: EmailMessage, **kwargs):
    """
    Po usunięciu wiadomości przelicz pary, które ta wiadomość tworzyła.
    """
    pairs = _message_pairs(instance)
    if pairs:
        recompute_partner_stats_for_pairs(pairs)


# ---- MessageRecipient (TO) adds/removes ------------------------------------
@receiver(post_save, sender=MessageRecipient)
def mr_post_save_recompute(sender, instance: MessageRecipient, created, **kwargs):
    if instance.kind != MessageRecipient.Kind.TO:
        return
    msg = instance.message
    pairs = _message_pairs(msg)  # już zawiera (from ↔ każdy TO) + (delivered_to ↔ from)
    if pairs:
        recompute_partner_stats_for_pairs(pairs)

@receiver(post_delete, sender=MessageRecipient)
def mr_post_delete_recompute(sender, instance: MessageRecipient, **kwargs):
    if instance.kind != MessageRecipient.Kind.TO:
        return
    msg = instance.message
    pairs = _message_pairs(msg)
    if pairs:
        recompute_partner_stats_for_pairs(pairs)