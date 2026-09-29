import uuid

from django.db import models
from django.utils.translation import gettext_lazy as _


class MuseTransactionStatus(models.TextChoices):
    PENDING   = 'PENDING',   _('Pending')
    SUCCESS   = 'SUCCESS',   _('Success')
    FAILED    = 'FAILED',    _('Failed')
    RETRYING  = 'RETRYING',  _('Retrying')
    REJECTED  = 'REJECTED',  _('Rejected by Gateway')
    UNKNOWN   = 'UNKNOWN',   _('Outcome unknown')
    NOT_SENT  = 'NOT_SENT',  _('Recorded without sending')


class MuseTransactionType(models.TextChoices):
    BULK_PAYMENT   = 'BULK_PAYMENT',   _('Bulk payment message')
    ACK            = 'ACK',            _('Acknowledgement')
    RESPONSE       = 'RESPONSE',       _('Batch response')
    PAYMENT_STATUS = 'PAYMENT_STATUS', _('Payment status')


class MuseTransactionLog(models.Model):
    """
    Audit log of every message exchanged with MUSE: one row per outbound BULK_PAYMENT
    attempt (tasaf_payment.muse_sender) and one per inbound ACK / RESPONSE /
    PAYMENT_STATUS (tasaf_payment.muse_inbound). Never deleted.

    References are plain strings (msg_id, batch_reference, paylist_uuid, benefit_code),
    not foreign keys, so the log stays intact whatever happens to the rows it describes.
    """

    id = models.BigAutoField(primary_key=True)
    uuid = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)

    transaction_type = models.CharField(
        max_length=20,
        choices=MuseTransactionType.choices,
        db_index=True,
    )
    status = models.CharField(
        max_length=20,
        choices=MuseTransactionStatus.choices,
        default=MuseTransactionStatus.PENDING,
        db_index=True,
    )

    # References — stored as plain strings for independence
    benefit_code = models.CharField(max_length=255, db_index=True, blank=True, default='')

    # One row per message (outbound attempt or inbound message), not per payment.
    direction = models.CharField(max_length=3, default='OUT')
    msg_id = models.CharField(max_length=32, blank=True, default='', db_index=True)
    batch_reference = models.CharField(max_length=100, blank=True, default='')
    paylist_uuid = models.CharField(max_length=36, blank=True, default='', db_index=True)
    item_count = models.IntegerField(null=True, blank=True)
    esb_request_id = models.CharField(max_length=100, blank=True, default='')
    amount = models.DecimalField(max_digits=18, decimal_places=2, null=True)

    # MUSE response fields
    muse_reference = models.CharField(max_length=255, blank=True, null=True)
    http_status_code = models.IntegerField(null=True, blank=True)
    response_body = models.TextField(blank=True, null=True)
    error_message = models.TextField(blank=True, null=True)

    # Retry tracking
    attempt_number = models.PositiveSmallIntegerField(default=1)

    # Timestamps (immutable — auto-set on creation only)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        managed = True
        db_table = 'muse_TransactionLog'
        indexes = [
            models.Index(fields=['benefit_code', 'transaction_type']),
            models.Index(fields=['created_at']),
        ]
        # No soft-delete, no history — audit logs are immutable
        ordering = ['-created_at']

    def __str__(self):
        return (
            f"[{self.transaction_type}] {self.benefit_code} "
            f"→ {self.status} (attempt {self.attempt_number})"
        )
