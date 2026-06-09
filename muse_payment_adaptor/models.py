import uuid

from django.db import models
from django.utils.translation import gettext_lazy as _


class MuseTransactionStatus(models.TextChoices):
    PENDING   = 'PENDING',   _('Pending')
    SUCCESS   = 'SUCCESS',   _('Success')
    FAILED    = 'FAILED',    _('Failed')
    RETRYING  = 'RETRYING',  _('Retrying')
    REJECTED  = 'REJECTED',  _('Rejected by Gateway')


class MuseTransactionType(models.TextChoices):
    PAYMENT        = 'PAYMENT',        _('Payment')
    RECONCILIATION = 'RECONCILIATION', _('Reconciliation')


class MuseTransactionLog(models.Model):
    """
    Immutable audit log for every HTTP call made to the MUSE payment gateway.

    One record is written per API attempt (including retries).  Never update
    or soft-delete records — this is a compliance audit trail.  Use
    status=RETRYING to mark transient failures that will be retried, then
    write a fresh SUCCESS/FAILED record for the final outcome.

    payroll_id and benefit_code are plain CharField references (not FK) so
    this table can be read independently and remains intact even if the
    related payroll is later deleted.
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
    payroll_id = models.IntegerField(null=True, blank=True, db_index=True)
    benefit_code = models.CharField(max_length=255, db_index=True)
    account_number = models.CharField(max_length=50, blank=True, null=True)
    fsp_name = models.CharField(max_length=100, blank=True, null=True)
    fsp_type = models.CharField(max_length=20, blank=True, null=True)
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
            models.Index(fields=['payroll_id', 'status']),
            models.Index(fields=['created_at']),
        ]
        # No soft-delete, no history — audit logs are immutable
        ordering = ['-created_at']

    def __str__(self):
        return (
            f"[{self.transaction_type}] {self.benefit_code} "
            f"→ {self.status} (attempt {self.attempt_number})"
        )
