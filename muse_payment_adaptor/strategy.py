"""
muse_payment_adaptor.strategy
==============================
StrategyMusePayment — the MUSE-specific payment strategy.

Extends StrategyOnlinePayment (from payroll) with:

1.  Bulk account resolution: fetches ALL PaymentAccounts for a payroll's
    beneficiaries in a single SQL query before starting the dispatch loop.
    Essential for payrolls covering millions of records.

2.  Account enrichment: passes account_number, fsp_code, fsp_type,
    beneficiary_name to the connector so MUSE receives a complete payload.

3.  Missing-account guard: skips individuals without a verified primary
    account and logs them for investigation, rather than crashing the batch.

4.  Bulk dispatch: calls connector.send_payment_batch() when the MUSE
    API supports a bulk endpoint; falls back to sequential otherwise.

5.  Celery-backed make_payment_for_payroll: large payrolls are split into
    configurable chunks and dispatched as independent Celery tasks so
    workers can process them in parallel.

Registration
------------
Registered in muse_payment_adaptor.apps.MusePaymentAdaptorConfig.ready()
into payroll's PaymentsMethodRegistryPoint.  Payroll records that want
MUSE disbursement must have payment_method = "StrategyMusePayment".
"""

import logging

from django.db import transaction
from django.db.models import Q

from payroll.models import BenefitConsumptionStatus
from payroll.strategies.strategy_online_payment import StrategyOnlinePayment

logger = logging.getLogger(__name__)

# How many benefits to dispatch in a single Celery task.
# Tune to match your MUSE API rate limits and worker concurrency.
_CHUNK_SIZE = 500


class StrategyMusePayment(StrategyOnlinePayment):
    """
    MUSE-specific payment strategy.

    payment_method key (stored on Payroll.payment_method): "StrategyMusePayment"
    """

    WORKFLOW_NAME  = "muse-payment"
    WORKFLOW_GROUP = "openimis-tasaf-muse-payment"

    # ──────────────────────────────────────────────────────────────────────────
    # Override: large payrolls use Celery chunks, not a single blocking loop
    # ──────────────────────────────────────────────────────────────────────────

    @classmethod
    def make_payment_for_payroll(cls, payroll, user, **kwargs):
        """
        Dispatch payment instructions to MUSE for all ACCEPTED benefits.

        For small payrolls (≤ _CHUNK_SIZE) the dispatch happens in-process.
        For larger payrolls the benefit IDs are split into chunks and each
        chunk is processed by a dedicated Celery task running in parallel.
        """
        from muse_payment_adaptor.tasks import dispatch_muse_payment_chunks

        benefit_ids = list(
            cls.get_benefits_attached_to_payroll(payroll, BenefitConsumptionStatus.ACCEPTED)
            .values_list('id', flat=True)
        )
        total = len(benefit_ids)
        logger.info(
            "StrategyMusePayment: queuing %d benefit(s) for payroll %s",
            total, payroll.id,
        )

        if total == 0:
            logger.warning("StrategyMusePayment: no ACCEPTED benefits for payroll %s", payroll.id)
            return

        if total <= _CHUNK_SIZE:
            # Small payroll — run directly in the calling Celery task context
            cls._dispatch_chunk(payroll.id, benefit_ids, user.id)
        else:
            # Large payroll — fan out into parallel Celery sub-tasks
            dispatch_muse_payment_chunks.delay(payroll.id, benefit_ids, user.id)

    # ──────────────────────────────────────────────────────────────────────────
    # Core dispatch: resolves accounts, calls connector, updates statuses
    # ──────────────────────────────────────────────────────────────────────────

    @classmethod
    def _dispatch_chunk(cls, payroll_id, benefit_ids: list, user_id: int) -> None:
        """
        Process a single chunk of benefit IDs:
        1. Bulk-load verified PaymentAccounts for all individuals in the chunk.
        2. Call connector.send_payment() for each benefit.
        3. Batch-update statuses.
        """
        from payroll.models import BenefitConsumption

        # Gateway must be initialised by the caller before invoking _dispatch_chunk.
        # (openIMIS convention: initialisation is the task/orchestrator's responsibility,
        # matching payroll/tasks.py send_requests_to_gateway_payment pattern.)

        benefits = list(
            BenefitConsumption.objects.filter(
                id__in=benefit_ids,
                status=BenefitConsumptionStatus.ACCEPTED,
                is_deleted=False,
            ).select_related('individual')
        )

        if not benefits:
            return

        # One SQL query to fetch all relevant verified accounts
        account_map = cls._build_account_map(benefits)

        approved_ids, skipped_ids = [], []

        for benefit in benefits:
            account = account_map.get(benefit.individual_id)
            if not account:
                logger.warning(
                    "MusePayment: no verified primary PaymentAccount for individual %s "
                    "(benefit %s) — skipping",
                    benefit.individual_id, benefit.code,
                )
                skipped_ids.append(benefit.id)
                continue

            try:
                ok = cls.PAYMENT_GATEWAY.send_payment(
                    invoice_id=benefit.code,
                    amount=benefit.amount,
                    account_number=account.account_number,
                    fsp_code=account.fsp_name,
                    fsp_type=account.fsp_type,
                    beneficiary_name=account.account_name,
                    payroll_id=payroll_id,
                )
                if ok:
                    approved_ids.append(benefit.id)
                else:
                    skipped_ids.append(benefit.id)
                    logger.info(
                        "MusePayment: gateway rejected benefit %s (account %s)",
                        benefit.code, account.account_number,
                    )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "MusePayment: error sending benefit %s: %s",
                    benefit.code, exc,
                )
                skipped_ids.append(benefit.id)

        # Bulk update approved benefits — single UPDATE query
        if approved_ids:
            with transaction.atomic():
                BenefitConsumption.objects.filter(id__in=approved_ids).update(
                    status=BenefitConsumptionStatus.APPROVE_FOR_PAYMENT
                )
            logger.info(
                "MusePayment: payroll %s — approved %d, skipped %d of %d benefits",
                payroll_id, len(approved_ids), len(skipped_ids), len(benefits),
            )

    @classmethod
    def _build_account_map(cls, benefits: list) -> dict:
        """
        Returns {individual_id: PaymentAccount} for all individuals in *benefits*.

        Single SQL query via JOIN through:
        Individual → GroupIndividual → Group → GroupBeneficiary → PaymentAccount
        """
        from tasaf_payment.models import PaymentAccount, VerificationStatus

        individual_ids = {b.individual_id for b in benefits}

        # Fetch accounts with traversal to individual_id
        qs = (
            PaymentAccount.objects
            .filter(
                group_beneficiary__group__groupindividuals__individual_id__in=individual_ids,
                group_beneficiary__group__groupindividuals__is_deleted=False,
                verification_status=VerificationStatus.VERIFIED,
                is_primary=True,
                is_deleted=False,
            )
            .values(
                'id',
                'account_number',
                'account_name',
                'fsp_name',
                'fsp_type',
                'group_beneficiary__group__groupindividuals__individual_id',
            )
            .distinct()
        )

        account_map = {}
        for row in qs:
            ind_id = row['group_beneficiary__group__groupindividuals__individual_id']
            if ind_id not in account_map:
                # Build a lightweight object so callers can use dot notation
                account_map[ind_id] = _AccountProxy(
                    account_number=row['account_number'],
                    account_name=row['account_name'],
                    fsp_name=row['fsp_name'],
                    fsp_type=row['fsp_type'],
                )
        return account_map


class _AccountProxy:
    """Lightweight stand-in for PaymentAccount used inside _build_account_map."""

    __slots__ = ('account_number', 'account_name', 'fsp_name', 'fsp_type')

    def __init__(self, *, account_number, account_name, fsp_name, fsp_type):
        self.account_number = account_number
        self.account_name   = account_name
        self.fsp_name       = fsp_name
        self.fsp_type       = fsp_type
