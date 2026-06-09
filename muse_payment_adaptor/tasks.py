"""
muse_payment_adaptor.tasks
===========================
Celery tasks for MUSE payment dispatch and reconciliation.

All large-data operations run asynchronously through Celery so the web
request returns immediately and workers process the data in parallel.

Task topology for a large payroll
----------------------------------

    dispatch_muse_payment_chunks
        │
        ├── process_muse_payment_chunk (chunk 1)  ← worker A
        ├── process_muse_payment_chunk (chunk 2)  ← worker B
        └── process_muse_payment_chunk (chunk N)  ← worker C

Each chunk processes _CHUNK_SIZE benefits, resolves accounts, and calls
the MUSE connector.  Chunk size is tunable via payroll config.
"""

import logging
from itertools import islice

from celery import shared_task

from core.models import User
from payroll.models import Payroll

logger = logging.getLogger(__name__)

_CHUNK_SIZE = 500  # benefits per Celery task


# ─── helpers ──────────────────────────────────────────────────────────────────

def _chunks(iterable, size):
    """Yield successive slices of *size* from *iterable*."""
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk


# ─── tasks ────────────────────────────────────────────────────────────────────

@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    name='muse_payment_adaptor.dispatch_muse_payment_chunks',
)
def dispatch_muse_payment_chunks(self, payroll_id: int, benefit_ids: list, user_id: int):
    """
    Fan-out task: splits *benefit_ids* into chunks and dispatches each
    to process_muse_payment_chunk as an independent Celery task.

    Called by StrategyMusePayment.make_payment_for_payroll() for large payrolls.
    """
    try:
        total = len(benefit_ids)
        chunks = list(_chunks(benefit_ids, _CHUNK_SIZE))
        logger.info(
            "dispatch_muse_payment_chunks: payroll=%s, %d benefits → %d chunks",
            payroll_id, total, len(chunks),
        )
        for i, chunk in enumerate(chunks, start=1):
            process_muse_payment_chunk.delay(payroll_id, chunk, user_id, chunk_index=i)

    except Exception as exc:
        logger.exception("dispatch_muse_payment_chunks failed for payroll %s", payroll_id)
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=30,
    name='muse_payment_adaptor.process_muse_payment_chunk',
)
def process_muse_payment_chunk(
    self,
    payroll_id: int,
    benefit_ids: list,
    user_id: int,
    chunk_index: int = 0,
):
    """
    Process a single chunk of benefit IDs:
    1. Initialise the MUSE gateway connector.
    2. Resolve verified PaymentAccounts for all individuals.
    3. Call connector.send_payment() per benefit.
    4. Bulk-update approved statuses.

    Retried up to 3× on transient failure (5xx, timeout).
    """
    try:
        payroll = Payroll.objects.get(id=payroll_id)
        from muse_payment_adaptor.strategy import StrategyMusePayment

        logger.info(
            "process_muse_payment_chunk: payroll=%s chunk=%d (%d benefits)",
            payroll_id, chunk_index, len(benefit_ids),
        )
        StrategyMusePayment.initialize_payment_gateway()
        StrategyMusePayment._dispatch_chunk(payroll_id, benefit_ids, user_id)

    except Exception as exc:
        logger.exception(
            "process_muse_payment_chunk failed: payroll=%s chunk=%d",
            payroll_id, chunk_index,
        )
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=120,
    name='muse_payment_adaptor.reconcile_muse_payroll',
)
def reconcile_muse_payroll(self, payroll_id: int, user_id: int):
    """
    Reconcile all APPROVE_FOR_PAYMENT benefits in a payroll with MUSE.
    Called after the reconciliation Task is approved in tasks_management.

    Splits into chunks and runs each chunk as process_muse_reconcile_chunk.
    """
    try:
        from payroll.models import BenefitConsumption, BenefitConsumptionStatus

        benefit_ids = list(
            BenefitConsumption.objects.filter(
                payrollbenefitconsumption__payroll_id=payroll_id,
                status=BenefitConsumptionStatus.APPROVE_FOR_PAYMENT,
                is_deleted=False,
            ).values_list('id', flat=True)
        )

        if not benefit_ids:
            logger.info("reconcile_muse_payroll: no benefits to reconcile for payroll %s", payroll_id)
            return

        for i, chunk in enumerate(_chunks(benefit_ids, _CHUNK_SIZE), start=1):
            process_muse_reconcile_chunk.delay(payroll_id, chunk, user_id, chunk_index=i)

    except Exception as exc:
        logger.exception("reconcile_muse_payroll failed for payroll %s", payroll_id)
        raise self.retry(exc=exc)


@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    name='muse_payment_adaptor.process_muse_reconcile_chunk',
)
def process_muse_reconcile_chunk(
    self,
    payroll_id: int,
    benefit_ids: list,
    user_id: int,
    chunk_index: int = 0,
):
    """
    Reconcile a chunk of benefits with MUSE, then update their status
    and create PaymentInvoice records.
    """
    try:
        from payroll.models import BenefitConsumption, BenefitConsumptionStatus
        from muse_payment_adaptor.strategy import StrategyMusePayment

        # Fetch user once — used for both non-reconciled saves and reconcile_benefit_consumption.
        # openIMIS convention: always use a real User object, never a hardcoded string username.
        user = _get_user(user_id)
        StrategyMusePayment.initialize_payment_gateway()

        benefits = list(
            BenefitConsumption.objects.filter(
                id__in=benefit_ids,
                status=BenefitConsumptionStatus.APPROVE_FOR_PAYMENT,
                is_deleted=False,
            )
        )

        benefits_to_reconcile = []
        for benefit in benefits:
            is_reconciled = StrategyMusePayment.PAYMENT_GATEWAY.reconcile(
                invoice_id=benefit.code,
                amount=benefit.amount,
                payroll_id=payroll_id,
            )
            if benefit.json_ext is None:
                benefit.json_ext = {}

            gateway_result = {'settled': is_reconciled, 'payroll_id': payroll_id}
            benefit.json_ext = {**benefit.json_ext, 'muse_reconciliation': gateway_result}

            if is_reconciled:
                benefits_to_reconcile.append(benefit)
            else:
                benefit.save(username=user.login_name)

        if benefits_to_reconcile:
            StrategyMusePayment.reconcile_benefit_consumption(benefits_to_reconcile, user)

        logger.info(
            "process_muse_reconcile_chunk: payroll=%s chunk=%d — reconciled %d/%d",
            payroll_id, chunk_index, len(benefits_to_reconcile), len(benefits),
        )

    except Exception as exc:
        logger.exception(
            "process_muse_reconcile_chunk failed: payroll=%s chunk=%d",
            payroll_id, chunk_index,
        )
        raise self.retry(exc=exc)


def _get_user(user_id: int):
    """Fetch User for audit trail writes inside Celery tasks."""
    return User.objects.get(id=user_id)
