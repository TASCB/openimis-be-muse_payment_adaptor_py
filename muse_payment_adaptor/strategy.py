"""StrategyMusePayment: selected by Payroll.payment_method = "StrategyMusePayment".

Dispatch belongs to the tasaf_payment paylist flow (batched, two approvers, GovESB).
"""

import logging

from payroll.models import BenefitConsumptionStatus
from payroll.strategies.strategy_online_payment import StrategyOnlinePayment

logger = logging.getLogger(__name__)


class StrategyMusePayment(StrategyOnlinePayment):
    """
    MUSE-specific payment strategy.

    payment_method key (stored on Payroll.payment_method): "StrategyMusePayment"
    """

    WORKFLOW_NAME  = "muse-payment"
    WORKFLOW_GROUP = "openimis-tasaf-muse-payment"

    @classmethod
    def make_payment_for_payroll(cls, payroll, user, **kwargs):
        """No-op: a second route to the gateway would bypass batching and the
        two-approver gate and could pay a benefit twice. Logs instead of raising so
        payroll's Celery task still completes.
        """
        pending = cls.get_benefits_attached_to_payroll(
            payroll, BenefitConsumptionStatus.ACCEPTED,
        ).count()
        logger.warning(
            "%s.make_payment_for_payroll is a no-op: payroll=%s has %d ACCEPTED benefit(s); "
            "disbursement happens in the Tasaf Payments Disbursement tab, which batches, "
            "gates on two approvers and dispatches over GovESB.",
            cls.__name__, payroll.id, pending,
        )

    @classmethod
    def reconcile_payroll(cls, payroll, user):
        """Reconcile from PROCESSED paylist items; unapplied benefits stay ACCEPTED
        for a later cycle. Accounting is payroll's own reconcile_benefit_consumption.
        """
        from payroll.models import BenefitConsumption, PayrollStatus
        from tasaf_payment.models import PaylistItem, PaylistItemStatus

        settled_ids = (
            PaylistItem.objects
            .filter(paylist__payroll_id=payroll.id, is_deleted=False,
                    status=PaylistItemStatus.PROCESSED)
            .values_list('benefit_consumption_id', flat=True)
        )
        benefits = BenefitConsumption.objects.filter(id__in=settled_ids, is_deleted=False)
        count = benefits.count()

        if count:
            cls.reconcile_benefit_consumption(benefits, user)
        cls.change_status_of_payroll(payroll, PayrollStatus.RECONCILED, user)
        logger.info(
            "%s.reconcile_payroll: payroll=%s reconciled %d settled benefit(s)",
            cls.__name__, payroll.id, count,
        )
