from django.apps import AppConfig


class MusePaymentAdaptorConfig(AppConfig):
    name = 'muse_payment_adaptor'

    def ready(self):
        """
        Register StrategyMusePayment in payroll's PaymentsMethodRegistryPoint.

        After registration, a Payroll with payment_method="StrategyMusePayment"
        will use this strategy for dispatch and reconciliation.
        """
        from payroll.payments_registry import PaymentsMethodRegistryPoint
        from muse_payment_adaptor.strategy import StrategyMusePayment

        PaymentsMethodRegistryPoint.register_payment_method(
            payment_method_class_list=[StrategyMusePayment()]
        )
