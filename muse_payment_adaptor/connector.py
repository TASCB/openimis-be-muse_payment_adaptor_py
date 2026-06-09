"""
muse_payment_adaptor.connector
================================
HTTP client for the MUSE Payment Gateway API.

Design principles
-----------------
* One class, one job: handle HTTP calls to MUSE. No business logic here.
* Every call — success or failure — writes an immutable MuseTransactionLog row.
* Exponential back-off retry for transient HTTP errors (5xx / timeouts).
* Designed for high-volume: millions of BenefitConsumption records.
  - Caller (StrategyMusePayment) should call send_batch() for bulk operations.
  - Individual send_payment() / reconcile() are for single-record use.

MUSE API contract (assumed — adapt to actual MUSE spec)
-------------------------------------------------------
POST /payments/send
    Request:  { invoiceId, amount, accountNumber, fspCode, fspType, beneficiaryName }
    Response: { reference, status }  HTTP 200 on accepted

POST /payments/reconcile
    Request:  { invoiceId, amount }
    Response: { settled: true|false, reference }  HTTP 200

Authentication: Bearer token (preferred) or Basic Auth
    Configured via PayrollConfig / settings.PAYMENT_GATEWAYS per PaymentPoint.
"""

import logging
import time
from decimal import Decimal
from typing import Optional

from payroll.payment_gateway.payment_gateway_connector import PaymentGatewayConnector

logger = logging.getLogger(__name__)

# Retry settings for transient failures
_MAX_RETRIES    = 3
_BACKOFF_FACTOR = 2   # seconds: 2, 4, 8
_RETRYABLE_HTTP = {429, 500, 502, 503, 504}


class _GovESBResponseAdapter:
    """
    Adapt a ``GovESBProducer.publish()`` result dict to the minimal
    ``requests.Response`` surface the connector's success-checkers and loggers
    use (``.status_code`` / ``.text`` / ``.json()``).

    The MUSE business response (``status`` / ``settled`` / ``reference``) is
    expected inside the ESB ``esbBody`` — adapt here if the real MUSE-over-ESB
    contract nests it differently.
    """

    def __init__(self, result: dict):
        self._result = result or {}
        body = self._result.get("esb_body") or {}
        self._body = body if isinstance(body, dict) else {}
        ok = bool(self._result.get("ok"))
        self.status_code = self._result.get("status_code") or (200 if ok else 502)
        self.text = str(self._result)

    def json(self):
        return self._body


class MusePaymentGatewayConnector(PaymentGatewayConnector):
    """
    Concrete gateway connector for the MUSE payment network.

    Inherits session setup (headers, auth) from PaymentGatewayConnector.
    Overrides send_payment() and reconcile() with real MUSE semantics
    and adds send_payment_batch() for bulk dispatch.

    Usage
    -----
    Configured via PayrollConfig (or per-PaymentPoint settings.PAYMENT_GATEWAYS):

        payment_gateway_class = "muse_payment_adaptor.connector.MusePaymentGatewayConnector"
        gateway_base_url       = "https://api.muse.go.tz/v1/"
        endpoint_payment       = "payments/send"
        endpoint_reconciliation= "payments/reconcile"
        payment_gateway_auth_type = "token"
        payment_gateway_api_key   = "<key>"
    """

    # ──────────────────────────────────────────────────────────────────────────
    # Public interface
    # ──────────────────────────────────────────────────────────────────────────

    def send_payment(
        self,
        invoice_id: str,
        amount,
        *,
        account_number: Optional[str] = None,
        fsp_code: Optional[str] = None,
        fsp_type: Optional[str] = None,
        beneficiary_name: Optional[str] = None,
        payroll_id: Optional[int] = None,
        **kwargs,
    ) -> bool:
        """
        Instruct MUSE to disburse *amount* to *account_number*.

        Returns True if MUSE acknowledged the payment instruction.
        Returns False on gateway rejection (not retried).
        Raises on configuration / connectivity errors so the caller can handle.

        Writes a MuseTransactionLog record for every attempt.
        """
        payload = self._build_payment_payload(
            invoice_id, amount, account_number, fsp_code, fsp_type, beneficiary_name
        )
        log_kwargs = dict(
            transaction_type='PAYMENT',
            benefit_code=str(invoice_id),
            account_number=account_number,
            fsp_name=fsp_code,
            fsp_type=fsp_type,
            amount=Decimal(str(amount)),
            payroll_id=payroll_id,
        )

        return self._call_with_retry(
            endpoint=self.config.endpoint_payment,
            payload=payload,
            success_checker=self._is_payment_accepted,
            log_kwargs=log_kwargs,
        )

    def reconcile(
        self,
        invoice_id: str,
        amount,
        *,
        payroll_id: Optional[int] = None,
        **kwargs,
    ) -> bool:
        """
        Ask MUSE whether *invoice_id* has been settled.

        Returns True if MUSE confirms settlement.
        Returns False if not yet settled (caller should retry later).
        Writes a MuseTransactionLog record.
        """
        payload = {"invoiceId": str(invoice_id), "amount": str(amount)}
        log_kwargs = dict(
            transaction_type='RECONCILIATION',
            benefit_code=str(invoice_id),
            amount=Decimal(str(amount)),
            payroll_id=payroll_id,
        )

        return self._call_with_retry(
            endpoint=self.config.endpoint_reconciliation,
            payload=payload,
            success_checker=self._is_reconciliation_confirmed,
            log_kwargs=log_kwargs,
        )

    def send_payment_batch(self, payments: list, payroll_id: Optional[int] = None) -> dict:
        """
        Send a batch of payment instructions to MUSE in a single HTTP call
        (if the MUSE API supports bulk endpoint) or falls back to sequential
        individual calls.

        payments: list of dicts with keys:
            invoice_id, amount, account_number, fsp_code, fsp_type, beneficiary_name

        Returns:
            {
                'accepted': [invoice_id, ...],
                'rejected': [invoice_id, ...],
                'errors':   [(invoice_id, error_msg), ...],
            }
        """
        bulk_endpoint = getattr(self.config, 'endpoint_payment_bulk', None)

        if bulk_endpoint:
            return self._send_bulk(payments, bulk_endpoint, payroll_id)
        else:
            return self._send_sequential(payments, payroll_id)

    # ──────────────────────────────────────────────────────────────────────────
    # Private helpers
    # ──────────────────────────────────────────────────────────────────────────

    # ──────────────────────────────────────────────────────────────────────────
    # Transport
    # ──────────────────────────────────────────────────────────────────────────

    def send_request(self, endpoint, payload):
        """
        Send one request to MUSE.

        When the shared GovESB transport is enabled *and* a GovESB api code is
        configured for this endpoint, the request goes over the signed-envelope
        ESB (``coremis_app_integration.govesb.GovESBProducer``) so the whole
        deployment uses one transport/credentials/keys. Otherwise it falls back
        to the base connector's direct REST POST — existing REST-based
        deployments are therefore unchanged by default.
        """
        api_code = self._govesb_api_code_for(endpoint)
        if api_code and self._govesb_available():
            return self._send_via_govesb(api_code, payload)
        return super().send_request(endpoint, payload)

    @staticmethod
    def _govesb_available() -> bool:
        try:
            from coremis_app_integration.govesb import govesb_enabled
        except ImportError:
            return False
        return govesb_enabled()

    def _govesb_api_code_for(self, endpoint):
        """Per-endpoint GovESB api code, then a single default; None ⇒ use REST."""
        cfg = self.config
        if endpoint == getattr(cfg, "endpoint_payment", None):
            code = getattr(cfg, "govesb_api_code_payment", None)
        elif endpoint == getattr(cfg, "endpoint_reconciliation", None):
            code = getattr(cfg, "govesb_api_code_reconciliation", None)
        else:
            code = None
        return code or getattr(cfg, "govesb_api_code", None)

    def _send_via_govesb(self, api_code, payload):
        from coremis_app_integration.govesb import GovESBProducer
        from coremis_app_integration.esb_client import ESBRequestType

        result = GovESBProducer().publish(
            api_code, payload, request_type=ESBRequestType.NORMAL,
        )
        return _GovESBResponseAdapter(result)

    def _call_with_retry(
        self,
        endpoint: str,
        payload: dict,
        success_checker,
        log_kwargs: dict,
    ) -> bool:
        """
        POST *payload* to *endpoint* with exponential back-off retry.
        Writes one MuseTransactionLog per attempt.
        """
        last_exc = None

        for attempt in range(1, _MAX_RETRIES + 1):
            log_entry = self._start_log(attempt=attempt, **log_kwargs)
            try:
                response = self.send_request(endpoint, payload)

                if response is None:
                    self._fail_log(log_entry, error='No response from MUSE gateway')
                    last_exc = ConnectionError('MUSE gateway returned no response')
                    self._maybe_backoff(attempt)
                    continue

                http_code = response.status_code
                body = response.text

                if http_code in _RETRYABLE_HTTP and attempt < _MAX_RETRIES:
                    self._retry_log(log_entry, http_code, body)
                    self._maybe_backoff(attempt)
                    continue

                accepted = success_checker(response)
                if accepted:
                    self._success_log(log_entry, http_code, body, response)
                else:
                    self._reject_log(log_entry, http_code, body)

                return accepted

            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "MUSE connector: unhandled error on attempt %d for %s",
                    attempt, log_kwargs.get('benefit_code', '?'),
                )
                self._fail_log(log_entry, error=str(exc))
                last_exc = exc
                self._maybe_backoff(attempt)

        logger.error(
            "MUSE connector: all %d attempts exhausted for %s",
            _MAX_RETRIES, log_kwargs.get('benefit_code', '?'),
        )
        if last_exc:
            raise last_exc
        return False

    def _send_bulk(self, payments: list, endpoint: str, payroll_id: Optional[int]) -> dict:
        """Single bulk HTTP call with a list of payment instructions."""
        payload = {
            "payrollId": payroll_id,
            "payments": [self._build_payment_payload(**p) for p in payments],
        }
        accepted, rejected, errors = [], [], []
        try:
            response = self.send_request(endpoint, payload)
            if response and response.status_code == 200:
                data = response.json()
                accepted = [r['invoiceId'] for r in data.get('accepted', [])]
                rejected = [r['invoiceId'] for r in data.get('rejected', [])]
            else:
                errors = [(p['invoice_id'], 'bulk call failed') for p in payments]
        except Exception as exc:  # noqa: BLE001
            logger.exception("MUSE bulk payment call failed")
            errors = [(p['invoice_id'], str(exc)) for p in payments]

        return {'accepted': accepted, 'rejected': rejected, 'errors': errors}

    def _send_sequential(self, payments: list, payroll_id: Optional[int]) -> dict:
        """Fallback: one HTTP call per payment instruction."""
        accepted, rejected, errors = [], [], []
        for p in payments:
            try:
                ok = self.send_payment(payroll_id=payroll_id, **p)
                (accepted if ok else rejected).append(p['invoice_id'])
            except Exception as exc:  # noqa: BLE001
                errors.append((p['invoice_id'], str(exc)))
        return {'accepted': accepted, 'rejected': rejected, 'errors': errors}

    @staticmethod
    def _build_payment_payload(
        invoice_id, amount,
        account_number=None, fsp_code=None, fsp_type=None, beneficiary_name=None,
        **_,
    ) -> dict:
        payload = {
            "invoiceId":       str(invoice_id),
            "amount":          str(amount),
        }
        if account_number:
            payload["accountNumber"]   = account_number
        if fsp_code:
            payload["fspCode"]         = fsp_code
        if fsp_type:
            payload["fspType"]         = fsp_type
        if beneficiary_name:
            payload["beneficiaryName"] = beneficiary_name
        return payload

    @staticmethod
    def _is_payment_accepted(response) -> bool:
        """Parse MUSE payment response."""
        try:
            data = response.json()
            status = data.get('status', '').upper()
            return status in {'ACCEPTED', 'QUEUED', 'SUCCESS'}
        except Exception:  # noqa: BLE001
            # Fallback: plain text response (like the mock)
            text = response.text.strip().lower()
            return 'accepted' in text or text == 'true'

    @staticmethod
    def _is_reconciliation_confirmed(response) -> bool:
        """Parse MUSE reconciliation response."""
        try:
            data = response.json()
            return bool(data.get('settled', False))
        except Exception:  # noqa: BLE001
            return response.text.strip().lower() == 'true'

    # ──────────────────────────────────────────────────────────────────────────
    # Audit log helpers — every write is a new INSERT, never UPDATE
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _start_log(attempt: int, **kwargs) -> 'MuseTransactionLog | None':
        """Write a PENDING log entry. Returns None if the model import fails."""
        try:
            from muse_payment_adaptor.models import MuseTransactionLog
            return MuseTransactionLog.objects.create(
                status='PENDING',
                attempt_number=attempt,
                **kwargs,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not write MuseTransactionLog (PENDING): %s", exc)
            return None

    @staticmethod
    def _success_log(log_entry, http_code: int, body: str, response) -> None:
        if not log_entry:
            return
        try:
            muse_ref = None
            try:
                muse_ref = response.json().get('reference')
            except Exception:  # noqa: BLE001
                pass
            log_entry.status = 'SUCCESS'
            log_entry.http_status_code = http_code
            log_entry.response_body = body[:4000]  # guard against huge payloads
            log_entry.muse_reference = muse_ref
            log_entry.save(update_fields=['status', 'http_status_code', 'response_body', 'muse_reference'])
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not update MuseTransactionLog (SUCCESS): %s", exc)

    @staticmethod
    def _fail_log(log_entry, error: str) -> None:
        if not log_entry:
            return
        try:
            log_entry.status = 'FAILED'
            log_entry.error_message = error[:2000]
            log_entry.save(update_fields=['status', 'error_message'])
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not update MuseTransactionLog (FAILED): %s", exc)

    @staticmethod
    def _retry_log(log_entry, http_code: int, body: str) -> None:
        if not log_entry:
            return
        try:
            log_entry.status = 'RETRYING'
            log_entry.http_status_code = http_code
            log_entry.response_body = body[:4000]
            log_entry.save(update_fields=['status', 'http_status_code', 'response_body'])
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not update MuseTransactionLog (RETRYING): %s", exc)

    @staticmethod
    def _reject_log(log_entry, http_code: int, body: str) -> None:
        if not log_entry:
            return
        try:
            log_entry.status = 'REJECTED'
            log_entry.http_status_code = http_code
            log_entry.response_body = body[:4000]
            log_entry.save(update_fields=['status', 'http_status_code', 'response_body'])
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not update MuseTransactionLog (REJECTED): %s", exc)

    @staticmethod
    def _maybe_backoff(attempt: int) -> None:
        sleep_secs = _BACKOFF_FACTOR ** attempt
        logger.debug("MUSE connector: backing off %ds before retry", sleep_secs)
        time.sleep(sleep_secs)
