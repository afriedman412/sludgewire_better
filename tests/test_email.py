"""Tests for email service: filtering, sending, failure handling."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest
from sqlmodel import Session

from app.schemas import EmailRecipient, AppConfig
from app.email_service import (
    get_active_recipients,
    _filter_filings_for_recipient,
    _build_alert_email,
    send_email,
    send_filing_alert,
)


# -------------------------------------------------------
# Fixtures
# -------------------------------------------------------

def _add_recipient(session, email, active=True, committee_ids=None):
    r = EmailRecipient(email=email, active=active, committee_ids=committee_ids)
    session.add(r)
    session.commit()
    session.refresh(r)
    return r


SAMPLE_F3X_FILINGS = [
    {
        "committee_name": "Big PAC",
        "committee_id": "C00000001",
        "form_type": "F3XN",
        "report_type": "Q1",
        "coverage_from": "2025-01-01",
        "coverage_through": "2025-03-31",
        "filed_at_utc": "2025-02-10 14:00",
        "total_receipts": 100_000.0,
        "fec_url": "https://example.com/1.fec",
    },
    {
        "committee_name": "Small PAC",
        "committee_id": "C00000002",
        "form_type": "F3XN",
        "report_type": "Q1",
        "coverage_from": "2025-01-01",
        "coverage_through": "2025-03-31",
        "filed_at_utc": "2025-02-10 15:00",
        "total_receipts": 60_000.0,
        "fec_url": "https://example.com/2.fec",
    },
    {
        "committee_name": "Other PAC",
        "committee_id": "C00000003",
        "form_type": "F3XN",
        "report_type": "Q1",
        "coverage_from": "2025-01-01",
        "coverage_through": "2025-03-31",
        "filed_at_utc": "2025-02-10 16:00",
        "total_receipts": 200_000.0,
        "fec_url": "https://example.com/3.fec",
    },
]

SAMPLE_IE_EVENTS = [
    {
        "committee_name": "Super PAC",
        "committee_id": "C00000010",
        "candidate_name": "Jane Smith",
        "candidate_id": "P00000001",
        "candidate_office": "H",
        "candidate_state": "CA",
        "candidate_district": "12",
        "candidate_party": "DEM",
        "support_oppose": "S",
        "purpose": "TV Ad",
        "payee_name": "Media Inc",
        "expenditure_date": "2025-02-15",
        "amount": 50_000.0,
        "fec_url": "https://example.com/10.fec",
    },
]


# -------------------------------------------------------
# get_active_recipients
# -------------------------------------------------------

class TestGetActiveRecipients:
    def test_returns_active_only(self, session):
        _add_recipient(session, "active@test.com", active=True)
        _add_recipient(session, "inactive@test.com", active=False)
        result = get_active_recipients(session)
        emails = [r.email for r in result]
        assert "active@test.com" in emails
        assert "inactive@test.com" not in emails

    def test_returns_empty_when_none(self, session):
        assert get_active_recipients(session) == []

    def test_returns_full_objects(self, session):
        _add_recipient(session, "user@test.com", committee_ids=["C00000001"])
        result = get_active_recipients(session)
        assert len(result) == 1
        assert result[0].committee_ids == ["C00000001"]


# -------------------------------------------------------
# _filter_filings_for_recipient
# -------------------------------------------------------

class TestFilterFilingsForRecipient:
    def test_no_filter_returns_all(self, session):
        r = _add_recipient(session, "all@test.com", committee_ids=None)
        filtered = _filter_filings_for_recipient(SAMPLE_F3X_FILINGS, r)
        assert len(filtered) == 3

    def test_empty_list_filter_returns_all(self, session):
        r = _add_recipient(session, "all@test.com", committee_ids=[])
        filtered = _filter_filings_for_recipient(SAMPLE_F3X_FILINGS, r)
        assert len(filtered) == 3

    def test_single_committee_filter(self, session):
        r = _add_recipient(session, "filtered@test.com", committee_ids=["C00000001"])
        filtered = _filter_filings_for_recipient(SAMPLE_F3X_FILINGS, r)
        assert len(filtered) == 1
        assert filtered[0]["committee_id"] == "C00000001"

    def test_multi_committee_filter(self, session):
        r = _add_recipient(session, "multi@test.com", committee_ids=["C00000001", "C00000003"])
        filtered = _filter_filings_for_recipient(SAMPLE_F3X_FILINGS, r)
        assert len(filtered) == 2
        ids = {f["committee_id"] for f in filtered}
        assert ids == {"C00000001", "C00000003"}

    def test_filter_with_no_matches(self, session):
        r = _add_recipient(session, "nomatch@test.com", committee_ids=["C99999999"])
        filtered = _filter_filings_for_recipient(SAMPLE_F3X_FILINGS, r)
        assert len(filtered) == 0


# -------------------------------------------------------
# _build_alert_email
# -------------------------------------------------------

class TestBuildAlertEmail:
    def test_f3x_subject_and_body(self):
        subject, body = _build_alert_email("3x", SAMPLE_F3X_FILINGS)
        assert "3" in subject  # count of filings
        assert "F3X" in subject
        assert "Big PAC" in body
        assert "$100,000.00" in body

    def test_ie_subject_and_body(self):
        subject, body = _build_alert_email("e", SAMPLE_IE_EVENTS)
        assert "Schedule E" in subject
        assert "Jane Smith" in body
        assert "$50,000.00" in body

    def test_empty_filings(self):
        subject, body = _build_alert_email("3x", [])
        assert "0" in subject


# -------------------------------------------------------
# send_email
# -------------------------------------------------------

class TestSendEmail:
    @patch("app.email_service.load_settings")
    def test_missing_credentials(self, mock_settings):
        mock_settings.return_value = MagicMock(google_app_pw=None, email_from=None)
        assert send_email(["test@test.com"], "Subject", "<p>Body</p>") is False

    @patch("app.email_service.load_settings")
    def test_no_recipients(self, mock_settings):
        mock_settings.return_value = MagicMock(google_app_pw="pw", email_from="from@test.com")
        assert send_email([], "Subject", "<p>Body</p>") is False

    @patch("app.email_service.smtplib.SMTP")
    @patch("app.email_service.load_settings")
    def test_successful_send(self, mock_settings, mock_smtp_cls):
        mock_settings.return_value = MagicMock(google_app_pw="pw", email_from="from@test.com")
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)

        result = send_email(["to@test.com"], "Subject", "<p>Body</p>")
        assert result is True
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("from@test.com", "pw")
        mock_server.sendmail.assert_called_once()

    @patch("app.email_service.smtplib.SMTP")
    @patch("app.email_service.load_settings")
    def test_smtp_failure(self, mock_settings, mock_smtp_cls):
        mock_settings.return_value = MagicMock(google_app_pw="pw", email_from="from@test.com")
        mock_smtp_cls.return_value.__enter__ = MagicMock(side_effect=Exception("SMTP error"))
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)

        result = send_email(["to@test.com"], "Subject", "<p>Body</p>")
        assert result is False


# -------------------------------------------------------
# send_filing_alert (end-to-end with mocked SMTP)
# -------------------------------------------------------

class TestSendFilingAlert:
    @patch("app.email_service.send_email", return_value=True)
    def test_sends_to_all_unfiltered_recipients(self, mock_send, session):
        _add_recipient(session, "a@test.com")
        _add_recipient(session, "b@test.com")

        result = send_filing_alert(session, "3x", SAMPLE_F3X_FILINGS)

        assert mock_send.call_count == 2
        assert "a@test.com" in result
        assert "b@test.com" in result
        assert result["a@test.com"] == 3
        assert result["b@test.com"] == 3

    @patch("app.email_service.send_email", return_value=True)
    def test_filtered_recipient_gets_subset(self, mock_send, session):
        _add_recipient(session, "all@test.com", committee_ids=None)
        _add_recipient(session, "filtered@test.com", committee_ids=["C00000001"])

        result = send_filing_alert(session, "3x", SAMPLE_F3X_FILINGS)

        assert result["all@test.com"] == 3
        assert result["filtered@test.com"] == 1

    @patch("app.email_service.send_email", return_value=True)
    def test_filtered_recipient_no_matches_gets_nothing(self, mock_send, session):
        _add_recipient(session, "nomatch@test.com", committee_ids=["C99999999"])

        result = send_filing_alert(session, "3x", SAMPLE_F3X_FILINGS)

        assert mock_send.call_count == 0
        assert result == {}

    def test_no_recipients_returns_empty(self, session):
        result = send_filing_alert(session, "3x", SAMPLE_F3X_FILINGS)
        assert result == {}

    def test_empty_filings_returns_empty(self, session):
        _add_recipient(session, "a@test.com")
        result = send_filing_alert(session, "3x", [])
        assert result == {}

    @patch("app.email_service.send_email", return_value=False)
    def test_smtp_failure_excluded_from_result(self, mock_send, session):
        _add_recipient(session, "a@test.com")

        result = send_filing_alert(session, "3x", SAMPLE_F3X_FILINGS)

        assert mock_send.call_count == 1
        assert result == {}  # send failed, so no entries

    @patch("app.email_service.send_email", return_value=True)
    def test_inactive_recipient_excluded(self, mock_send, session):
        _add_recipient(session, "active@test.com", active=True)
        _add_recipient(session, "inactive@test.com", active=False)

        result = send_filing_alert(session, "3x", SAMPLE_F3X_FILINGS)

        assert mock_send.call_count == 1
        assert "active@test.com" in result
        assert "inactive@test.com" not in result

    @patch("app.email_service.send_email", return_value=True)
    def test_ie_events_sent(self, mock_send, session):
        _add_recipient(session, "a@test.com")

        result = send_filing_alert(session, "e", SAMPLE_IE_EVENTS)

        assert result["a@test.com"] == 1
        # Verify subject mentions Schedule E
        call_args = mock_send.call_args
        assert "Schedule E" in call_args[0][1]


# -------------------------------------------------------
# SA/SB attention alert
# -------------------------------------------------------

class TestSaSbAttentionAlert:
    def _make_filing(self, session, fid, *, committee_id="C00001", name="Test PAC"):
        from app.repo import upsert_f3x
        from datetime import date
        upsert_f3x(
            session,
            filing_id=fid,
            committee_id=committee_id,
            committee_name=name,
            form_type="F3XN",
            report_type="Q1",
            coverage_from=date(2025, 1, 1),
            coverage_through=date(2025, 3, 31),
            filed_at_utc=datetime.now(timezone.utc),
            fec_url=f"https://ex.com/{fid}.fec",
            total_receipts=1000.0,
            total_disbursements=500.0,
            threshold_flag=False,
            raw_meta={},
        )
        session.commit()

    def test_build_includes_skipped_section(self, session):
        from app.email_service import build_sa_sb_attention_email
        self._make_filing(session, 1001, name="Skipped PAC")
        subject, body = build_sa_sb_attention_email(
            session, mode="big",
            skipped_filing_ids=[1001],
            streamed_filing_ids=[],
            failed_filing_ids=[],
        )
        assert "1 skipped" in subject
        assert "Skipped PAC" in body
        assert "Skipped: file too large" in body
        assert "Streamed:" not in body  # empty section omitted

    def test_build_includes_streamed_section(self, session):
        from app.email_service import build_sa_sb_attention_email
        self._make_filing(session, 2001, name="Streamed PAC")
        subject, body = build_sa_sb_attention_email(
            session, mode="big",
            skipped_filing_ids=[],
            streamed_filing_ids=[2001],
            failed_filing_ids=[],
        )
        assert "1 streamed" in subject
        assert "Streamed PAC" in body
        assert "degraded accuracy" in body

    def test_build_includes_failed_section(self, session):
        from app.email_service import build_sa_sb_attention_email
        self._make_filing(session, 3001, name="Failed PAC")
        subject, body = build_sa_sb_attention_email(
            session, mode="small",
            skipped_filing_ids=[],
            streamed_filing_ids=[],
            failed_filing_ids=[3001],
        )
        assert "1 failed" in subject
        assert "Failed PAC" in body

    def test_no_recipients_returns_empty(self, session):
        from app.email_service import send_sa_sb_attention_alert
        # No recipients added
        result = send_sa_sb_attention_alert(
            session, mode="big",
            skipped_filing_ids=[1],
            streamed_filing_ids=[],
            failed_filing_ids=[],
        )
        assert result == {}

    def test_no_events_returns_empty_without_sending(self, session):
        from app.email_service import send_sa_sb_attention_alert
        _add_recipient(session, "a@test.com")
        # All three lists empty → no work to do
        result = send_sa_sb_attention_alert(
            session, mode="big",
            skipped_filing_ids=[],
            streamed_filing_ids=[],
            failed_filing_ids=[],
        )
        assert result == {}

    @patch("app.email_service.send_email", return_value=True)
    def test_send_to_all_recipients(self, mock_send, session):
        from app.email_service import send_sa_sb_attention_alert
        self._make_filing(session, 4001)
        _add_recipient(session, "a@test.com")
        _add_recipient(session, "b@test.com")
        result = send_sa_sb_attention_alert(
            session, mode="big",
            skipped_filing_ids=[4001],
            streamed_filing_ids=[],
            failed_filing_ids=[],
        )
        assert result == {"a@test.com": 1, "b@test.com": 1}
        # Per-recipient send (no committee filtering for ops alerts)
        assert mock_send.call_count == 2
