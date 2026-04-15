"""Tests for filing ingestion: claim/dedup, status tracking, success & failure paths."""
from __future__ import annotations

from datetime import datetime, date, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest
from sqlmodel import Session

from app.schemas import IngestionTask, FilingF3X, IEScheduleE, AppConfig, ScheduleB
from app.repo import (
    claim_filing,
    update_filing_status,
    upsert_f3x,
    insert_ie_event,
    insert_sb_event,
    enqueue_sa_sb_task,
    record_skipped_filing,
    mark_tasks_emailed,
    reset_failed_tasks,
    get_max_new_per_run,
    get_email_enabled,
)
from tests.conftest import make_rss_item


# -------------------------------------------------------
# claim_filing
# -------------------------------------------------------

class TestClaimFiling:
    def test_first_claim_succeeds(self, session):
        assert claim_filing(session, 100, "F3X") is True
        row = session.get(IngestionTask, (100, "F3X"))
        assert row is not None
        assert row.status == "claimed"

    def test_duplicate_claim_returns_false(self, session):
        claim_filing(session, 100, "F3X")
        assert claim_filing(session, 100, "F3X") is False

    def test_same_filing_different_feed_both_succeed(self, session):
        assert claim_filing(session, 100, "F3X") is True
        assert claim_filing(session, 100, "IE-24H") is True

    def test_claim_does_not_reset_existing_status(self, session):
        """Once a filing is ingested, re-claiming should return False (not reset status)."""
        claim_filing(session, 100, "F3X")
        update_filing_status(session, 100, "F3X", "ingested")
        session.commit()

        assert claim_filing(session, 100, "F3X") is False
        row = session.get(IngestionTask, (100, "F3X"))
        assert row.status == "ingested"


# -------------------------------------------------------
# update_filing_status
# -------------------------------------------------------

class TestUpdateFilingStatus:
    def test_updates_claimed_to_ingested(self, session):
        claim_filing(session, 200, "F3X")
        update_filing_status(session, 200, "F3X", "ingested")
        session.flush()
        row = session.get(IngestionTask, (200, "F3X"))
        assert row.status == "ingested"

    def test_updates_claimed_to_failed(self, session):
        claim_filing(session, 200, "F3X")
        update_filing_status(session, 200, "F3X", "failed")
        session.flush()
        row = session.get(IngestionTask, (200, "F3X"))
        assert row.status == "failed"

    def test_updates_claimed_to_skipped(self, session):
        claim_filing(session, 200, "F3X")
        update_filing_status(session, 200, "F3X", "skipped")
        session.flush()
        row = session.get(IngestionTask, (200, "F3X"))
        assert row.status == "skipped"

    def test_noop_for_missing_filing(self, session):
        """Updating a non-existent filing should be a no-op, not crash."""
        update_filing_status(session, 999, "F3X", "ingested")  # no error

    def test_records_failed_step(self, session):
        claim_filing(session, 200, "F3X")
        update_filing_status(
            session, 200, "F3X", "failed",
            failed_step="downloading", error_message="Connection refused",
        )
        session.flush()
        row = session.get(IngestionTask, (200, "F3X"))
        assert row.status == "failed"
        assert row.failed_step == "downloading"
        assert row.error_message == "Connection refused"

    def test_substep_transitions(self, session):
        """Verify the full pipeline status progression."""
        claim_filing(session, 200, "F3X")
        for status in ("downloading", "downloaded", "parsing", "ingested"):
            update_filing_status(session, 200, "F3X", status)
            session.flush()
            row = session.get(IngestionTask, (200, "F3X"))
            assert row.status == status

    def test_updates_updated_at(self, session):
        claim_filing(session, 200, "F3X")
        session.flush()
        row = session.get(IngestionTask, (200, "F3X"))
        original_updated = row.updated_at.replace(tzinfo=None)

        update_filing_status(session, 200, "F3X", "downloading")
        session.flush()
        session.expire(row)
        row = session.get(IngestionTask, (200, "F3X"))
        assert row.updated_at.replace(tzinfo=None) >= original_updated


# -------------------------------------------------------
# record_skipped_filing
# -------------------------------------------------------

class TestRecordSkippedFiling:
    def test_records_skipped(self, session):
        claim_filing(session, 500, "F3X")
        record_skipped_filing(
            session, 500, "F3X", "too_large",
            file_size_mb=120.5, fec_url="https://example.com/500.fec",
        )
        session.commit()
        row = session.get(IngestionTask, (500, "F3X"))
        assert row is not None
        assert row.skip_reason == "too_large"
        assert row.file_size_mb == 120.5
        assert row.fec_url == "https://example.com/500.fec"

    def test_skipped_noop_without_claim(self, session):
        """record_skipped_filing is a no-op if the task doesn't exist."""
        record_skipped_filing(session, 500, "F3X", "too_large")
        session.commit()
        row = session.get(IngestionTask, (500, "F3X"))
        assert row is None


# -------------------------------------------------------
# mark_tasks_emailed
# -------------------------------------------------------

class TestMarkTasksEmailed:
    def test_marks_with_source_feed(self, session):
        claim_filing(session, 600, "F3X")
        update_filing_status(session, 600, "F3X", "ingested")
        session.flush()

        mark_tasks_emailed(session, [600], source_feed="F3X")
        session.flush()

        row = session.get(IngestionTask, (600, "F3X"))
        assert row.emailed_at is not None

    def test_marks_without_source_feed(self, session):
        claim_filing(session, 601, "http://feed-a")
        update_filing_status(session, 601, "http://feed-a", "ingested")
        session.flush()

        mark_tasks_emailed(session, [601])
        session.flush()

        row = session.get(IngestionTask, (601, "http://feed-a"))
        assert row.emailed_at is not None

    def test_skips_already_emailed(self, session):
        claim_filing(session, 602, "F3X")
        update_filing_status(session, 602, "F3X", "ingested")
        mark_tasks_emailed(session, [602], source_feed="F3X")
        session.flush()
        row = session.get(IngestionTask, (602, "F3X"))
        first_emailed = row.emailed_at

        # Second call without source_feed should skip already-emailed
        mark_tasks_emailed(session, [602])
        session.flush()
        row = session.get(IngestionTask, (602, "F3X"))
        assert row.emailed_at == first_emailed


# -------------------------------------------------------
# reset_failed_tasks
# -------------------------------------------------------

class TestResetFailedTasks:
    def test_resets_failed_to_claimed(self, session):
        claim_filing(session, 700, "F3X")
        update_filing_status(
            session, 700, "F3X", "failed",
            failed_step="parsing", error_message="bad data",
        )
        session.flush()

        count = reset_failed_tasks(session)
        session.flush()

        assert count == 1
        row = session.get(IngestionTask, (700, "F3X"))
        assert row.status == "claimed"
        assert row.failed_step is None
        assert row.error_message is None

    def test_resets_only_specified_ids(self, session):
        claim_filing(session, 701, "F3X")
        claim_filing(session, 702, "F3X")
        update_filing_status(session, 701, "F3X", "failed")
        update_filing_status(session, 702, "F3X", "failed")
        session.flush()

        count = reset_failed_tasks(session, filing_ids=[701])
        session.flush()

        assert count == 1
        assert session.get(IngestionTask, (701, "F3X")).status == "claimed"
        assert session.get(IngestionTask, (702, "F3X")).status == "failed"

    def test_does_not_reset_non_failed(self, session):
        claim_filing(session, 703, "F3X")
        update_filing_status(session, 703, "F3X", "ingested")
        session.flush()

        count = reset_failed_tasks(session)
        assert count == 0
        assert session.get(IngestionTask, (703, "F3X")).status == "ingested"


# -------------------------------------------------------
# upsert_f3x
# -------------------------------------------------------

class TestUpsertF3X:
    def _upsert(self, session, filing_id=300, total_receipts=100_000.0, **overrides):
        defaults = dict(
            filing_id=filing_id,
            committee_id="C00000001",
            committee_name="Test PAC",
            form_type="F3XN",
            report_type="Q1",
            coverage_from=date(2025, 1, 1),
            coverage_through=date(2025, 3, 31),
            filed_at_utc=datetime.now(timezone.utc),
            fec_url=f"https://example.com/{filing_id}.fec",
            total_receipts=total_receipts,
            total_disbursements=None,
            threshold_flag=total_receipts is not None and total_receipts >= 50_000,
            raw_meta={"FormType": "F3XN"},
        )
        defaults.update(overrides)
        upsert_f3x(session, **defaults)
        session.commit()

    def test_insert_new(self, session):
        self._upsert(session)
        row = session.get(FilingF3X, 300)
        assert row is not None
        assert row.committee_name == "Test PAC"
        assert row.total_receipts == 100_000.0

    def test_update_existing(self, session):
        self._upsert(session, total_receipts=100_000.0)
        self._upsert(session, total_receipts=200_000.0, committee_name="Updated PAC")
        row = session.get(FilingF3X, 300)
        assert row.total_receipts == 200_000.0
        assert row.committee_name == "Updated PAC"

    def test_null_receipts(self, session):
        self._upsert(session, total_receipts=None, threshold_flag=False)
        row = session.get(FilingF3X, 300)
        assert row.total_receipts is None
        assert row.threshold_flag is False

    def test_total_disbursements_persisted(self, session):
        self._upsert(session, total_disbursements=42_000.0)
        row = session.get(FilingF3X, 300)
        assert row.total_disbursements == 42_000.0

    def test_total_disbursements_null_by_default(self, session):
        self._upsert(session)
        row = session.get(FilingF3X, 300)
        assert row.total_disbursements is None


# -------------------------------------------------------
# insert_ie_event
# -------------------------------------------------------

class TestInsertIEEvent:
    def _make_event(self, event_id="evt-1", filing_id=400, amount=5000.0):
        return IEScheduleE(
            event_id=event_id,
            filing_id=filing_id,
            filer_id="C00000002",
            committee_id="C00000002",
            committee_name="Test Super PAC",
            fec_url="https://example.com/400.fec",
            raw_line="SE|C00000002|...",
            amount=amount,
        )

    def test_insert_new_event(self, session):
        assert insert_ie_event(session, self._make_event()) is True
        row = session.get(IEScheduleE, "evt-1")
        assert row is not None
        assert row.amount == 5000.0

    def test_duplicate_event_returns_false(self, session):
        insert_ie_event(session, self._make_event())
        assert insert_ie_event(session, self._make_event()) is False


# -------------------------------------------------------
# Config helpers
# -------------------------------------------------------

class TestConfigHelpers:
    def test_max_new_default(self, session):
        assert get_max_new_per_run(session) == 50

    def test_max_new_from_db(self, session):
        session.add(AppConfig(key="max_new_per_run", value="25"))
        session.commit()
        assert get_max_new_per_run(session) == 25

    def test_email_enabled_default_true(self, session):
        assert get_email_enabled(session) is True

    def test_email_enabled_false(self, session):
        session.add(AppConfig(key="email_enabled", value="false"))
        session.commit()
        assert get_email_enabled(session) is False


# -------------------------------------------------------
# run_f3x integration (mocked network)
# -------------------------------------------------------

class TestRunF3XIntegration:
    """Test the full run_f3x flow with mocked downloads and parsing."""

    FAKE_FEC_TEXT = "HDR|FEC|8.3\nF3XN|Test PAC|C00000001|0|0|0|75000.00|rest...\n"

    @patch("app.ingest_f3x.resolve_committee_name", return_value="Test PAC")
    @patch("app.ingest_f3x.download_fec_header")
    @patch("app.ingest_f3x.fetch_rss_items")
    def test_successful_ingestion(self, mock_fetch, mock_download, mock_resolve, session):
        now = datetime.now(timezone.utc)
        mock_fetch.return_value = [make_rss_item(filing_id=1001, pub_date=now)]
        mock_download.return_value = self.FAKE_FEC_TEXT

        from app.ingest_f3x import run_f3x
        result = run_f3x(session, feed_url="http://fake", receipts_threshold=50_000)

        assert result.new_count == 1
        assert result.failed_count == 0
        # Verify ingestion_tasks status
        task = session.get(IngestionTask, (1001, "F3X"))
        assert task is not None
        assert task.status == "ingested"
        # Verify F3X row was created
        f3x = session.get(FilingF3X, 1001)
        assert f3x is not None

    @patch("app.ingest_f3x.resolve_committee_name", return_value="Test PAC")
    @patch("app.ingest_f3x.download_fec_header", side_effect=Exception("Network error"))
    @patch("app.ingest_f3x.fetch_rss_items")
    def test_download_failure_marks_failed(self, mock_fetch, mock_download, mock_resolve, session):
        """Generic download errors are now caught and recorded as failed."""
        now = datetime.now(timezone.utc)
        mock_fetch.return_value = [make_rss_item(filing_id=1002, pub_date=now)]

        from app.ingest_f3x import run_f3x
        result = run_f3x(session, feed_url="http://fake", receipts_threshold=50_000)

        assert result.new_count == 0
        assert result.failed_count == 1
        assert "Network error" in result.last_error
        task = session.get(IngestionTask, (1002, "F3X"))
        assert task is not None
        assert task.status == "failed"
        assert task.failed_step == "downloading"
        assert "Network error" in task.error_message

    @patch("app.ingest_f3x.fetch_rss_items")
    def test_duplicate_filing_skipped(self, mock_fetch, session):
        now = datetime.now(timezone.utc)
        mock_fetch.return_value = [make_rss_item(filing_id=1003, pub_date=now)]
        # Pre-claim the filing
        claim_filing(session, 1003, "F3X")
        session.commit()

        from app.ingest_f3x import run_f3x
        result = run_f3x(session, feed_url="http://fake", receipts_threshold=50_000)

        assert result.new_count == 0  # nothing new processed

    @patch("app.ingest_f3x.fetch_rss_items")
    def test_stops_at_yesterday(self, mock_fetch, session):
        """Filings from before today should be ignored."""
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        mock_fetch.return_value = [make_rss_item(filing_id=1004, pub_date=yesterday)]

        from app.ingest_f3x import run_f3x
        result = run_f3x(session, feed_url="http://fake", receipts_threshold=50_000)

        assert result.new_count == 0
        assert session.get(IngestionTask, (1004, "F3X")) is None

    @patch("app.ingest_f3x.download_fec_header", side_effect=Exception("Connection reset"))
    @patch("app.ingest_f3x.fetch_rss_items")
    def test_download_error_tracked_in_result(self, mock_fetch, mock_download, session):
        """Download errors are tracked in the returned result."""
        now = datetime.now(timezone.utc)
        mock_fetch.return_value = [make_rss_item(filing_id=1005, pub_date=now)]

        from app.ingest_f3x import run_f3x
        result = run_f3x(session, feed_url="http://fake", receipts_threshold=50_000)

        assert result.new_count == 0
        assert result.failed_count == 1
        task = session.get(IngestionTask, (1005, "F3X"))
        assert task is not None
        assert task.status == "failed"
        assert task.failed_step == "downloading"

    @patch("app.ingest_f3x.resolve_committee_name", return_value="Test PAC")
    @patch("app.ingest_f3x.download_fec_header")
    @patch("app.ingest_f3x.fetch_rss_items")
    def test_respects_max_per_run(self, mock_fetch, mock_download, mock_resolve, session):
        session.add(AppConfig(key="max_new_per_run", value="1"))
        session.commit()

        now = datetime.now(timezone.utc)
        mock_fetch.return_value = [
            make_rss_item(filing_id=2001, pub_date=now),
            make_rss_item(filing_id=2002, pub_date=now),
        ]
        mock_download.return_value = self.FAKE_FEC_TEXT

        from app.ingest_f3x import run_f3x
        result = run_f3x(session, feed_url="http://fake", receipts_threshold=50_000)

        assert result.new_count == 1
        # Second filing should not have been claimed
        assert session.get(IngestionTask, (2002, "F3X")) is None

    @patch("app.ingest_f3x.resolve_committee_name", return_value="Test PAC")
    @patch("app.ingest_f3x.parse_f3x_header_only", side_effect=ValueError("bad data"))
    @patch("app.ingest_f3x.download_fec_header")
    @patch("app.ingest_f3x.fetch_rss_items")
    def test_parse_failure_marks_failed(self, mock_fetch, mock_download, mock_parse, mock_resolve, session):
        """Parse errors are caught and recorded with failed_step='parsing'."""
        now = datetime.now(timezone.utc)
        mock_fetch.return_value = [make_rss_item(filing_id=1006, pub_date=now)]
        mock_download.return_value = self.FAKE_FEC_TEXT

        from app.ingest_f3x import run_f3x
        result = run_f3x(session, feed_url="http://fake", receipts_threshold=50_000)

        assert result.new_count == 0
        assert result.failed_count == 1
        task = session.get(IngestionTask, (1006, "F3X"))
        assert task.status == "failed"
        assert task.failed_step == "parsing"
        assert "bad data" in task.error_message


# -------------------------------------------------------
# run_ie_schedule_e integration (mocked network)
# -------------------------------------------------------

class TestRunIEIntegration:

    FAKE_IE_FEC_TEXT = (
        "HDR|FEC|8.3\n"
        "F24|C00000002|Committee Name|20250101|20250331\n"
        "SE|C00000002|20250215|5000.00|S|P00000001|Jane Doe||H|CA|12|DEM|G2026|TV Ad|Media Inc|line\n"
    )

    @patch("app.ingest_ie.resolve_committee_name", return_value="Test Super PAC")
    @patch("app.ingest_ie.parse_fec_filing")
    @patch("app.ingest_ie.download_fec_text")
    @patch("app.ingest_ie.fetch_rss_items")
    def test_successful_ie_ingestion(self, mock_fetch, mock_download, mock_parse, mock_resolve, session):
        now = datetime.now(timezone.utc)
        mock_fetch.return_value = [make_rss_item(filing_id=3001, committee_id="C00000002", form_type="F24", pub_date=now)]
        mock_download.return_value = self.FAKE_IE_FEC_TEXT
        # Return empty parsed result so it falls through to raw line parsing
        mock_parse.return_value = {"filing": {}, "itemizations": {}}

        from app.ingest_ie import run_ie_schedule_e
        filings, events = run_ie_schedule_e(session, feed_urls=["http://fake-ie"])

        assert filings == 1
        assert events >= 1
        task = session.get(IngestionTask, (3001, "http://fake-ie"))
        assert task is not None
        assert task.status == "ingested"

    @patch("app.ingest_ie.download_fec_text", side_effect=Exception("Timeout"))
    @patch("app.ingest_ie.fetch_rss_items")
    def test_ie_download_failure_marks_failed(self, mock_fetch, mock_download, session):
        """Generic download errors are now caught and recorded as failed."""
        now = datetime.now(timezone.utc)
        mock_fetch.return_value = [make_rss_item(filing_id=3002, form_type="F24", pub_date=now)]

        from app.ingest_ie import run_ie_schedule_e
        filings, events = run_ie_schedule_e(session, feed_urls=["http://fake-ie"])

        assert filings == 0
        task = session.get(IngestionTask, (3002, "http://fake-ie"))
        assert task is not None
        assert task.status == "failed"
        assert task.failed_step == "downloading"
        assert "Timeout" in task.error_message


# -------------------------------------------------------
# enqueue_sa_sb_task (queue for the SA/SB worker)
# -------------------------------------------------------

class TestEnqueueSaSbTask:
    def test_first_enqueue_inserts_claimed_row(self, session):
        enqueue_sa_sb_task(session, 9001, fec_url="https://ex.com/9001.fec")
        session.commit()
        row = session.get(IngestionTask, (9001, "SA_SB"))
        assert row is not None
        assert row.status == "claimed"
        assert row.fec_url == "https://ex.com/9001.fec"

    def test_default_priority_equals_filing_id(self, session):
        enqueue_sa_sb_task(session, 9001)
        session.commit()
        row = session.get(IngestionTask, (9001, "SA_SB"))
        assert row.priority == 9001

    def test_explicit_priority_overrides_default(self, session):
        enqueue_sa_sb_task(session, 9001, priority=500000)
        session.commit()
        row = session.get(IngestionTask, (9001, "SA_SB"))
        assert row.priority == 500000

    def test_reenqueue_with_higher_priority_bumps(self, session):
        enqueue_sa_sb_task(session, 9001)  # priority=9001
        session.commit()
        enqueue_sa_sb_task(session, 9001, priority=999999)
        session.commit()
        row = session.get(IngestionTask, (9001, "SA_SB"))
        assert row.priority == 999999

    def test_reenqueue_with_lower_priority_does_not_bump(self, session):
        enqueue_sa_sb_task(session, 9001, priority=999999)
        session.commit()
        enqueue_sa_sb_task(session, 9001, priority=100)
        session.commit()
        row = session.get(IngestionTask, (9001, "SA_SB"))
        assert row.priority == 999999

    def test_independent_of_f3x_task(self, session):
        # SA_SB enqueue should not conflict with an existing F3X task
        claim_filing(session, 9001, "F3X")
        update_filing_status(session, 9001, "F3X", "ingested")
        enqueue_sa_sb_task(session, 9001)
        session.commit()
        assert session.get(IngestionTask, (9001, "F3X")).status == "ingested"
        assert session.get(IngestionTask, (9001, "SA_SB")).status == "claimed"


# -------------------------------------------------------
# insert_sb_event (Schedule B dedupe)
# -------------------------------------------------------

class TestInsertSbEvent:
    def _make_sb(self, event_id="sb-1", filing_id=500):
        return ScheduleB(
            event_id=event_id,
            filing_id=filing_id,
            committee_id="C00000003",
            committee_name="PAC",
            payee_name="ACME CORP",
            disbursement_amount=1500.00,
            disbursement_date=date(2024, 3, 1),
            fec_url="https://ex.com/500.fec",
            raw_line="SB|C00000003|ORG|ACME...",
        )

    def test_insert_new(self, session):
        assert insert_sb_event(session, self._make_sb()) is True
        row = session.get(ScheduleB, "sb-1")
        assert row is not None
        assert row.disbursement_amount == 1500.00

    def test_duplicate_returns_false(self, session):
        insert_sb_event(session, self._make_sb())
        session.commit()
        assert insert_sb_event(session, self._make_sb()) is False
