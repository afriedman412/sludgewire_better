"""Tests for the SA/SB worker (app.ingest_sa_sb).

Covers:
- Atomic claim filtering by worker mode (small vs big)
- Priority ordering
- HEAD-and-handoff flow (small worker discovers >= 50MB file)
- Hard cap enforcement (skip + record reason)
- fecfile happy path (SA + SB rows persisted)
- Streaming path for >= 300MB files
- Failure path (parse errors → status='failed')
- Filing missing from filings_f3x → status='failed'
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import patch

import pytest
from sqlmodel import Session

from app.ingest_sa_sb import (
    BIG_FECFILE_MAX_MB,
    HARD_SIZE_CAP_MB,
    SMALL_WORKER_MAX_MB,
    _claim_next,
    run_sa_sb,
)
from app.repo import enqueue_sa_sb_task, upsert_f3x
from app.schemas import FilingF3X, IngestionTask, ScheduleA, ScheduleB


# -------------------------------------------------------
# helpers
# -------------------------------------------------------

def _make_filing(
    session: Session,
    filing_id: int,
    *,
    committee_id: str = "C00000001",
    committee_name: str = "Test PAC",
    fec_url: str | None = None,
) -> FilingF3X:
    upsert_f3x(
        session,
        filing_id=filing_id,
        committee_id=committee_id,
        committee_name=committee_name,
        form_type="F3XN",
        report_type="Q1",
        coverage_from=date(2025, 1, 1),
        coverage_through=date(2025, 3, 31),
        filed_at_utc=datetime.now(timezone.utc),
        fec_url=fec_url or f"https://ex.com/{filing_id}.fec",
        total_receipts=100_000.0,
        total_disbursements=50_000.0,
        threshold_flag=True,
        raw_meta={"FormType": "F3XN"},
    )
    session.commit()
    return session.get(FilingF3X, filing_id)


def _enqueue(
    session: Session,
    filing_id: int,
    *,
    file_size_mb: float | None = None,
    priority: int | None = None,
) -> None:
    enqueue_sa_sb_task(
        session, filing_id, fec_url=f"https://ex.com/{filing_id}.fec",
        priority=priority,
    )
    session.commit()
    if file_size_mb is not None:
        task = session.get(IngestionTask, (filing_id, "SA_SB"))
        task.file_size_mb = file_size_mb
        session.add(task)
        session.commit()


def _fake_parsed_with_items(
    sa_count: int = 0, sb_count: int = 0
) -> dict:
    """Build a fake fecfile parsed dict with N SA/SB items each."""
    sa_items = [
        {
            "contributor_organization_name": f"DONOR {i}",
            "contribution_amount": "100.00",
            "contribution_date": "2024-02-15",
            "entity_type": "ORG",
        }
        for i in range(sa_count)
    ]
    sb_items = [
        {
            "payee_organization_name": f"VENDOR {i}",
            "expenditure_amount": "200.00",
            "expenditure_date": "2024-03-10",
            "entity_type": "ORG",
        }
        for i in range(sb_count)
    ]
    return {
        "filing": {},
        "itemizations": {
            "Schedule A": sa_items,
            "Schedule B": sb_items,
        },
    }


# -------------------------------------------------------
# _claim_next — atomic claim filtering
# -------------------------------------------------------

class TestClaimNext:
    def test_small_claims_unknown_size_row(self, session):
        _make_filing(session, 100)
        _enqueue(session, 100, file_size_mb=None)
        task = _claim_next(session, mode="small")
        assert task is not None
        assert task["filing_id"] == 100

    def test_small_claims_under_threshold(self, session):
        _make_filing(session, 101)
        _enqueue(session, 101, file_size_mb=20.0)
        task = _claim_next(session, mode="small")
        assert task is not None
        assert task["filing_id"] == 101

    def test_small_skips_over_threshold(self, session):
        _make_filing(session, 102)
        _enqueue(session, 102, file_size_mb=200.0)
        assert _claim_next(session, mode="small") is None

    def test_big_skips_unknown_and_under_threshold(self, session):
        _make_filing(session, 103)
        _make_filing(session, 104)
        _enqueue(session, 103, file_size_mb=None)
        _enqueue(session, 104, file_size_mb=20.0)
        assert _claim_next(session, mode="big") is None

    def test_big_claims_over_threshold(self, session):
        _make_filing(session, 105)
        _enqueue(session, 105, file_size_mb=200.0)
        task = _claim_next(session, mode="big")
        assert task is not None
        assert task["filing_id"] == 105

    def test_priority_ordering(self, session):
        # filing_id ordering by default would prefer 200 over 100,
        # but explicit priority bump on 100 should win.
        _make_filing(session, 100)
        _make_filing(session, 200)
        _enqueue(session, 100, file_size_mb=10.0, priority=999_999)
        _enqueue(session, 200, file_size_mb=10.0)  # priority=200
        task = _claim_next(session, mode="small")
        assert task["filing_id"] == 100

    def test_claim_transitions_status_to_downloading(self, session):
        _make_filing(session, 100)
        _enqueue(session, 100, file_size_mb=10.0)
        _claim_next(session, mode="small")
        row = session.get(IngestionTask, (100, "SA_SB"))
        assert row.status == "downloading"

    def test_returns_none_when_queue_empty(self, session):
        assert _claim_next(session, mode="small") is None
        assert _claim_next(session, mode="big") is None


# -------------------------------------------------------
# run_sa_sb — full worker flow
# -------------------------------------------------------

class TestRunSaSbSmall:
    def test_processes_small_filing_with_fecfile(self, session):
        _make_filing(session, 100)
        _enqueue(session, 100, file_size_mb=10.0)
        # file_size_mb is already known and < 50, so no HEAD needed
        with patch(
            "app.ingest_sa_sb.download_fec_text",
            return_value="<fake_text>",
        ), patch(
            "app.ingest_sa_sb.parse_fec_filing",
            return_value=_fake_parsed_with_items(sa_count=3, sb_count=2),
        ):
            result = run_sa_sb(session, mode="small", max_filings=10)

        assert result.filings_processed == 1
        assert result.sa_inserted == 3
        assert result.sb_inserted == 2
        # Verify rows actually landed in the DB
        sa_rows = session.exec(
            ScheduleA.__table__.select().where(
                ScheduleA.__table__.c.filing_id == 100)
        ).all()
        sb_rows = session.exec(
            ScheduleB.__table__.select().where(
                ScheduleB.__table__.c.filing_id == 100)
        ).all()
        assert len(sa_rows) == 3
        assert len(sb_rows) == 2
        # Status should be ingested
        task = session.get(IngestionTask, (100, "SA_SB"))
        assert task.status == "ingested"

    def test_hands_off_oversized_file_to_big_worker(self, session):
        _make_filing(session, 100)
        _enqueue(session, 100, file_size_mb=None)
        # HEAD says it's 200MB → small worker hands off
        with patch(
            "app.ingest_sa_sb.check_file_size", return_value=200.0
        ):
            result = run_sa_sb(session, mode="small", max_filings=10)

        assert result.handed_off_count == 1
        assert result.filings_processed == 0
        task = session.get(IngestionTask, (100, "SA_SB"))
        assert task.status == "claimed"  # released back
        assert task.file_size_mb == 200.0  # size now persisted

    def test_handed_off_row_picked_up_by_big_worker(self, session):
        _make_filing(session, 100)
        _enqueue(session, 100, file_size_mb=None)
        # First: small worker discovers it's big and hands off
        with patch(
            "app.ingest_sa_sb.check_file_size", return_value=200.0
        ):
            run_sa_sb(session, mode="small", max_filings=10)
        # Then: big worker should pick it up
        with patch(
            "app.ingest_sa_sb.download_fec_text",
            return_value="<fake_text>",
        ), patch(
            "app.ingest_sa_sb.parse_fec_filing",
            return_value=_fake_parsed_with_items(sa_count=1, sb_count=1),
        ):
            result = run_sa_sb(session, mode="big", max_filings=10)

        assert result.filings_processed == 1
        task = session.get(IngestionTask, (100, "SA_SB"))
        assert task.status == "ingested"

    def test_small_worker_persists_size_when_under_threshold(self, session):
        _make_filing(session, 100)
        _enqueue(session, 100, file_size_mb=None)
        with patch(
            "app.ingest_sa_sb.check_file_size", return_value=12.5
        ), patch(
            "app.ingest_sa_sb.download_fec_text",
            return_value="<fake>",
        ), patch(
            "app.ingest_sa_sb.parse_fec_filing",
            return_value=_fake_parsed_with_items(),
        ):
            run_sa_sb(session, mode="small", max_filings=10)
        task = session.get(IngestionTask, (100, "SA_SB"))
        assert task.file_size_mb == 12.5
        assert task.status == "ingested"

    def test_stops_when_queue_empty(self, session):
        # No filings enqueued.
        result = run_sa_sb(session, mode="small", max_filings=10)
        assert result.filings_processed == 0

    def test_respects_max_filings(self, session):
        for fid in (100, 101, 102):
            _make_filing(session, fid)
            _enqueue(session, fid, file_size_mb=10.0)
        with patch(
            "app.ingest_sa_sb.download_fec_text", return_value="<fake>",
        ), patch(
            "app.ingest_sa_sb.parse_fec_filing",
            return_value=_fake_parsed_with_items(),
        ):
            result = run_sa_sb(session, mode="small", max_filings=2)
        assert result.filings_processed == 2


class TestRunSaSbBig:
    def test_uses_streaming_for_giant_file(self, session):
        _make_filing(session, 100)
        _enqueue(session, 100, file_size_mb=400.0)  # >= BIG_FECFILE_MAX_MB
        fake_stream = [
            ("SA", "raw|sa|line", {
                "contributor_name": "DONOR X",
                "contributor_employer": None,
                "contributor_occupation": None,
                "contribution_amount": 100.0,
                "contribution_date": date(2024, 2, 15),
                "receipt_description": None,
                "contributor_type": None,
                "memo_text": None,
            }),
            ("SB", "raw|sb|line", {
                "payee_name": "VENDOR Y",
                "payee_employer": None,
                "payee_occupation": None,
                "disbursement_amount": 250.0,
                "disbursement_date": date(2024, 3, 10),
                "disbursement_description": None,
                "payee_type": None,
                "purpose": None,
                "category_code": None,
                "memo_text": None,
                "beneficiary_candidate_id": None,
                "beneficiary_candidate_name": None,
            }),
        ]
        with patch(
            "app.ingest_sa_sb.stream_sa_sb_from_url",
            return_value=iter(fake_stream),
        ):
            result = run_sa_sb(session, mode="big", max_filings=10)

        assert result.filings_processed == 1
        assert result.streamed_count == 1
        assert 100 in result.streamed_filing_ids
        assert result.sa_inserted == 1
        assert result.sb_inserted == 1
        task = session.get(IngestionTask, (100, "SA_SB"))
        assert task.status == "ingested"

    def test_skips_above_hard_cap(self, session):
        _make_filing(session, 100)
        _enqueue(session, 100, file_size_mb=HARD_SIZE_CAP_MB + 100)
        result = run_sa_sb(session, mode="big", max_filings=10)
        assert result.skipped_too_large_count == 1
        assert 100 in result.skipped_filing_ids
        task = session.get(IngestionTask, (100, "SA_SB"))
        assert task.status == "skipped"
        assert task.skip_reason == "too_large"

    def test_records_failure_on_parse_error(self, session):
        _make_filing(session, 100)
        _enqueue(session, 100, file_size_mb=100.0)
        with patch(
            "app.ingest_sa_sb.download_fec_text",
            return_value="<fake>",
        ), patch(
            "app.ingest_sa_sb.parse_fec_filing",
            side_effect=RuntimeError("malformed file"),
        ):
            result = run_sa_sb(session, mode="big", max_filings=10)

        assert result.failed_count == 1
        assert 100 in result.failed_filing_ids
        task = session.get(IngestionTask, (100, "SA_SB"))
        assert task.status == "failed"
        assert "malformed" in task.error_message

    def test_records_failure_when_filing_missing(self, session):
        # Enqueue a task whose filing_id doesn't exist in filings_f3x.
        _enqueue(session, 999, file_size_mb=100.0)
        result = run_sa_sb(session, mode="big", max_filings=10)
        assert result.failed_count == 1
        task = session.get(IngestionTask, (999, "SA_SB"))
        assert task.status == "failed"
        assert task.failed_step == "lookup"


class TestModeValidation:
    def test_unknown_mode_raises(self, session):
        with pytest.raises(ValueError):
            _claim_next(session, mode="banana")
