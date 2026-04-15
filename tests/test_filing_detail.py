"""Tests for the donor/recipient detail pages and dashboard wiring.

Uses FastAPI TestClient with dependency_overrides to inject the test
Postgres session, so we exercise the real route + template render
without touching the production engine.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from app.api import USER_BUMP_PRIORITY, app, get_session
from app.repo import enqueue_sa_sb_task, insert_sb_event, upsert_f3x
from app.schemas import (
    AppConfig,
    FilingF3X,
    IngestionTask,
    ScheduleA,
    ScheduleB,
)


@pytest.fixture
def client(session: Session):
    """TestClient with get_session overridden to use the test DB."""
    def override():
        yield session
    app.dependency_overrides[get_session] = override
    yield TestClient(app)
    app.dependency_overrides.clear()


def _make_filing(session: Session, fid: int = 12345) -> FilingF3X:
    upsert_f3x(
        session,
        filing_id=fid,
        committee_id="C00001",
        committee_name="Test PAC",
        form_type="F3XN",
        report_type="Q1",
        coverage_from=date(2025, 1, 1),
        coverage_through=date(2025, 3, 31),
        filed_at_utc=datetime(2025, 4, 1, 12, tzinfo=timezone.utc),
        fec_url=f"https://ex.com/{fid}.fec",
        total_receipts=100_000.0,
        total_disbursements=50_000.0,
        threshold_flag=True,
        raw_meta={},
    )
    session.commit()
    return session.get(FilingF3X, fid)


# -------------------------------------------------------
# /filing/{id}/donors
# -------------------------------------------------------

class TestDonorsRoute:
    def test_no_task_row_enqueues_at_user_priority(self, session, client):
        _make_filing(session, 100)
        # No SA_SB task exists yet
        r = client.get("/filing/100/donors")
        assert r.status_code == 200
        assert "Queued" in r.text or "Processing" in r.text
        task = session.get(IngestionTask, (100, "SA_SB"))
        assert task is not None
        assert task.priority == USER_BUMP_PRIORITY

    def test_in_flight_filing_bumps_priority(self, session, client):
        _make_filing(session, 100)
        enqueue_sa_sb_task(session, 100, fec_url="https://ex.com/100.fec")
        session.commit()
        # Status is "claimed" by default; visiting should bump priority.
        r = client.get("/filing/100/donors")
        assert r.status_code == 200
        assert "Processing" in r.text
        task = session.get(IngestionTask, (100, "SA_SB"))
        assert task.priority == USER_BUMP_PRIORITY

    def test_ingested_filing_renders_donor_rows(self, session, client):
        filing = _make_filing(session, 100)
        enqueue_sa_sb_task(session, 100, fec_url=filing.fec_url)
        task = session.get(IngestionTask, (100, "SA_SB"))
        task.status = "ingested"
        session.add(task)
        # Insert a couple of SA rows
        for i, name in enumerate(["DONOR A", "DONOR B"]):
            session.add(ScheduleA(
                event_id=f"sa-{i}",
                filing_id=100,
                committee_id="C00001",
                contributor_name=name,
                contribution_amount=1000.0 * (i + 1),
                contribution_date=date(2024, 2, 15),
                fec_url=filing.fec_url,
                raw_line=f"SA|{name}|...",
            ))
        session.commit()

        r = client.get("/filing/100/donors")
        assert r.status_code == 200
        assert "DONOR A" in r.text
        assert "DONOR B" in r.text
        assert "$1,000.00" in r.text
        assert "$2,000.00" in r.text
        # Should NOT show the loading banner
        assert "auto-refreshes" not in r.text

    def test_skipped_filing_shows_skipped_banner(self, session, client):
        filing = _make_filing(session, 100)
        enqueue_sa_sb_task(session, 100, fec_url=filing.fec_url)
        task = session.get(IngestionTask, (100, "SA_SB"))
        task.status = "skipped"
        task.skip_reason = "too_large"
        task.file_size_mb = 400.0
        session.add(task)
        session.commit()

        r = client.get("/filing/100/donors")
        assert r.status_code == 200
        assert "Skipped" in r.text
        assert "400.0 MB" in r.text

    def test_failed_filing_shows_error(self, session, client):
        filing = _make_filing(session, 100)
        enqueue_sa_sb_task(session, 100, fec_url=filing.fec_url)
        task = session.get(IngestionTask, (100, "SA_SB"))
        task.status = "failed"
        task.error_message = "malformed pipe row at line 47"
        session.add(task)
        session.commit()

        r = client.get("/filing/100/donors")
        assert r.status_code == 200
        assert "Failed" in r.text
        assert "malformed" in r.text

    def test_ingested_no_rows_shows_empty_message(self, session, client):
        filing = _make_filing(session, 100)
        enqueue_sa_sb_task(session, 100, fec_url=filing.fec_url)
        task = session.get(IngestionTask, (100, "SA_SB"))
        task.status = "ingested"
        session.add(task)
        session.commit()
        # No SA rows inserted
        r = client.get("/filing/100/donors")
        assert r.status_code == 200
        assert "No donors found" in r.text


# -------------------------------------------------------
# /filing/{id}/recipients
# -------------------------------------------------------

class TestRecipientsRoute:
    def test_ingested_renders_sb_rows(self, session, client):
        filing = _make_filing(session, 200)
        enqueue_sa_sb_task(session, 200, fec_url=filing.fec_url)
        task = session.get(IngestionTask, (200, "SA_SB"))
        task.status = "ingested"
        session.add(task)
        # Use insert_sb_event so dedupe semantics are exercised
        insert_sb_event(session, ScheduleB(
            event_id="sb-1",
            filing_id=200,
            committee_id="C00001",
            payee_name="VENDOR X",
            disbursement_amount=2500.00,
            disbursement_date=date(2024, 3, 10),
            fec_url=filing.fec_url,
            raw_line="SB|VENDOR X|...",
        ))
        session.commit()

        r = client.get("/filing/200/recipients")
        assert r.status_code == 200
        assert "VENDOR X" in r.text
        assert "$2,500.00" in r.text

    def test_beneficiary_link_rendered(self, session, client):
        filing = _make_filing(session, 200)
        enqueue_sa_sb_task(session, 200, fec_url=filing.fec_url)
        task = session.get(IngestionTask, (200, "SA_SB"))
        task.status = "ingested"
        session.add(task)
        insert_sb_event(session, ScheduleB(
            event_id="sb-2",
            filing_id=200,
            committee_id="C00001",
            payee_name="ATTACK ADS LLC",
            disbursement_amount=500.0,
            disbursement_date=date(2024, 4, 1),
            beneficiary_candidate_id="H0NY12345",
            beneficiary_candidate_name="Pat Smith",
            fec_url=filing.fec_url,
            raw_line="SB|...",
        ))
        session.commit()
        r = client.get("/filing/200/recipients")
        assert "H0NY12345" in r.text
        assert "Pat Smith" in r.text


# -------------------------------------------------------
# Dashboard rendering — clickable amounts + LOADING badges
# -------------------------------------------------------

class TestDashboardWiring:
    def _make_today(self, session: Session, fid: int) -> FilingF3X:
        # Use a "today" filed_at_utc so /dashboard/3x picks it up.
        from app.api import et_today_utc_bounds
        start, _ = et_today_utc_bounds()
        upsert_f3x(
            session,
            filing_id=fid,
            committee_id="C00001",
            committee_name="Today PAC",
            form_type="F3XN",
            report_type="Q1",
            coverage_from=date(2025, 1, 1),
            coverage_through=date(2025, 3, 31),
            filed_at_utc=start.replace(hour=14),
            fec_url=f"https://ex.com/{fid}.fec",
            total_receipts=75_000.0,
            total_disbursements=20_000.0,
            threshold_flag=True,
            raw_meta={},
        )
        session.commit()
        return session.get(FilingF3X, fid)

    def test_ingested_filing_amounts_are_clickable(self, session, client):
        f = self._make_today(session, 1001)
        enqueue_sa_sb_task(session, 1001, fec_url=f.fec_url)
        task = session.get(IngestionTask, (1001, "SA_SB"))
        task.status = "ingested"
        session.add(task)
        session.commit()

        r = client.get("/dashboard/3x?threshold=0")
        assert r.status_code == 200
        assert '/filing/1001/donors' in r.text
        assert '/filing/1001/recipients' in r.text

    def test_in_flight_filing_shows_loading_badge(self, session, client):
        f = self._make_today(session, 1002)
        enqueue_sa_sb_task(session, 1002, fec_url=f.fec_url)
        # Status is "claimed" → should render LOADING badge
        session.commit()

        r = client.get("/dashboard/3x?threshold=0")
        assert r.status_code == 200
        assert "LOADING" in r.text

    def test_no_task_row_shows_plain_amount(self, session, client):
        self._make_today(session, 1003)
        # No SA_SB task enqueued at all
        r = client.get("/dashboard/3x?threshold=0")
        assert r.status_code == 200
        # Amount visible but not as a link to /filing/.../donors
        assert '/filing/1003/donors' not in r.text
        assert "$75,000.00" in r.text
