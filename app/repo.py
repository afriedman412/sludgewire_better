from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Dict, Any

from sqlmodel import Session

import json

from .schemas import IngestionTask, FilingF3X, IEScheduleE, ScheduleA, ScheduleB, AppConfig, RaceCandidate


# Default values for configurable settings
DEFAULT_MAX_NEW_PER_RUN = 50


def get_config_int(session: Session, key: str, default: int) -> int:
    """Get an integer config value from AppConfig table."""
    config = session.get(AppConfig, key)
    if config and config.value:
        try:
            return int(config.value)
        except (ValueError, TypeError):
            pass
    return default


def get_max_new_per_run(session: Session) -> int:
    """Get the max filings to process per run from config."""
    return get_config_int(session, "max_new_per_run", DEFAULT_MAX_NEW_PER_RUN)


def get_email_enabled(session: Session) -> bool:
    """Check if email alerts are enabled."""
    config = session.get(AppConfig, "email_enabled")
    if config and config.value:
        return config.value.lower() in ("true", "1", "yes")
    return True  # Default to enabled


def get_ptr_email_enabled(session: Session) -> bool:
    """Check if PTR email alerts are enabled."""
    config = session.get(AppConfig, "ptr_email_enabled")
    if config and config.value:
        return config.value.lower() in ("true", "1", "yes")
    return False  # Default to disabled


def claim_filing(session: Session, filing_id: int, source_feed: str) -> bool:
    """
    Returns True if we successfully claimed this filing_id+source_feed (first time seen).
    Handles race conditions where another process already claimed it.
    """
    existing = session.get(IngestionTask, (filing_id, source_feed))
    if existing:
        return False

    try:
        with session.begin_nested():
            session.add(IngestionTask(
                filing_id=filing_id,
                source_feed=source_feed,
                status="claimed",
            ))
        return True
    except Exception:
        return False


def update_filing_status(
    session: Session,
    filing_id: int,
    source_feed: str,
    status: str,
    failed_step: str = None,
    error_message: str = None,
) -> None:
    """Update the status of an ingestion task, optionally recording failure details."""
    row = session.get(IngestionTask, (filing_id, source_feed))
    if row:
        row.status = status
        row.updated_at = datetime.now(timezone.utc)
        if failed_step is not None:
            row.failed_step = failed_step
        if error_message is not None:
            row.error_message = error_message
        session.add(row)


def record_skipped_filing(
    session: Session,
    filing_id: int,
    source_feed: str,
    reason: str,
    file_size_mb: float = None,
    fec_url: str = None,
) -> None:
    """Record skip metadata on the existing IngestionTask row."""
    row = session.get(IngestionTask, (filing_id, source_feed))
    if row:
        row.skip_reason = reason
        row.file_size_mb = file_size_mb
        row.fec_url = fec_url
        row.updated_at = datetime.now(timezone.utc)
        session.add(row)


def mark_tasks_emailed(
    session: Session,
    filing_ids: list[int],
    source_feed: str = None,
) -> None:
    """Set emailed_at on IngestionTask rows after alerts are sent.

    If source_feed is given, looks up by (filing_id, source_feed).
    If None, marks all tasks matching the filing_ids (for IE where
    the source feed URL varies).
    """
    from sqlmodel import select
    now = datetime.now(timezone.utc)
    if source_feed is not None:
        for fid in filing_ids:
            row = session.get(IngestionTask, (fid, source_feed))
            if row:
                row.emailed_at = now
                session.add(row)
    else:
        stmt = select(IngestionTask).where(
            IngestionTask.filing_id.in_(filing_ids),
            IngestionTask.emailed_at.is_(None),
        )
        for row in session.exec(stmt).all():
            row.emailed_at = now
            session.add(row)


def reset_failed_tasks(
    session: Session,
    filing_ids: list[int] = None,
) -> int:
    """Reset failed tasks back to 'claimed' for retry. Returns count reset."""
    from sqlmodel import select
    stmt = select(IngestionTask).where(
        IngestionTask.status == "failed"
    )
    if filing_ids is not None:
        stmt = stmt.where(IngestionTask.filing_id.in_(filing_ids))
    rows = session.exec(stmt).all()
    now = datetime.now(timezone.utc)
    for row in rows:
        row.status = "claimed"
        row.failed_step = None
        row.error_message = None
        row.updated_at = now
        session.add(row)
    return len(rows)


def upsert_f3x(
    session: Session,
    *,
    filing_id: int,
    committee_id: str,
    committee_name: Optional[str] = None,
    form_type: Optional[str],
    report_type: Optional[str],
    coverage_from,
    coverage_through,
    filed_at_utc,
    fec_url: str,
    total_receipts: Optional[float],
    total_disbursements: Optional[float],
    threshold_flag: bool,
    raw_meta: Optional[Dict[str, Any]],
) -> None:
    row = session.get(FilingF3X, filing_id)
    now = datetime.now(timezone.utc)

    if row is None:
        session.add(
            FilingF3X(
                filing_id=filing_id,
                committee_id=committee_id,
                committee_name=committee_name,
                form_type=form_type,
                report_type=report_type,
                coverage_from=coverage_from,
                coverage_through=coverage_through,
                filed_at_utc=filed_at_utc,
                fec_url=fec_url,
                total_receipts=total_receipts,
                total_disbursements=total_disbursements,
                threshold_flag=threshold_flag,
                raw_meta=raw_meta,
                updated_at_utc=now,
            )
        )
    else:
        row.committee_id = committee_id
        row.committee_name = committee_name
        row.form_type = form_type
        row.report_type = report_type
        row.coverage_from = coverage_from
        row.coverage_through = coverage_through
        row.filed_at_utc = filed_at_utc
        row.fec_url = fec_url
        row.total_receipts = total_receipts
        row.total_disbursements = total_disbursements
        row.threshold_flag = threshold_flag
        if raw_meta is not None:
            row.raw_meta = raw_meta
        row.updated_at_utc = now
        session.add(row)

    session.flush()


def enqueue_sa_sb_task(
    session: Session,
    filing_id: int,
    *,
    fec_url: Optional[str] = None,
    priority: Optional[int] = None,
) -> None:
    """Enqueue a filing for full-file SA/SB ingestion.

    Inserts an IngestionTask row with source_feed='SA_SB' if one does not
    already exist. If it exists and priority is higher than the stored
    value, bump it (used when a user accesses a detail page for a filing
    whose SA/SB ingestion is still pending).

    Default priority is the filing_id itself, so naturally newer filings
    are processed before older ones (FEC filing_ids are monotonic).
    """
    effective_priority = priority if priority is not None else filing_id
    existing = session.get(IngestionTask, (filing_id, "SA_SB"))
    if existing is None:
        try:
            with session.begin_nested():
                session.add(IngestionTask(
                    filing_id=filing_id,
                    source_feed="SA_SB",
                    status="claimed",
                    fec_url=fec_url,
                    priority=effective_priority,
                ))
        except Exception:
            pass
        return
    if existing.priority is None or effective_priority > existing.priority:
        existing.priority = effective_priority
        existing.updated_at = datetime.now(timezone.utc)
        session.add(existing)


def insert_sb_event(session: Session, event: ScheduleB) -> bool:
    """Insert-only dedupe by primary key event_id."""
    existing = session.get(ScheduleB, event.event_id)
    if existing:
        return False
    try:
        with session.begin_nested():
            session.add(event)
        return True
    except Exception:
        return False


def insert_ie_event(session: Session, event: IEScheduleE) -> bool:
    """
    Insert-only dedupe by primary key event_id.
    Returns True if inserted, False if already existed.
    """
    existing = session.get(IEScheduleE, event.event_id)
    if existing:
        return False
    try:
        with session.begin_nested():
            session.add(event)
        return True
    except Exception:
        return False


def insert_sa_event(session: Session, event: ScheduleA) -> bool:
    """
    Insert-only dedupe by primary key event_id.
    Returns True if inserted, False if already existed.
    """
    existing = session.get(ScheduleA, event.event_id)
    if existing:
        return False
    try:
        with session.begin_nested():
            session.add(event)
        return True
    except Exception:
        return False


def get_pac_groups(session: Session) -> list[dict]:
    """Get PAC groups from AppConfig."""
    config = session.get(AppConfig, "pac_groups")
    if config and config.value:
        try:
            groups = json.loads(config.value)
            if isinstance(groups, list):
                return groups
        except (json.JSONDecodeError, TypeError):
            pass
    return []


def set_pac_groups(session: Session, groups: list[dict]) -> None:
    """Save PAC groups to AppConfig as JSON."""
    config = session.get(AppConfig, "pac_groups")
    value = json.dumps(groups)
    if config:
        config.value = value
        config.updated_at = datetime.now(timezone.utc)
    else:
        config = AppConfig(key="pac_groups", value=value)
    session.add(config)


def sync_race_candidates_from_ie(session: Session) -> int:
    """Upsert IE candidates into race_candidates after IE ingestion.

    Ensures every candidate with IE spending is in race_candidates.
    Opponent lookup happens lazily via the /api/races endpoint.
    """
    from sqlalchemy import text

    added = 0
    ie_rows = session.execute(text("""
        SELECT candidate_id,
               MAX(candidate_name) AS name,
               MAX(candidate_party) AS party,
               MAX(candidate_state) AS state,
               MAX(candidate_office) AS office,
               MAX(candidate_district) AS district
        FROM ie_schedule_e
        WHERE candidate_id IS NOT NULL AND amount > 0
        GROUP BY candidate_id
    """)).all()

    for row in ie_rows:
        existing = session.get(RaceCandidate, row.candidate_id)
        if existing:
            if not existing.has_ie_spending:
                existing.has_ie_spending = True
                session.add(existing)
            if not existing.party and row.party:
                existing.party = row.party
                session.add(existing)
            continue
        session.add(RaceCandidate(
            candidate_id=row.candidate_id,
            candidate_name=row.name,
            party=row.party,
            state=row.state,
            office=row.office,
            district=row.district or "",
            has_ie_spending=True,
        ))
        added += 1

    session.flush()
    print(f"[race_candidates] Synced IE candidates: {added} new")
    return added
