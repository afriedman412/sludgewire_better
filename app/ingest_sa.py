"""
Schedule A second-pass ingestion.

After F3X headers are ingested, this module re-downloads the full filing
for target PAC committees and extracts Schedule A line items.

Files are processed one at a time with explicit gc cleanup to keep memory
usage bounded — full F3X files can be 100MB+.
"""
from __future__ import annotations

import gc
from dataclasses import dataclass
from typing import Optional

from sqlmodel import Session, select
from sqlalchemy import exists
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .fec_parse import (
    download_fec_text, parse_fec_filing,
    extract_schedule_a_best_effort, sha256_hex, FileTooLargeError,
)
from .repo import (
    claim_filing, update_filing_status,
    get_sa_target_committee_ids, get_pac_groups, get_max_new_per_run,
    record_skipped_filing,
)
from .schemas import FilingF3X, IngestionTask, ScheduleA

BATCH_SIZE = 500

MAX_FILE_SIZE_MB = 50


@dataclass
class SAResult:
    filings_processed: int = 0
    events_inserted: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    last_error: Optional[str] = None


def _log_mem(label: str):
    try:
        import resource
        import platform
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if platform.system() == "Darwin":
            mb = usage / (1024 * 1024)
        else:
            mb = usage / 1024
        print(f"[SA-MEM] {label}: {mb:.1f} MB")
    except Exception:
        pass


def _record_failure(session: Session, filing_id: int, failed_step: str, error_message: str):
    """Record a failed SA ingestion after a rollback wiped the claimed row."""
    session.add(IngestionTask(
        filing_id=filing_id,
        source_feed="SA",
        status="failed",
        failed_step=failed_step,
        error_message=error_message,
    ))
    session.commit()


def _flush_batch(session: Session, batch: list[dict]) -> int:
    """Bulk insert SA events, skipping duplicates. Returns count inserted."""
    if not batch:
        return 0
    stmt = pg_insert(ScheduleA).values(batch)
    stmt = stmt.on_conflict_do_nothing(index_elements=["event_id"])
    result = session.execute(stmt)
    session.flush()
    return result.rowcount


def run_sa(
    session: Session,
) -> SAResult:
    """
    Second-pass Schedule A ingestion.

    Finds all F3X filings from target committees that don't yet have
    an SA ingestion task, downloads the full FEC file (one at a time),
    parses Schedule A items, and inserts them.
    """
    result = SAResult()

    # Always include all PAC groups, plus any additional SA targets
    target_set: set[str] = set()
    pac_groups = get_pac_groups(session)
    for group in pac_groups:
        for pac in group.get("pacs", []):
            cid = pac.get("committee_id", "").strip()
            if cid:
                target_set.add(cid)
    extra_ids = get_sa_target_committee_ids(session)
    target_set.update(extra_ids)
    target_ids = list(target_set)
    if not target_ids:
        print("[SA] No PAC groups or SA targets configured, skipping")
        return result
    print(f"[SA] Targeting {len(target_ids)} committees "
          f"({len(target_ids) - len(extra_ids)} from PAC groups"
          f"{f', {len(extra_ids)} additional' if extra_ids else ''})")

    max_per_run = get_max_new_per_run(session)

    # Find target PAC F3X filings with no SA ingestion task yet
    sa_exists = exists(
        select(IngestionTask.filing_id)
        .where(IngestionTask.filing_id == FilingF3X.filing_id)
        .where(IngestionTask.source_feed == "SA")
    )
    stmt = (
        select(FilingF3X)
        .where(FilingF3X.committee_id.in_(target_ids))
        .where(~sa_exists)
        .order_by(FilingF3X.filed_at_utc.desc())
    )
    filings = session.exec(stmt).all()
    print(f"[SA] Found {len(filings)} unprocessed F3X filings from "
          f"{len(target_ids)} target committees")

    for filing in filings:
        if result.filings_processed >= max_per_run:
            print(f"[SA] Reached limit of {max_per_run}, stopping")
            break

        filing_id = filing.filing_id

        # Claim with source_feed="SA" — skips if already processed
        if not claim_filing(session, filing_id, source_feed="SA"):
            result.skipped_count += 1
            continue

        print(f"[SA] Processing filing {filing_id} "
              f"from {filing.committee_id} ({filing.committee_name})")
        _log_mem(f"before_download_{filing_id}")

        # Download full file (one at a time for memory safety)
        update_filing_status(session, filing_id, "SA", "downloading")
        try:
            fec_text = download_fec_text(
                filing.fec_url, max_size_mb=MAX_FILE_SIZE_MB)
        except FileTooLargeError as e:
            print(f"[SA] Skipping {filing_id}: "
                  f"{e.size_mb:.1f}MB exceeds limit")
            record_skipped_filing(
                session, filing_id, "SA", "too_large",
                file_size_mb=e.size_mb, fec_url=filing.fec_url,
            )
            update_filing_status(session, filing_id, "SA", "skipped")
            continue
        except Exception as e:
            print(f"[SA] Download failed for {filing_id}: {e}")
            session.rollback()
            _record_failure(session, filing_id, "downloading", str(e)[:500])
            result.failed_count += 1
            result.last_error = str(e)[:500]
            continue

        update_filing_status(session, filing_id, "SA", "downloaded")
        _log_mem(f"after_download_{filing_id}")

        # Parse Schedule A items
        update_filing_status(session, filing_id, "SA", "parsing")
        try:
            parsed = parse_fec_filing(fec_text)
            _log_mem(f"after_parse_{filing_id}")

            events_this_filing = 0
            batch = []
            for raw_line, fields in extract_schedule_a_best_effort(
                    fec_text, parsed=parsed):
                event_id = sha256_hex(f"{filing_id}|{raw_line}")
                batch.append(dict(
                    event_id=event_id,
                    filing_id=filing_id,
                    committee_id=filing.committee_id,
                    committee_name=filing.committee_name,
                    form_type=filing.form_type,
                    report_type=filing.report_type,
                    coverage_from=filing.coverage_from,
                    coverage_through=filing.coverage_through,
                    filed_at_utc=filing.filed_at_utc,
                    contributor_name=fields["contributor_name"],
                    contributor_employer=fields[
                        "contributor_employer"],
                    contributor_occupation=fields[
                        "contributor_occupation"],
                    contribution_amount=fields[
                        "contribution_amount"],
                    contribution_date=fields["contribution_date"],
                    receipt_description=fields[
                        "receipt_description"],
                    contributor_type=fields["contributor_type"],
                    memo_text=fields["memo_text"],
                    fec_url=filing.fec_url,
                    raw_line=raw_line[:200],
                ))
                if len(batch) >= BATCH_SIZE:
                    events_this_filing += _flush_batch(
                        session, batch)
                    batch = []
            if batch:
                events_this_filing += _flush_batch(
                    session, batch)

            update_filing_status(session, filing_id, "SA", "ingested")
            result.events_inserted += events_this_filing
            print(f"[SA] Filing {filing_id}: "
                  f"{events_this_filing} SA events inserted")

        except Exception as e:
            print(f"[SA] Failed to process {filing_id}: {e}")
            session.rollback()
            _record_failure(session, filing_id, "parsing", str(e)[:500])
            result.failed_count += 1
            result.last_error = str(e)[:500]
            continue

        # Explicit cleanup — one filing at a time to bound memory
        del fec_text
        del parsed
        gc.collect()
        _log_mem(f"after_gc_{filing_id}")

        result.filings_processed += 1

    print(
        f"[SA] Done: {result.filings_processed} processed, "
        f"{result.events_inserted} events, "
        f"{result.failed_count} failed, "
        f"{result.skipped_count} skipped"
    )
    return result
