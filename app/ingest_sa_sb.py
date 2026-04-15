"""SA/SB ingestion worker — combined Schedule A + Schedule B extraction
from full F3X .fec files, designed for parallel-safe queue consumption.

Three-tier processing strategy based on file size:
- Small (< 50MB):   fecfile parse on the small worker (8 GiB).
- Big   (50-300MB): fecfile parse on the big worker (32 GiB).
- Giant (>= 300MB): streaming raw parse on the big worker (degraded
                    field accuracy, but bounded memory).

Workers share the queue (`ingestion_tasks WHERE source_feed='SA_SB'`)
and claim rows atomically via SELECT FOR UPDATE SKIP LOCKED. Routing
between small/big workers happens via the `file_size_mb` column on
the task row, which is populated by the small worker on first touch.
"""
from __future__ import annotations

import gc
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlmodel import Session

from .fec_parse import (
    check_file_size,
    download_fec_text,
    extract_schedule_a_best_effort,
    extract_schedule_b_best_effort,
    parse_fec_filing,
    sha256_hex,
    stream_sa_sb_from_url,
)
from .repo import get_max_new_per_run
from .schemas import FilingF3X, IngestionTask, ScheduleA, ScheduleB

# Size thresholds, in MB.
SMALL_WORKER_MAX_MB = 50      # < this → small worker handles it
BIG_FECFILE_MAX_MB = 300      # above this → switch to streaming
HARD_SIZE_CAP_MB = 1500       # above this → skip + alert (sanity guard)

BATCH_SIZE = 500              # SA/SB rows per bulk insert


@dataclass
class SaSbResult:
    filings_processed: int = 0
    sa_inserted: int = 0
    sb_inserted: int = 0
    failed_count: int = 0
    skipped_too_large_count: int = 0
    streamed_count: int = 0
    handed_off_count: int = 0  # small worker → big worker
    failed_filing_ids: list[int] = field(default_factory=list)
    streamed_filing_ids: list[int] = field(default_factory=list)
    skipped_filing_ids: list[int] = field(default_factory=list)
    last_error: Optional[str] = None


def _log_mem(label: str) -> None:
    try:
        import platform
        import resource
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if platform.system() == "Darwin":
            mb = usage / (1024 * 1024)
        else:
            mb = usage / 1024
        print(f"[SA_SB-MEM] {label}: {mb:.1f} MB")
    except Exception:
        pass


def _claim_next(session: Session, mode: str) -> Optional[dict]:
    """Atomically claim the next available SA_SB task for this worker mode.

    Returns a dict with filing_id, fec_url, file_size_mb, priority — or
    None if the queue is empty for this mode. Uses FOR UPDATE SKIP LOCKED
    so multiple workers can poll concurrently without colliding.
    """
    if mode == "small":
        size_filter = (
            f"(file_size_mb IS NULL OR file_size_mb < {SMALL_WORKER_MAX_MB})"
        )
    elif mode == "big":
        size_filter = f"file_size_mb >= {SMALL_WORKER_MAX_MB}"
    else:
        raise ValueError(f"unknown worker mode: {mode!r}")

    sql = text(f"""
        UPDATE ingestion_tasks
        SET status = 'downloading',
            updated_at = NOW()
        WHERE (filing_id, source_feed) = (
            SELECT filing_id, source_feed
            FROM ingestion_tasks
            WHERE source_feed = 'SA_SB'
              AND status = 'claimed'
              AND {size_filter}
            ORDER BY priority DESC, filing_id DESC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING filing_id, fec_url, file_size_mb, priority
    """)
    row = session.execute(sql).first()
    session.commit()
    if row is None:
        return None
    return {
        "filing_id": row.filing_id,
        "fec_url": row.fec_url,
        "file_size_mb": row.file_size_mb,
        "priority": row.priority,
    }


def _release_to_big_worker(
    session: Session, filing_id: int, file_size_mb: float
) -> None:
    """Reset a row back to claimed status with file_size_mb populated, so the
    big worker's filter (file_size_mb >= 50) picks it up next cycle."""
    task = session.get(IngestionTask, (filing_id, "SA_SB"))
    if task is None:
        return
    task.status = "claimed"
    task.file_size_mb = file_size_mb
    session.add(task)
    session.commit()


def _mark_status(
    session: Session,
    filing_id: int,
    status: str,
    *,
    failed_step: Optional[str] = None,
    error_message: Optional[str] = None,
    skip_reason: Optional[str] = None,
    file_size_mb: Optional[float] = None,
) -> None:
    """Update the SA_SB task row's status + optional metadata."""
    task = session.get(IngestionTask, (filing_id, "SA_SB"))
    if task is None:
        return
    task.status = status
    if failed_step is not None:
        task.failed_step = failed_step
    if error_message is not None:
        task.error_message = error_message[:500]
    if skip_reason is not None:
        task.skip_reason = skip_reason
    if file_size_mb is not None:
        task.file_size_mb = file_size_mb
    session.add(task)
    session.commit()


def _flush_sa_batch(session: Session, batch: list[dict]) -> int:
    if not batch:
        return 0
    stmt = pg_insert(ScheduleA).values(batch).on_conflict_do_nothing(
        index_elements=["event_id"])
    result = session.execute(stmt)
    session.flush()
    return result.rowcount or 0


def _flush_sb_batch(session: Session, batch: list[dict]) -> int:
    if not batch:
        return 0
    stmt = pg_insert(ScheduleB).values(batch).on_conflict_do_nothing(
        index_elements=["event_id"])
    result = session.execute(stmt)
    session.flush()
    return result.rowcount or 0


def _process_with_fecfile(
    session: Session,
    filing: FilingF3X,
    fec_text: str,
    result: SaSbResult,
) -> None:
    """Parse with fecfile and insert SA + SB rows."""
    parsed = parse_fec_filing(fec_text)
    _log_mem(f"after_parse_{filing.filing_id}")

    sa_batch: list[dict] = []
    sb_batch: list[dict] = []
    sa_inserted = 0
    sb_inserted = 0

    for raw_line, fields in extract_schedule_a_best_effort(
            fec_text, parsed=parsed):
        sa_batch.append(_sa_row_dict(filing, raw_line, fields))
        if len(sa_batch) >= BATCH_SIZE:
            sa_inserted += _flush_sa_batch(session, sa_batch)
            sa_batch = []
    sa_inserted += _flush_sa_batch(session, sa_batch)

    for raw_line, fields in extract_schedule_b_best_effort(
            fec_text, parsed=parsed):
        sb_batch.append(_sb_row_dict(filing, raw_line, fields))
        if len(sb_batch) >= BATCH_SIZE:
            sb_inserted += _flush_sb_batch(session, sb_batch)
            sb_batch = []
    sb_inserted += _flush_sb_batch(session, sb_batch)

    result.sa_inserted += sa_inserted
    result.sb_inserted += sb_inserted
    print(f"[SA_SB] filing {filing.filing_id}: "
          f"{sa_inserted} SA + {sb_inserted} SB rows (fecfile)")

    del parsed


def _process_with_streaming(
    session: Session,
    filing: FilingF3X,
    result: SaSbResult,
) -> None:
    """Stream the file from URL, extracting SA + SB without loading
    everything into memory. Used for filings >= BIG_FECFILE_MAX_MB."""
    sa_batch: list[dict] = []
    sb_batch: list[dict] = []
    sa_inserted = 0
    sb_inserted = 0

    for kind, raw_line, fields in stream_sa_sb_from_url(filing.fec_url):
        if kind == "SA":
            sa_batch.append(_sa_row_dict(filing, raw_line, fields))
            if len(sa_batch) >= BATCH_SIZE:
                sa_inserted += _flush_sa_batch(session, sa_batch)
                sa_batch = []
        elif kind == "SB":
            sb_batch.append(_sb_row_dict(filing, raw_line, fields))
            if len(sb_batch) >= BATCH_SIZE:
                sb_inserted += _flush_sb_batch(session, sb_batch)
                sb_batch = []
    sa_inserted += _flush_sa_batch(session, sa_batch)
    sb_inserted += _flush_sb_batch(session, sb_batch)

    result.sa_inserted += sa_inserted
    result.sb_inserted += sb_inserted
    result.streamed_count += 1
    result.streamed_filing_ids.append(filing.filing_id)
    print(f"[SA_SB] filing {filing.filing_id}: "
          f"{sa_inserted} SA + {sb_inserted} SB rows (streamed)")


def _sa_row_dict(filing: FilingF3X, raw_line: str, fields: dict) -> dict:
    event_id = sha256_hex(f"{filing.filing_id}|{raw_line}")
    return dict(
        event_id=event_id,
        filing_id=filing.filing_id,
        committee_id=filing.committee_id,
        committee_name=filing.committee_name,
        form_type=filing.form_type,
        report_type=filing.report_type,
        coverage_from=filing.coverage_from,
        coverage_through=filing.coverage_through,
        filed_at_utc=filing.filed_at_utc,
        contributor_name=fields["contributor_name"],
        contributor_employer=fields["contributor_employer"],
        contributor_occupation=fields["contributor_occupation"],
        contribution_amount=fields["contribution_amount"],
        contribution_date=fields["contribution_date"],
        receipt_description=fields["receipt_description"],
        contributor_type=fields["contributor_type"],
        memo_text=fields["memo_text"],
        fec_url=filing.fec_url,
        raw_line=raw_line[:200],
    )


def _sb_row_dict(filing: FilingF3X, raw_line: str, fields: dict) -> dict:
    event_id = sha256_hex(f"{filing.filing_id}|{raw_line}")
    return dict(
        event_id=event_id,
        filing_id=filing.filing_id,
        committee_id=filing.committee_id,
        committee_name=filing.committee_name,
        form_type=filing.form_type,
        report_type=filing.report_type,
        coverage_from=filing.coverage_from,
        coverage_through=filing.coverage_through,
        filed_at_utc=filing.filed_at_utc,
        payee_name=fields["payee_name"],
        payee_employer=fields["payee_employer"],
        payee_occupation=fields["payee_occupation"],
        disbursement_amount=fields["disbursement_amount"],
        disbursement_date=fields["disbursement_date"],
        disbursement_description=fields["disbursement_description"],
        payee_type=fields["payee_type"],
        purpose=fields["purpose"],
        category_code=fields["category_code"],
        memo_text=fields["memo_text"],
        beneficiary_candidate_id=fields["beneficiary_candidate_id"],
        beneficiary_candidate_name=fields["beneficiary_candidate_name"],
        fec_url=filing.fec_url,
        raw_line=raw_line[:200],
    )


def run_sa_sb(
    session: Session,
    *,
    mode: str = "small",
    max_filings: Optional[int] = None,
) -> SaSbResult:
    """Drain the SA_SB queue for this worker mode.

    mode="small": handles filings with file_size_mb < 50 (or unknown).
                  When it discovers an unknown row is actually >=50MB,
                  it populates file_size_mb and releases the row back
                  to the queue for the big worker.
    mode="big":   handles filings with file_size_mb >= 50. Uses fecfile
                  for 50-300MB; switches to streaming for >=300MB.

    Returns SaSbResult with counters for monitoring + email alerts.
    Stops when the queue is empty or max_filings is reached.
    """
    result = SaSbResult()
    max_per_run = max_filings or get_max_new_per_run(session)
    print(f"[SA_SB] starting worker mode={mode} max={max_per_run}")
    _log_mem("worker_start")

    while result.filings_processed < max_per_run:
        task = _claim_next(session, mode=mode)
        if task is None:
            print(f"[SA_SB] queue empty for mode={mode}, exiting")
            break

        filing_id = task["filing_id"]
        fec_url = task["fec_url"]
        known_size = task["file_size_mb"]
        print(f"[SA_SB] claimed filing {filing_id} (size={known_size})")

        # Small worker: HEAD-check size if unknown. Hand off if >=50MB.
        if mode == "small" and known_size is None:
            try:
                size_mb = check_file_size(fec_url)
            except Exception as e:
                _mark_status(
                    session, filing_id, "failed",
                    failed_step="size_check", error_message=str(e),
                )
                result.failed_count += 1
                result.failed_filing_ids.append(filing_id)
                continue

            if size_mb is None:
                # HEAD didn't return size — try downloading anyway with
                # the small-worker cap. If too big, the download itself
                # will raise FileTooLargeError.
                size_mb = 0  # placeholder so we proceed
            else:
                if size_mb >= SMALL_WORKER_MAX_MB:
                    print(f"[SA_SB] filing {filing_id} is "
                          f"{size_mb:.1f}MB — handing off to big worker")
                    _release_to_big_worker(session, filing_id, size_mb)
                    result.handed_off_count += 1
                    continue
                # Persist the size so we don't HEAD again next time.
                _mark_status(
                    session, filing_id, "downloading",
                    file_size_mb=size_mb,
                )
                known_size = size_mb

        # Big worker: enforce the hard sanity cap.
        if known_size is not None and known_size >= HARD_SIZE_CAP_MB:
            print(f"[SA_SB] filing {filing_id} exceeds hard cap "
                  f"({known_size:.1f}MB >= {HARD_SIZE_CAP_MB}MB) — skipping")
            _mark_status(
                session, filing_id, "skipped",
                skip_reason="too_large", file_size_mb=known_size,
            )
            result.skipped_too_large_count += 1
            result.skipped_filing_ids.append(filing_id)
            continue

        filing = session.get(FilingF3X, filing_id)
        if filing is None:
            _mark_status(
                session, filing_id, "failed",
                failed_step="lookup",
                error_message="filing_f3x row missing",
            )
            result.failed_count += 1
            result.failed_filing_ids.append(filing_id)
            continue

        # Decide path: streaming for giants, fecfile otherwise.
        use_streaming = (
            known_size is not None and known_size >= BIG_FECFILE_MAX_MB
        )

        try:
            if use_streaming:
                _mark_status(session, filing_id, "parsing")
                _process_with_streaming(session, filing, result)
            else:
                fec_text = download_fec_text(
                    fec_url, max_size_mb=HARD_SIZE_CAP_MB)
                _log_mem(f"after_download_{filing_id}")
                _mark_status(session, filing_id, "parsing")
                _process_with_fecfile(session, filing, fec_text, result)
                del fec_text
            _mark_status(session, filing_id, "ingested")
        except Exception as e:
            print(f"[SA_SB] failed filing {filing_id}: {e}")
            session.rollback()
            _mark_status(
                session, filing_id, "failed",
                failed_step="parsing" if not use_streaming else "streaming",
                error_message=str(e),
            )
            result.failed_count += 1
            result.failed_filing_ids.append(filing_id)
            result.last_error = str(e)[:500]
            continue
        finally:
            gc.collect()
            _log_mem(f"after_gc_{filing_id}")

        result.filings_processed += 1

    print(
        f"[SA_SB] done mode={mode}: "
        f"processed={result.filings_processed} "
        f"sa={result.sa_inserted} sb={result.sb_inserted} "
        f"failed={result.failed_count} "
        f"skipped_too_large={result.skipped_too_large_count} "
        f"streamed={result.streamed_count} "
        f"handed_off={result.handed_off_count}"
    )
    return result
