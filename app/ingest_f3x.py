from __future__ import annotations

import gc
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from sqlmodel import Session

from .feeds import fetch_rss_items, infer_filing_id, parse_mmddyyyy
from .fec_lookup import resolve_committee_name
from .fec_parse import download_fec_header, parse_f3x_header_only, extract_committee_name
from .repo import (
    claim_filing, update_filing_status, upsert_f3x,
    get_max_new_per_run, enqueue_sa_sb_task,
)

MAX_HEADER_BYTES = 50_000  # Only need first ~50KB for F3X header


@dataclass
class F3XResult:
    new_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    last_error: Optional[str] = None


def _log_mem(label: str):
    """Quick memory log."""
    try:
        import resource
        import platform
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if platform.system() == "Darwin":
            mb = usage / (1024 * 1024)
        else:
            mb = usage / 1024
        print(f"[F3X-MEM] {label}: {mb:.1f} MB")
    except Exception:
        pass


def run_f3x(session: Session, *, feed_url: str, receipts_threshold: float) -> F3XResult:
    result = F3XResult()
    max_per_run = get_max_new_per_run(session)
    items = fetch_rss_items(feed_url)
    print(f"[F3X] RSS feed has {len(items)} items (max {max_per_run} per run)")
    _log_mem("after_fetch_rss")
    today = datetime.now(timezone.utc).date()

    for i, item in enumerate(items):
        # Stop when we hit filings from before today (feed is newest-first)
        if item.pub_date_utc and item.pub_date_utc.date() < today:
            print(f"[F3X] Reached filings from {item.pub_date_utc.date()}, stopping")
            break

        # Stop if we've processed enough new filings this run
        if result.new_count >= max_per_run:
            print(
                f"[F3X] Reached limit of {max_per_run} new filings, stopping")
            break
        filing_id = infer_filing_id(item)
        if filing_id is None:
            continue

        if not claim_filing(session, filing_id, source_feed="F3X"):
            result.skipped_count += 1
            continue

        print(f"[F3X] Processing new filing {filing_id} ({result.new_count + 1})")
        _log_mem(f"before_download_{filing_id}")

        update_filing_status(session, filing_id, "F3X", "downloading")
        try:
            fec_text = download_fec_header(item.link, max_bytes=MAX_HEADER_BYTES)
        except Exception as e:
            print(f"[F3X] Download failed for {filing_id}: {e}")
            update_filing_status(
                session, filing_id, "F3X", "failed",
                failed_step="downloading", error_message=str(e)[:500])
            result.failed_count += 1
            result.last_error = str(e)[:500]
            continue

        update_filing_status(session, filing_id, "F3X", "downloaded")
        _log_mem(f"after_download_{filing_id}")

        update_filing_status(session, filing_id, "F3X", "parsing")
        try:
            # Light parse - header only, no fecfile
            parsed = parse_f3x_header_only(fec_text)
            _log_mem(f"after_parse_{filing_id}")
            filing_fields = parsed.get("filing", {})

            def _to_float(v):
                if v in (None, ""):
                    return None
                try:
                    return float(v)
                except (TypeError, ValueError):
                    return None

            total = _to_float(filing_fields.get("col_a_total_receipts"))
            total_disb = _to_float(
                filing_fields.get("col_a_total_disbursements"))
            threshold_flag = (total is not None and total >= receipts_threshold)

            meta = item.meta
            committee_id = meta.get("CommitteeId") or ""

            # Get committee name from DB, or insert provisional from filing
            form_name = extract_committee_name(parsed)
            committee_name = resolve_committee_name(
                session, committee_id, fallback_name=form_name)
            upsert_f3x(
                session,
                filing_id=filing_id,
                committee_id=committee_id,
                committee_name=committee_name,
                form_type=meta.get("FormType"),
                report_type=meta.get("ReportType"),
                coverage_from=parse_mmddyyyy(meta.get("CoverageFrom")),
                coverage_through=parse_mmddyyyy(meta.get("CoverageThrough")),
                filed_at_utc=item.pub_date_utc,
                fec_url=item.link,
                total_receipts=total,
                total_disbursements=total_disb,
                threshold_flag=threshold_flag,
                raw_meta=meta,
            )
            update_filing_status(
                session, filing_id, "F3X", "ingested")
            # Enqueue full-file SA/SB ingestion for the background worker.
            # Default priority = filing_id (newest first since FEC ids
            # are monotonic).
            enqueue_sa_sb_task(
                session, filing_id, fec_url=item.link)
        except Exception as e:
            print(f"[F3X] Failed to process {filing_id}: {e}")
            update_filing_status(
                session, filing_id, "F3X", "failed",
                failed_step="parsing", error_message=str(e)[:500])
            result.failed_count += 1
            result.last_error = str(e)[:500]
            continue

        # Explicit cleanup to free memory
        del fec_text
        del parsed
        gc.collect()
        _log_mem(f"after_gc_{filing_id}")

        result.new_count += 1

    print(f"[F3X] Done: {result.new_count} new, {result.failed_count} failed, {result.skipped_count} skipped")
    _log_mem("end")
    return result
