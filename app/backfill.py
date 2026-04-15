"""FEC API backfill logic for fetching historical data by date."""
from __future__ import annotations

import gc
from datetime import date, datetime, timezone, timedelta
from typing import Optional

import requests
from sqlmodel import Session, select

from .schemas import BackfillJob, FilingF3X, IEScheduleE
from .fec_lookup import resolve_committee_name
from .fec_parse import download_fec_text, download_fec_header, parse_fec_filing, parse_f3x_header_only, extract_committee_name, sha256_hex, extract_schedule_e_best_effort
from .repo import claim_filing, upsert_f3x, insert_ie_event
from .settings import load_settings

FEC_API_BASE = "https://api.open.fec.gov/v1"


def get_or_create_backfill_job(session: Session, target_date: date, filing_type: str) -> BackfillJob:
    """Get existing backfill job or create a new one."""
    stmt = select(BackfillJob).where(
        BackfillJob.target_date == target_date,
        BackfillJob.filing_type == filing_type
    )
    job = session.exec(stmt).first()

    if job is None:
        job = BackfillJob(
            target_date=target_date,
            filing_type=filing_type,
            status="pending"
        )
        session.add(job)
        session.commit()
        session.refresh(job)

    return job


def backfill_date_f3x(session: Session, target_date: date) -> BackfillJob:
    """Backfill F3X filings for a specific date from FEC API."""
    settings = load_settings()
    job = get_or_create_backfill_job(session, target_date, "3x")

    # Skip if already completed
    if job.status == "completed":
        return job

    # Restart stale "running" jobs (stuck for > 10 min)
    if job.status == "running" and job.started_at:
        stale_threshold = datetime.now(timezone.utc) - timedelta(minutes=10)
        if job.started_at < stale_threshold:
            job.status = "pending"  # Reset to retry

    # Mark as running
    job.status = "running"
    job.started_at = datetime.now(timezone.utc)
    session.add(job)
    session.commit()

    try:
        filings_found = 0
        files_checked = 0
        page = 1
        per_page = 100

        while True:
            # Query FEC API for F3X filings on this date
            params = {
                "api_key": settings.gov_api_key or "DEMO_KEY",
                "form_type": "F3X",
                "min_receipt_date": target_date.isoformat(),
                "max_receipt_date": target_date.isoformat(),
                "per_page": per_page,
                "page": page,
                "sort": "-receipt_date",
            }

            resp = requests.get(f"{FEC_API_BASE}/filings/", params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            results = data.get("results", [])
            if not results:
                break

            # Get total count on first page
            if page == 1:
                total_count = data.get("pagination", {}).get("count", 0)
                job.error_message = f"0/{total_count} files"  # Repurpose for progress
                session.add(job)
                session.commit()

            for filing in results:
                filing_id = filing.get("file_number")
                if not filing_id:
                    continue

                # Skip if already seen
                if not claim_filing(session, filing_id, source_feed="BACKFILL"):
                    continue

                # Download and parse the FEC file
                fec_url = f"https://docquery.fec.gov/dcdev/posted/{filing_id}.fec"
                fec_text = None
                parsed = {}
                try:
                    fec_text = download_fec_header(fec_url)
                    parsed = parse_f3x_header_only(fec_text)
                except Exception:
                    # If we can't download, still record basic info from API
                    pass

                filing_fields = parsed.get("filing", {})
                total = filing_fields.get("col_a_total_receipts")
                if total not in (None, ""):
                    try:
                        total = float(total)
                    except (TypeError, ValueError):
                        total = None
                else:
                    # Fallback to API data
                    total = filing.get("total_receipts")

                total_disb = filing_fields.get("col_a_total_disbursements")
                if total_disb not in (None, ""):
                    try:
                        total_disb = float(total_disb)
                    except (TypeError, ValueError):
                        total_disb = None
                else:
                    total_disb = filing.get("total_disbursements")

                threshold_flag = (total is not None and total >= settings.receipts_threshold)

                committee_id = filing.get("committee_id") or ""
                form_name = extract_committee_name(parsed) if parsed else None
                committee_name = resolve_committee_name(session, committee_id, fallback_name=form_name)

                # Parse dates from API response
                coverage_from = None
                coverage_through = None
                if filing.get("coverage_start_date"):
                    try:
                        coverage_from = date.fromisoformat(filing["coverage_start_date"][:10])
                    except ValueError:
                        pass
                if filing.get("coverage_end_date"):
                    try:
                        coverage_through = date.fromisoformat(filing["coverage_end_date"][:10])
                    except ValueError:
                        pass

                filed_at_utc = None
                if filing.get("receipt_date"):
                    try:
                        filed_at_utc = datetime.fromisoformat(
                            filing["receipt_date"][:10] + "T12:00:00+00:00"
                        )
                    except ValueError:
                        pass

                upsert_f3x(
                    session,
                    filing_id=filing_id,
                    committee_id=committee_id,
                    committee_name=committee_name,
                    form_type=filing.get("form_type"),
                    report_type=filing.get("report_type"),
                    coverage_from=coverage_from,
                    coverage_through=coverage_through,
                    filed_at_utc=filed_at_utc,
                    fec_url=fec_url,
                    total_receipts=total,
                    total_disbursements=total_disb,
                    threshold_flag=threshold_flag,
                    raw_meta=filing,
                )
                filings_found += 1

                # Explicit cleanup to free memory
                del fec_text
                del parsed
                gc.collect()

                # Update progress
                files_checked += 1
                job.filings_found = filings_found
                job.error_message = f"{files_checked}/{total_count} files checked"
                session.add(job)
                session.commit()

            # Check pagination
            pagination = data.get("pagination", {})
            if page >= pagination.get("pages", 1):
                break
            page += 1

        job.status = "completed"
        job.completed_at = datetime.now(timezone.utc)
        job.filings_found = filings_found
        job.error_message = None  # Clear progress message

    except Exception as e:
        job.status = "failed"
        job.error_message = str(e)[:500]

    session.add(job)
    session.commit()
    return job


def backfill_date_e(session: Session, target_date: date) -> BackfillJob:
    """Backfill Schedule E filings for a specific date from FEC API."""
    settings = load_settings()
    job = get_or_create_backfill_job(session, target_date, "e")

    if job.status == "completed":
        return job

    # Restart stale "running" jobs (stuck for > 10 min)
    if job.status == "running" and job.started_at:
        stale_threshold = datetime.now(timezone.utc) - timedelta(minutes=10)
        if job.started_at < stale_threshold:
            job.status = "pending"  # Reset to retry

    job.status = "running"
    job.started_at = datetime.now(timezone.utc)
    session.add(job)
    session.commit()

    try:
        filings_found = 0
        files_checked = 0
        total_count = 0
        page = 1
        per_page = 100

        while True:
            # Query FEC API for F24 and F5 (independent expenditure forms)
            params = {
                "api_key": settings.gov_api_key or "DEMO_KEY",
                "form_type": ["F24", "F5"],
                "min_receipt_date": target_date.isoformat(),
                "max_receipt_date": target_date.isoformat(),
                "per_page": per_page,
                "page": page,
                "sort": "-receipt_date",
            }

            resp = requests.get(f"{FEC_API_BASE}/filings/", params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            results = data.get("results", [])
            if not results:
                break

            # Get total count on first page
            if page == 1:
                total_count = data.get("pagination", {}).get("count", 0)
                job.error_message = f"0/{total_count} filings"
                session.add(job)
                session.commit()

            for filing in results:
                filing_id = filing.get("file_number")
                if not filing_id:
                    continue

                # Download and parse the FEC file for Schedule E lines
                fec_url = f"https://docquery.fec.gov/dcdev/posted/{filing_id}.fec"
                try:
                    fec_text = download_fec_text(fec_url)
                except Exception:
                    continue

                committee_id = filing.get("committee_id") or ""

                filed_at_utc = None
                if filing.get("receipt_date"):
                    try:
                        filed_at_utc = datetime.fromisoformat(
                            filing["receipt_date"][:10] + "T12:00:00+00:00"
                        )
                    except ValueError:
                        pass

                # Parse dates
                coverage_from = None
                coverage_through = None
                if filing.get("coverage_start_date"):
                    try:
                        coverage_from = date.fromisoformat(filing["coverage_start_date"][:10])
                    except ValueError:
                        pass
                if filing.get("coverage_end_date"):
                    try:
                        coverage_through = date.fromisoformat(filing["coverage_end_date"][:10])
                    except ValueError:
                        pass

                # Extract Schedule E lines
                for raw_line, fields in extract_schedule_e_best_effort(fec_text):
                    event_id = sha256_hex(f"{filing_id}|{raw_line}")

                    event = IEScheduleE(
                        event_id=event_id,
                        filing_id=filing_id,
                        filer_id=committee_id,
                        committee_id=committee_id,
                        committee_name=filing.get("committee_name"),
                        form_type=filing.get("form_type"),
                        report_type=filing.get("report_type"),
                        coverage_from=coverage_from,
                        coverage_through=coverage_through,
                        filed_at_utc=filed_at_utc,
                        expenditure_date=fields.get("expenditure_date"),
                        amount=fields.get("amount"),
                        support_oppose=fields.get("support_oppose"),
                        candidate_id=fields.get("candidate_id"),
                        candidate_name=fields.get("candidate_name"),
                        candidate_office=fields.get("candidate_office"),
                        candidate_state=fields.get("candidate_state"),
                        candidate_district=fields.get("candidate_district"),
                        candidate_party=fields.get("candidate_party"),
                        election_code=fields.get("election_code"),
                        purpose=fields.get("purpose"),
                        payee_name=fields.get("payee_name"),
                        fec_url=fec_url,
                        raw_line=raw_line[:200],  # Truncate to save memory
                    )
                    if insert_ie_event(session, event):
                        filings_found += 1

                # Explicit cleanup to free memory
                del fec_text
                gc.collect()

                # Update progress after each filing
                files_checked += 1
                job.filings_found = filings_found
                job.error_message = f"{files_checked}/{total_count} filings ({filings_found} events)"
                session.add(job)
                session.commit()

            pagination = data.get("pagination", {})
            if page >= pagination.get("pages", 1):
                break
            page += 1

        job.status = "completed"
        job.completed_at = datetime.now(timezone.utc)
        job.filings_found = filings_found
        job.error_message = None  # Clear progress message

    except Exception as e:
        job.status = "failed"
        job.error_message = str(e)[:500]

    session.add(job)
    session.commit()
    return job


def get_backfill_status(session: Session, target_date: date, filing_type: str) -> Optional[BackfillJob]:
    """Get the status of a backfill job."""
    stmt = select(BackfillJob).where(
        BackfillJob.target_date == target_date,
        BackfillJob.filing_type == filing_type
    )
    return session.exec(stmt).first()
