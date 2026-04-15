"""
Cloud Run Job for continuous FEC filing ingestion.

Loops until no new filings are found, then exits.
Designed to run as a Cloud Run Job triggered by Cloud Scheduler.
"""
from __future__ import annotations

import gc
import json
import sys
import time
from datetime import datetime, timezone

from sqlmodel import Session, select

# Add parent to path for imports
sys.path.insert(0, "/app")

from app.settings import load_settings
from app.db import make_engine, init_db

from app.ingest_f3x import run_f3x
from app.ingest_ie import run_ie_schedule_e
from app.email_service import send_filing_alert, send_ptr_alert
from app.repo import get_max_new_per_run, get_email_enabled, get_ptr_email_enabled, mark_tasks_emailed
from app.schemas import AppConfig, FilingF3X, IEScheduleE, PtrFiling, PtrTransaction

from scripts.ingest_ptr import fetch_filing_index, sync_filing_index, process_pending as process_ptr_pending


MAX_ITERATIONS = 50  # Safety limit to prevent infinite loops
MAX_RUNTIME_MINUTES = 55  # Leave buffer before Cloud Run's 60 min timeout
PAUSE_BETWEEN_BATCHES = 2  # Seconds to pause between batches


def log(msg: str):
    """Log with timestamp."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def save_job_status(engine, results: dict):
    """Save job run status to AppConfig for config page display."""
    with Session(engine) as session:
        config_entry = session.get(AppConfig, "last_cron_run")
        if config_entry:
            config_entry.value = json.dumps(results)
            config_entry.updated_at = datetime.now(timezone.utc)
        else:
            config_entry = AppConfig(
                key="last_cron_run", value=json.dumps(results))
        session.add(config_entry)
        session.commit()


def run_job():
    """Main job loop - process filings until caught up."""
    log("Starting FEC ingestion job")

    settings = load_settings()
    engine = make_engine(settings)
    init_db(engine)

    start_time = time.time()
    iteration = 0
    total_f3x = 0
    total_f3x_failed = 0
    last_f3x_error = None
    total_ie_filings = 0
    total_ie_events = 0
    total_ptr_new = 0
    total_ptr_parsed = 0

    results = {
        "status": "running",
        "source": "cloud_run_job",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
        "f3x_new": 0,
        "f3x_failed": 0,
        "ie_events_new": 0,
        "ptr_new": 0,
        "ptr_parsed": 0,
        "iterations": 0,
        "email_sent": False,
        "emails_sent_to": [],
    }
    save_job_status(engine, results)

    try:
        while iteration < MAX_ITERATIONS:
            iteration += 1
            elapsed_minutes = (time.time() - start_time) / 60

            # Check runtime limit
            if elapsed_minutes >= MAX_RUNTIME_MINUTES:
                log(f"Reached max runtime ({MAX_RUNTIME_MINUTES} min), stopping")
                break

            log(f"=== Iteration {iteration} (elapsed: {elapsed_minutes:.1f} min) ===")

            with Session(engine) as session:
                max_per_run = get_max_new_per_run(session)
                log(f"Max per run: {max_per_run}")

                # Run F3X ingestion
                f3x_new = 0
                try:
                    f3x_result = run_f3x(
                        session,
                        feed_url=settings.f3x_feed,
                        receipts_threshold=settings.receipts_threshold,
                    )
                    f3x_new = f3x_result.new_count
                    total_f3x += f3x_new
                    total_f3x_failed += f3x_result.failed_count
                    if f3x_result.last_error:
                        last_f3x_error = f3x_result.last_error
                    log(f"F3X: {f3x_new} new, {f3x_result.failed_count} failed this batch")
                except Exception as e:
                    log(f"F3X error: {e}")
                    last_f3x_error = str(e)

                # Run IE ingestion
                ie_filings = 0
                ie_events = 0
                try:
                    ie_filings, ie_events = run_ie_schedule_e(
                        session,
                        feed_urls=settings.ie_feeds,
                    )
                    total_ie_filings += ie_filings
                    total_ie_events += ie_events
                    log(f"IE: {ie_filings} filings, {ie_events} events this batch")
                except Exception as e:
                    log(f"IE error: {e}")

                # SA/SB second-pass now runs as a separate Cloud Run Job
                # (fec-sa-sb-job, fec-sa-sb-job-big). The light F3X
                # ingester enqueues SA_SB tasks; the workers drain them.
                session.commit()
                gc.collect()

            # Update status after each iteration
            results["f3x_new"] = total_f3x
            results["f3x_failed"] = total_f3x_failed
            results["ie_events_new"] = total_ie_events
            results["iterations"] = iteration
            if last_f3x_error:
                results["f3x_error"] = (
                    f"{total_f3x_failed} filing(s) failed: "
                    f"{last_f3x_error}"
                )
            save_job_status(engine, results)

            # Check if we're caught up (no new filings in this batch)
            if f3x_new == 0 and ie_filings == 0:
                log("No new filings found - caught up!")
                break

            # If we got a full batch, there might be more - continue
            if f3x_new >= max_per_run or ie_filings >= max_per_run:
                log(f"Got full batch, continuing after {PAUSE_BETWEEN_BATCHES}s pause...")
                time.sleep(PAUSE_BETWEEN_BATCHES)
            else:
                # Partial batch means we're nearly done
                log("Partial batch - likely caught up")
                break

        # Run House PTR ingestion (once per job, not per iteration)
        try:
            from datetime import date as date_cls
            year = date_cls.today().year
            with Session(engine) as session:
                rows = fetch_filing_index(year)
                total_ptr_new = sync_filing_index(session, rows, year)
                total_ptr_parsed = process_ptr_pending(session, year, max_count=50)
                log(f"PTR: {total_ptr_new} new filings, {total_ptr_parsed} parsed")
        except Exception as e:
            log(f"PTR error: {e}")

        # Send PTR email alerts (separate from FEC alerts)
        if total_ptr_parsed > 0:
            try:
                with Session(engine) as session:
                    if get_ptr_email_enabled(session):
                        # Find ingested filings not yet emailed
                        unemailed = session.exec(
                            select(PtrFiling)
                            .where(PtrFiling.status == "ingested")
                            .where(PtrFiling.emailed_at == None)
                        ).all()
                        if unemailed:
                            filings_data = []
                            for f in unemailed:
                                filer_name = " ".join(
                                    p for p in [f.prefix, f.first_name, f.last_name, f.suffix] if p
                                )
                                # Get transaction count and tickers
                                txns = session.exec(
                                    select(PtrTransaction)
                                    .where(PtrTransaction.doc_id == f.doc_id)
                                ).all()
                                tickers = ", ".join(sorted(set(
                                    t.ticker for t in txns if t.ticker
                                )))
                                filings_data.append({
                                    "filer_name": filer_name,
                                    "state_district": f.state_district,
                                    "filing_date": str(f.filing_date) if f.filing_date else "",
                                    "transaction_count": len(txns),
                                    "tickers": tickers,
                                    "pdf_url": f.pdf_url or "",
                                })
                            sent = send_ptr_alert(session, filings_data)
                            if sent:
                                now = datetime.now(timezone.utc)
                                for f in unemailed:
                                    f.emailed_at = now
                                    session.add(f)
                                session.commit()
                                log(f"Sent PTR alert for {len(unemailed)} filings to {len(sent)} recipients")
                    else:
                        log("PTR email alerts disabled in config, skipping")
            except Exception as e:
                log(f"PTR email error (non-fatal): {e}")

        results["ptr_new"] = total_ptr_new
        results["ptr_parsed"] = total_ptr_parsed

        elapsed = (time.time() - start_time) / 60
        log(f"Job complete in {elapsed:.1f} min over {iteration} iterations")
        log(f"Totals: F3X={total_f3x}, IE filings={total_ie_filings}, "
            f"IE events={total_ie_events}, PTR new={total_ptr_new}, "
            f"PTR parsed={total_ptr_parsed}")

        # Send email if we found new high-value filings
        if total_f3x > 0 or total_ie_events > 0:
            log("Checking for email alerts...")
            try:
                with Session(engine) as session:
                    # Check if emails are enabled
                    if not get_email_enabled(session):
                        log("Email alerts disabled in config, skipping")
                    else:
                        today_start = datetime.now(timezone.utc).replace(
                            hour=0, minute=0, second=0, microsecond=0
                        )
                        all_sent_to = set()

                        if total_f3x > 0:
                            stmt = (
                                select(FilingF3X)
                                .where(FilingF3X.filed_at_utc >= today_start)
                                .where(FilingF3X.total_receipts >= settings.receipts_threshold)
                                .where(FilingF3X.emailed_at == None)
                                .order_by(FilingF3X.filed_at_utc.desc())
                                .limit(50)
                            )
                            f3x_filings = session.exec(stmt).all()
                            if f3x_filings:
                                filings_data = [{
                                    "committee_name": f.committee_name,
                                    "committee_id": f.committee_id,
                                    "form_type": f.form_type,
                                    "report_type": f.report_type,
                                    "coverage_from": str(f.coverage_from) if f.coverage_from else None,
                                    "coverage_through": str(f.coverage_through) if f.coverage_through else None,
                                    "filed_at_utc": str(f.filed_at_utc)[:16] if f.filed_at_utc else None,
                                    "total_receipts": f.total_receipts,
                                    "fec_url": f.fec_url,
                                } for f in f3x_filings]
                                sent = send_filing_alert(session, "3x", filings_data)
                                if sent:
                                    now = datetime.now(timezone.utc)
                                    for f in f3x_filings:
                                        f.emailed_at = now
                                        session.add(f)
                                    mark_tasks_emailed(
                                        session,
                                        [f.filing_id for f in f3x_filings],
                                        "F3X",
                                    )
                                    session.commit()
                                    all_sent_to.update(sent.keys())
                                    log(f"Sent F3X alert for {len(f3x_filings)} filings to {len(sent)} recipients")

                        if total_ie_events > 0:
                            stmt = (
                                select(IEScheduleE)
                                .where(IEScheduleE.filed_at_utc >= today_start)
                                .where(IEScheduleE.emailed_at == None)
                                .order_by(IEScheduleE.filed_at_utc.desc())
                                .limit(50)
                            )
                            ie_events_list = session.exec(stmt).all()
                            if ie_events_list:
                                events_data = [{
                                    "committee_name": e.committee_name,
                                    "committee_id": e.committee_id,
                                    "candidate_name": e.candidate_name,
                                    "candidate_id": e.candidate_id,
                                    "candidate_office": e.candidate_office,
                                    "candidate_state": e.candidate_state,
                                    "candidate_district": e.candidate_district,
                                    "candidate_party": e.candidate_party,
                                    "support_oppose": e.support_oppose,
                                    "purpose": e.purpose,
                                    "payee_name": e.payee_name,
                                    "expenditure_date": str(e.expenditure_date) if e.expenditure_date else None,
                                    "amount": e.amount,
                                    "fec_url": e.fec_url,
                                } for e in ie_events_list]
                                sent = send_filing_alert(session, "e", events_data)
                                if sent:
                                    now = datetime.now(timezone.utc)
                                    for e in ie_events_list:
                                        e.emailed_at = now
                                        session.add(e)
                                    ie_fids = list(set(e.filing_id for e in ie_events_list))
                                    mark_tasks_emailed(session, ie_fids)
                                    session.commit()
                                    all_sent_to.update(sent.keys())
                                    log(f"Sent IE alert for {len(ie_events_list)} events to {len(sent)} recipients")

                        if all_sent_to:
                            results["email_sent"] = True
                            results["emails_sent_to"] = sorted(all_sent_to)

            except Exception as e:
                log(f"Email error (non-fatal): {e}")

        has_errors = total_f3x_failed > 0
        results["status"] = "completed_with_errors" if has_errors else "success"
        results["completed_at"] = datetime.now(timezone.utc).isoformat()
        save_job_status(engine, results)

        log("Job finished successfully")
        return 0

    except Exception as e:
        log(f"Job crashed: {e}")
        results["status"] = "crashed"
        results["crash_error"] = str(e)
        results["completed_at"] = datetime.now(timezone.utc).isoformat()
        save_job_status(engine, results)
        raise


if __name__ == "__main__":
    sys.exit(run_job())
