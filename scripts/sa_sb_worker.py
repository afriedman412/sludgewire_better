"""Cloud Run Job entrypoint for the SA/SB worker.

Same image, two deploys (small + big). Mode is controlled by the
SA_SB_WORKER_MODE env var ("small" or "big"). Defaults to "small".

Loops calling run_sa_sb until the queue empties or the runtime limit
is reached, then sends an email alert if any filings were skipped or
streamed (so degraded-accuracy events get human visibility).
"""
from __future__ import annotations

import gc
import json
import os
import sys
import time
from datetime import datetime, timezone

from sqlmodel import Session

sys.path.insert(0, "/app")

from app.db import init_db, make_engine
from app.email_service import send_sa_sb_attention_alert
from app.ingest_sa_sb import run_sa_sb
from app.repo import get_email_enabled
from app.schemas import AppConfig
from app.settings import load_settings


MAX_ITERATIONS = 50
MAX_RUNTIME_MINUTES = 55  # leave buffer before 60min Cloud Run timeout
PAUSE_BETWEEN_BATCHES = 2


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _save_job_status(engine, mode: str, results: dict) -> None:
    key = f"last_sa_sb_run_{mode}"
    with Session(engine) as session:
        entry = session.get(AppConfig, key)
        if entry is None:
            entry = AppConfig(key=key, value=json.dumps(results))
        else:
            entry.value = json.dumps(results)
            entry.updated_at = datetime.now(timezone.utc)
        session.add(entry)
        session.commit()


def run_job(mode: str) -> int:
    if mode not in ("small", "big"):
        log(f"Invalid SA_SB_WORKER_MODE={mode!r}; must be 'small' or 'big'")
        return 2

    log(f"Starting SA/SB worker mode={mode}")
    settings = load_settings()
    engine = make_engine(settings)
    init_db(engine)

    start_time = time.time()
    iteration = 0
    totals = {
        "filings_processed": 0,
        "sa_inserted": 0,
        "sb_inserted": 0,
        "failed": 0,
        "skipped_too_large": 0,
        "streamed": 0,
        "handed_off": 0,
    }
    skipped_filing_ids: list[int] = []
    streamed_filing_ids: list[int] = []
    failed_filing_ids: list[int] = []
    last_error: str | None = None

    results = {
        "mode": mode,
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
        "iterations": 0,
        **totals,
    }
    _save_job_status(engine, mode, results)

    while iteration < MAX_ITERATIONS:
        iteration += 1
        elapsed_min = (time.time() - start_time) / 60
        if elapsed_min >= MAX_RUNTIME_MINUTES:
            log(f"Reached runtime limit ({MAX_RUNTIME_MINUTES} min)")
            break

        log(f"=== iteration {iteration} (elapsed {elapsed_min:.1f} min) ===")
        with Session(engine) as session:
            try:
                batch = run_sa_sb(session, mode=mode)
            except Exception as e:
                log(f"run_sa_sb raised: {e}")
                last_error = str(e)
                break

        totals["filings_processed"] += batch.filings_processed
        totals["sa_inserted"] += batch.sa_inserted
        totals["sb_inserted"] += batch.sb_inserted
        totals["failed"] += batch.failed_count
        totals["skipped_too_large"] += batch.skipped_too_large_count
        totals["streamed"] += batch.streamed_count
        totals["handed_off"] += batch.handed_off_count
        skipped_filing_ids.extend(batch.skipped_filing_ids)
        streamed_filing_ids.extend(batch.streamed_filing_ids)
        failed_filing_ids.extend(batch.failed_filing_ids)
        if batch.last_error:
            last_error = batch.last_error

        results.update(totals)
        results["iterations"] = iteration
        if last_error:
            results["last_error"] = last_error
        _save_job_status(engine, mode, results)

        # Queue empty? bail.
        if (
            batch.filings_processed == 0
            and batch.handed_off_count == 0
            and batch.skipped_too_large_count == 0
        ):
            log("queue empty for this mode, stopping")
            break

        gc.collect()
        time.sleep(PAUSE_BETWEEN_BATCHES)

    elapsed = (time.time() - start_time) / 60
    log(
        f"worker mode={mode} done in {elapsed:.1f} min: "
        f"{json.dumps(totals)}"
    )

    # Email alert on attention-worthy results.
    if (
        totals["skipped_too_large"] > 0
        or totals["streamed"] > 0
        or totals["failed"] > 0
    ):
        try:
            with Session(engine) as session:
                if get_email_enabled(session):
                    send_sa_sb_attention_alert(
                        session,
                        mode=mode,
                        skipped_filing_ids=skipped_filing_ids,
                        streamed_filing_ids=streamed_filing_ids,
                        failed_filing_ids=failed_filing_ids,
                    )
                    log(
                        f"sent SA/SB attention alert "
                        f"(skipped={len(skipped_filing_ids)}, "
                        f"streamed={len(streamed_filing_ids)}, "
                        f"failed={len(failed_filing_ids)})"
                    )
                else:
                    log("email_enabled=false, skipping attention alert")
        except Exception as e:
            log(f"attention email error (non-fatal): {e}")

    results["status"] = "success" if totals["failed"] == 0 else "completed_with_errors"
    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    _save_job_status(engine, mode, results)
    return 0


if __name__ == "__main__":
    mode = os.environ.get("SA_SB_WORKER_MODE", "small")
    sys.exit(run_job(mode))
