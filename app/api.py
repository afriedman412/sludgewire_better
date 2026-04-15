# app/api.py
from __future__ import annotations

from datetime import datetime, date, time, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Any, Dict
from pathlib import Path
import threading

from fastapi import FastAPI, Depends, Query, Request, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from sqlmodel import Session, select, col

from .settings import load_settings
from .db import make_engine, init_db
from .schemas import FilingF3X, IEScheduleE, EmailRecipient, BackfillJob, AppConfig, IngestionTask, ScheduleA, ScheduleB
import json
from .auth import verify_admin
from .repo import DEFAULT_MAX_NEW_PER_RUN, get_email_enabled, get_ptr_email_enabled, get_pac_groups, set_pac_groups

# --- App + DB bootstrap ---
settings = load_settings()
engine = make_engine(settings)
try:
    init_db(engine)
except Exception as _e:  # noqa: BLE001
    # Don't block import if the DB is unreachable (e.g. during test
    # collection or local dev without the proxy running). Routes
    # using get_session() will fail loudly when actually invoked.
    print(f"[api] init_db skipped: {_e}")

app = FastAPI(title="FEC Monitor", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate-limiting for current-day ingestion (in-memory, resets on deploy)
_last_ingestion_time: Optional[datetime] = None
_ingestion_lock = threading.Lock()
INGESTION_COOLDOWN_MINUTES = 5
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=BASE_DIR / "templates")

ET = ZoneInfo("America/New_York")


def get_session():
    with Session(engine) as session:
        yield session


def format_currency(value):
    if value is None:
        return ""
    return f"${value:,.2f}"


templates.env.filters["format_currency"] = format_currency


def et_today_utc_bounds(now_utc: Optional[datetime] = None) -> tuple[datetime, datetime]:
    """
    Return [start_utc, end_utc) for "today" in America/New_York.
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    today_et = now_utc.astimezone(ET).date()
    start_et = datetime.combine(today_et, time(0, 0, 0), tzinfo=ET)
    tomorrow_et = date.fromordinal(today_et.toordinal() + 1)
    end_et = datetime.combine(tomorrow_et, time(0, 0, 0), tzinfo=ET)

    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc)


def _model_dump(obj: Any) -> Dict[str, Any]:
    """
    SQLModel uses Pydantic under the hood. model_dump exists on newer pydantic.
    Fall back to dict() if needed.
    """
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    return obj.dict()  # type: ignore[attr-defined]


@app.get("/healthz")
def healthz():
    return {"ok": True}


# -------------------------
# Dashboards (HTML)
# -------------------------

def _sa_sb_status_map(
    session: Session, filing_ids: list[int]
) -> dict[int, str]:
    """Return {filing_id: status} for SA_SB tasks of the given filings.
    Filings without a task row are absent from the result."""
    if not filing_ids:
        return {}
    rows = session.exec(
        select(IngestionTask.filing_id, IngestionTask.status)
        .where(IngestionTask.source_feed == "SA_SB")
        .where(col(IngestionTask.filing_id).in_(filing_ids))
    ).all()
    return {r[0]: r[1] for r in rows}

@app.get("/dashboard/3x", response_class=HTMLResponse)
def dashboard_3x(
    request: Request,
    session: Session = Depends(get_session),
    threshold: float = Query(default=50_000, ge=0),
    limit: int = Query(default=200, ge=1, le=2000),
):
    # Trigger rate-limited ingestion on page load
    _maybe_run_ingestion()

    start_utc, end_utc = et_today_utc_bounds()
    today = start_utc.astimezone(ET).date()

    stmt = (
        select(FilingF3X)
        .where(FilingF3X.filed_at_utc >= start_utc)
        .where(FilingF3X.filed_at_utc < end_utc)
    )
    if threshold > 0:
        stmt = stmt.where(FilingF3X.total_receipts != None)  # noqa: E711
        stmt = stmt.where(FilingF3X.total_receipts >= threshold)
    stmt = stmt.order_by(FilingF3X.filed_at_utc.desc()).limit(limit)
    rows = session.exec(stmt).all()
    sa_sb_status = _sa_sb_status_map(
        session, [r.filing_id for r in rows])

    return templates.TemplateResponse(
        "dashboard_3x.html",
        {
            "request": request,
            "rows": rows,
            "sa_sb_status": sa_sb_status,
            "threshold": threshold,
            "day_et": today,
            "prev_date": today - timedelta(days=1),
            "next_date": today + timedelta(days=1),
            "today": today,
            "nav_date": today,
            "api_url": f"/api/3x/today?threshold={threshold}&limit={limit}",
        },
    )


@app.get("/dashboard/e", response_class=HTMLResponse)
def dashboard_e(
    request: Request,
    session: Session = Depends(get_session),
    limit: int = Query(default=200, ge=1, le=2000),
):
    """
    Shows Schedule E EVENTS filed today (ET).
    If you want "filings" instead, we can switch this to a DISTINCT filing_id query.
    """
    # Trigger rate-limited ingestion on page load
    _maybe_run_ingestion()

    start_utc, end_utc = et_today_utc_bounds()
    today = start_utc.astimezone(ET).date()

    stmt = (
        select(IEScheduleE)
        .where(IEScheduleE.filed_at_utc >= start_utc)
        .where(IEScheduleE.filed_at_utc < end_utc)
        .order_by(IEScheduleE.filed_at_utc.desc())
        .limit(limit)
    )
    rows = session.exec(stmt).all()

    return templates.TemplateResponse(
        "dashboard_e.html",
        {
            "request": request,
            "rows": rows,
            "day_et": today,
            "prev_date": today - timedelta(days=1),
            "next_date": today + timedelta(days=1),
            "today": today,
            "nav_date": today,
            "api_url": f"/api/e/today?limit={limit}",
        },
    )


# -------------------------
# Filing detail pages (donors / recipients)
# -------------------------

# Priority value used when a user accesses an unfinished detail page.
# 100x larger than any plausible filing_id (~10^7), so user-bumped rows
# always sort to the front of the SA/SB worker queue. Sized to fit in
# Postgres INTEGER (max ~2.1B).
USER_BUMP_PRIORITY = 1_000_000_000


def _render_filing_detail(
    request: Request,
    session: Session,
    filing_id: int,
    *,
    kind: str,
):
    """Render the donors or recipients detail page for an F3X filing.

    kind: "donors" (Schedule A) or "recipients" (Schedule B).

    Behavior by SA/SB task status:
      - ingested → render rows from schedule_a / schedule_b
      - claimed/downloading/parsing → "loading" with auto-refresh +
        priority bump so the user-accessed filing jumps the queue
      - skipped → friendly "too large" message
      - failed → error message with details
      - no task row → enqueue at user priority and show "queued"
    """
    from .repo import enqueue_sa_sb_task

    filing = session.get(FilingF3X, filing_id)
    task = session.get(IngestionTask, (filing_id, "SA_SB"))
    status = task.status if task else None
    error_message = task.error_message if task else None
    file_size_mb = task.file_size_mb if task else None

    priority_bumped = False
    if status in (None, "claimed", "downloading", "parsing"):
        # If no task row yet, enqueue one at user priority.
        # If task is in-flight, bump priority so the next free worker
        # grabs it ahead of normal queue items.
        if filing is not None:
            enqueue_sa_sb_task(
                session, filing_id,
                fec_url=filing.fec_url,
                priority=USER_BUMP_PRIORITY,
            )
            session.commit()
            priority_bumped = True

    rows: list = []
    if status == "ingested":
        if kind == "donors":
            rows = session.exec(
                select(ScheduleA)
                .where(ScheduleA.filing_id == filing_id)
                .order_by(ScheduleA.contribution_date.desc().nullslast(),
                          ScheduleA.contribution_amount.desc().nullslast())
            ).all()
        else:
            rows = session.exec(
                select(ScheduleB)
                .where(ScheduleB.filing_id == filing_id)
                .order_by(ScheduleB.disbursement_date.desc().nullslast(),
                          ScheduleB.disbursement_amount.desc().nullslast())
            ).all()

    return templates.TemplateResponse(
        "filing_detail.html",
        {
            "request": request,
            "filing_id": filing_id,
            "filing": filing,
            "status": status,
            "error_message": error_message,
            "file_size_mb": file_size_mb,
            "rows": rows,
            "kind": kind,
            "kind_label": "Donors" if kind == "donors" else "Recipients",
            "kind_lower": "donors" if kind == "donors" else "recipients",
            "priority_bumped": priority_bumped,
            "nav_date": (
                filing.filed_at_utc.date() if filing
                and filing.filed_at_utc else None
            ),
        },
    )


@app.get("/filing/{filing_id}/donors", response_class=HTMLResponse)
def filing_donors(
    filing_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    return _render_filing_detail(
        request, session, filing_id, kind="donors")


@app.get("/filing/{filing_id}/recipients", response_class=HTMLResponse)
def filing_recipients(
    filing_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    return _render_filing_detail(
        request, session, filing_id, kind="recipients")


# -------------------------
# JSON API (Today)
# -------------------------

@app.get("/api/3x/today")
def api_3x_today(
    session: Session = Depends(get_session),
    threshold: float = Query(default=50_000, ge=0),
    limit: int = Query(default=200, ge=1, le=5000),
):
    start_utc, end_utc = et_today_utc_bounds()

    stmt = (
        select(FilingF3X)
        .where(FilingF3X.filed_at_utc >= start_utc)
        .where(FilingF3X.filed_at_utc < end_utc)
        .where(FilingF3X.total_receipts != None)  # noqa: E711
        .where(FilingF3X.total_receipts >= threshold)
        .order_by(FilingF3X.filed_at_utc.desc())
        .limit(limit)
    )
    rows = session.exec(stmt).all()

    return {
        "day_et": start_utc.astimezone(ET).date().isoformat(),
        "threshold": threshold,
        "count": len(rows),
        "results": [_model_dump(r) for r in rows],
    }


@app.get("/api/e/today")
def api_e_today(
    session: Session = Depends(get_session),
    limit: int = Query(default=500, ge=1, le=5000),
):
    start_utc, end_utc = et_today_utc_bounds()

    stmt = (
        select(IEScheduleE)
        .where(IEScheduleE.filed_at_utc >= start_utc)
        .where(IEScheduleE.filed_at_utc < end_utc)
        .order_by(IEScheduleE.filed_at_utc.desc())
        .limit(limit)
    )
    rows = session.exec(stmt).all()

    return {
        "day_et": start_utc.astimezone(ET).date().isoformat(),
        "count": len(rows),
        "results": [_model_dump(r) for r in rows],
    }


# -------------------------
# JSON API (General query)
# -------------------------

@app.get("/api/3x")
def api_3x_query(
    session: Session = Depends(get_session),
    committee_id: Optional[str] = None,
    min_receipts: Optional[float] = Query(default=None, ge=0),
    filed_after_utc: Optional[datetime] = None,
    filed_before_utc: Optional[datetime] = None,
    limit: int = Query(default=200, ge=1, le=5000),
):
    stmt = select(FilingF3X)

    if committee_id:
        stmt = stmt.where(FilingF3X.committee_id == committee_id)
    if min_receipts is not None:
        stmt = stmt.where(FilingF3X.total_receipts != None)  # noqa: E711
        stmt = stmt.where(FilingF3X.total_receipts >= min_receipts)
    if filed_after_utc:
        stmt = stmt.where(FilingF3X.filed_at_utc >= filed_after_utc)
    if filed_before_utc:
        stmt = stmt.where(FilingF3X.filed_at_utc < filed_before_utc)

    stmt = stmt.order_by(FilingF3X.filed_at_utc.desc()).limit(limit)
    rows = session.exec(stmt).all()
    return {"count": len(rows), "results": [_model_dump(r) for r in rows]}


@app.get("/api/e")
def api_e_query(
    session: Session = Depends(get_session),
    filer_id: Optional[str] = None,
    candidate_id: Optional[str] = None,
    min_amount: Optional[float] = Query(default=None, ge=0),
    filed_after_utc: Optional[datetime] = None,
    filed_before_utc: Optional[datetime] = None,
    limit: int = Query(default=500, ge=1, le=5000),
):
    stmt = select(IEScheduleE)

    if filer_id:
        stmt = stmt.where(col(IEScheduleE.filer_id) == filer_id)
    if candidate_id:
        stmt = stmt.where(col(IEScheduleE.candidate_id) == candidate_id)
    if min_amount is not None:
        stmt = stmt.where(IEScheduleE.amount != None)  # noqa: E711
        stmt = stmt.where(IEScheduleE.amount >= min_amount)
    if filed_after_utc:
        stmt = stmt.where(IEScheduleE.filed_at_utc >= filed_after_utc)
    if filed_before_utc:
        stmt = stmt.where(IEScheduleE.filed_at_utc < filed_before_utc)

    stmt = stmt.order_by(IEScheduleE.filed_at_utc.desc()).limit(limit)
    rows = session.exec(stmt).all()
    return {"count": len(rows), "results": [_model_dump(r) for r in rows]}


# -------------------------
# Config Endpoints (Protected)
# -------------------------

def _get_memory_usage_mb() -> float:
    """Get current process memory usage in MB."""
    try:
        import resource
        # Returns bytes on Linux, need to convert
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # On macOS it's bytes, on Linux it's KB
        import platform
        if platform.system() == "Darwin":
            return usage / (1024 * 1024)
        else:
            return usage / 1024
    except Exception:
        return 0.0


# -------------------------
# Category View API
# -------------------------


@app.get("/api/category/groups")
def api_category_groups(session: Session = Depends(get_session)):
    """All PAC groups with summary totals from schedule_a and ie_schedule_e."""
    groups = get_pac_groups(session)
    if not groups:
        return {"groups": []}

    result_groups = []
    for group in groups:
        group_name = group.get("name", "")
        pacs = group.get("pacs", [])
        committee_ids = [p["committee_id"] for p in pacs if "committee_id" in p]

        if not committee_ids:
            result_groups.append({
                "name": group_name,
                "pacs": pacs,
                "total_contributions": 0,
                "donor_count": 0,
                "total_ie_spending": 0,
            })
            continue

        sa_row = session.execute(text("""
            SELECT
                COALESCE(SUM(sa.contribution_amount), 0) AS total,
                COUNT(DISTINCT sa.contributor_name) AS donors
            FROM schedule_a sa
            WHERE sa.committee_id = ANY(:ids)
              AND sa.contribution_amount > 0
        """), {"ids": committee_ids}).first()

        ie_row = session.execute(text("""
            SELECT COALESCE(SUM(ie.amount), 0) AS total
            FROM ie_schedule_e ie
            WHERE ie.committee_id = ANY(:ids)
              AND ie.amount > 0
        """), {"ids": committee_ids}).first()

        result_groups.append({
            "name": group_name,
            "pacs": pacs,
            "total_contributions": float(sa_row.total) if sa_row else 0,
            "donor_count": int(sa_row.donors) if sa_row else 0,
            "total_ie_spending": float(ie_row.total) if ie_row else 0,
        })

    return {"groups": result_groups}


@app.get("/api/category/pac/{committee_id}/industries")
def api_pac_industries(
    committee_id: str,
    session: Session = Depends(get_session),
    limit: int = Query(default=50, ge=1, le=500),
):
    """Industry breakdown for a PAC's Schedule A contributions."""
    rows = session.execute(text("""
        SELECT
            COALESCE(
                NULLIF(o.industry, ''),
                NULLIF(e.industry, ''),
                NULLIF(d.industry, ''),
                'Unclassified'
            ) AS industry,
            COALESCE(
                NULLIF(o.sector, ''),
                NULLIF(e.sector, ''),
                NULLIF(d.sector, ''),
                'Unclassified'
            ) AS sector,
            SUM(sa.contribution_amount) AS total_amount,
            COUNT(*) AS contribution_count,
            COUNT(DISTINCT sa.contributor_name) AS donor_count
        FROM schedule_a sa
        LEFT JOIN donor_industries d ON UPPER(sa.contributor_name) = d.name_upper
        LEFT JOIN org_industries o ON UPPER(sa.contributor_name) = o.org_upper
        LEFT JOIN org_industries e ON UPPER(sa.contributor_employer) = e.org_upper
        WHERE sa.committee_id = :committee_id
          AND sa.contribution_amount > 0
        GROUP BY 1, 2
        ORDER BY total_amount DESC
        LIMIT :limit
    """), {"committee_id": committee_id, "limit": limit}).all()

    return {
        "committee_id": committee_id,
        "count": len(rows),
        "results": [
            {
                "industry": r.industry,
                "sector": r.sector,
                "total_amount": float(r.total_amount) if r.total_amount else 0,
                "contribution_count": r.contribution_count,
                "donor_count": r.donor_count,
            }
            for r in rows
        ],
    }


@app.get("/api/category/pac/{committee_id}/donors")
def api_pac_donors(
    committee_id: str,
    session: Session = Depends(get_session),
    industry: Optional[str] = None,
    limit: int = Query(default=100, ge=1, le=1000),
):
    """Top donors for a PAC with industry classification."""
    params: dict[str, Any] = {"committee_id": committee_id, "limit": limit}

    industry_filter = ""
    if industry:
        industry_filter = """
        HAVING COALESCE(
            NULLIF(MAX(o.industry), ''),
            NULLIF(MAX(e.industry), ''),
            NULLIF(MAX(d.industry), ''),
            'Unclassified'
        ) = :industry
        """
        params["industry"] = industry

    rows = session.execute(text(f"""
        SELECT
            sa.contributor_name,
            MAX(sa.contributor_employer) AS employer,
            MAX(sa.contributor_occupation) AS occupation,
            MAX(sa.contributor_type) AS contributor_type,
            COALESCE(
                NULLIF(MAX(o.industry), ''),
                NULLIF(MAX(e.industry), ''),
                NULLIF(MAX(d.industry), ''),
                'Unclassified'
            ) AS industry,
            COALESCE(
                NULLIF(MAX(o.sector), ''),
                NULLIF(MAX(e.sector), ''),
                NULLIF(MAX(d.sector), ''),
                'Unclassified'
            ) AS sector,
            SUM(sa.contribution_amount) AS total_amount,
            COUNT(*) AS contribution_count
        FROM schedule_a sa
        LEFT JOIN donor_industries d ON UPPER(sa.contributor_name) = d.name_upper
        LEFT JOIN org_industries o ON UPPER(sa.contributor_name) = o.org_upper
        LEFT JOIN org_industries e ON UPPER(sa.contributor_employer) = e.org_upper
        WHERE sa.committee_id = :committee_id
          AND sa.contribution_amount > 0
        GROUP BY sa.contributor_name
        {industry_filter}
        ORDER BY total_amount DESC
        LIMIT :limit
    """), params).all()

    return {
        "committee_id": committee_id,
        "count": len(rows),
        "results": [
            {
                "contributor_name": r.contributor_name,
                "employer": r.employer,
                "occupation": r.occupation,
                "contributor_type": r.contributor_type,
                "industry": r.industry,
                "sector": r.sector,
                "total_amount": float(r.total_amount) if r.total_amount else 0,
                "contribution_count": r.contribution_count,
            }
            for r in rows
        ],
    }


@app.get("/api/category/pac/{committee_id}/candidates")
def api_pac_candidates(
    committee_id: str,
    session: Session = Depends(get_session),
    limit: int = Query(default=50, ge=1, le=500),
):
    """IE spending by candidate for a PAC."""
    rows = session.execute(text("""
        SELECT
            ie.candidate_id,
            MAX(ie.candidate_name) AS candidate_name,
            MAX(ie.candidate_party) AS candidate_party,
            MAX(ie.candidate_office) AS candidate_office,
            ie.candidate_state,
            ie.candidate_district,
            ie.support_oppose,
            SUM(ie.amount) AS total_amount,
            COUNT(*) AS expenditure_count,
            MAX(COALESCE(ie.expenditure_date, ie.filed_at_utc::date)) AS latest_date,
            MIN(COALESCE(ie.expenditure_date, ie.filed_at_utc::date)) AS earliest_date
        FROM ie_schedule_e ie
        WHERE ie.committee_id = :committee_id
          AND ie.amount > 0
        GROUP BY
            ie.candidate_id,
            ie.candidate_state, ie.candidate_district,
            ie.support_oppose
        ORDER BY total_amount DESC
        LIMIT :limit
    """), {"committee_id": committee_id, "limit": limit}).all()

    return {
        "committee_id": committee_id,
        "count": len(rows),
        "results": [
            {
                "candidate_id": r.candidate_id,
                "candidate_name": r.candidate_name,
                "candidate_party": r.candidate_party,
                "candidate_office": r.candidate_office,
                "candidate_state": r.candidate_state,
                "candidate_district": r.candidate_district,
                "support_oppose": r.support_oppose,
                "total_amount": float(r.total_amount) if r.total_amount else 0,
                "expenditure_count": r.expenditure_count,
                "latest_date": str(r.latest_date) if r.latest_date else None,
                "earliest_date": str(r.earliest_date) if r.earliest_date else None,
            }
            for r in rows
        ],
    }


@app.get("/api/races/{state}/{race_key}/candidates")
def api_race_candidates(
    state: str,
    race_key: str,
    session: Session = Depends(get_session),
):
    """All known candidates for a race (state+office+district), including opponents
    with no IE spending. race_key format: 'SEN' or '01', '02', etc.
    Auto-fetches from FEC API if fewer than 2 candidates are cached."""
    state = state.upper()
    if race_key.upper() == "SEN":
        office = "S"
        district = ""
    else:
        office = "H"
        district = race_key.zfill(2)

    rows = session.execute(text("""
        SELECT candidate_id, candidate_name, party, state, office, district,
               has_ie_spending
        FROM race_candidates
        WHERE state = :state AND office = :office
          AND (:district = '' OR district = :district OR district = '')
        ORDER BY has_ie_spending DESC, party, candidate_name
    """), {"state": state, "office": office, "district": district}).all()

    # Auto-fetch from FEC if we have fewer than 2 candidates
    if len(rows) < 2:
        try:
            rows = _backfill_race_from_fec(session, state, office, district, rows)
        except Exception as e:
            print(f"[race_candidates] FEC backfill error for {state}-{race_key}: {e}")

    return {
        "state": state,
        "race_key": race_key,
        "count": len(rows),
        "results": [
            {
                "candidate_id": r.candidate_id,
                "candidate_name": r.candidate_name,
                "party": r.party,
                "state": r.state,
                "office": r.office,
                "district": r.district,
                "has_ie_spending": r.has_ie_spending,
            }
            for r in rows
        ],
    }


def _backfill_race_from_fec(session, state, office, district, existing_rows):
    """Fetch top funded candidates from FEC API and cache in race_candidates."""
    import requests
    from .schemas import RaceCandidate

    api_key = settings.gov_api_key or "DEMO_KEY"
    params = {
        "state": state,
        "office": office,
        "election_year": 2026,
        "has_raised_funds": "true",
        "is_active_candidate": "true",
        "api_key": api_key,
        "per_page": 10,
    }
    if office == "H" and district:
        params["district"] = district.lstrip("0") or "0"

    resp = requests.get("https://api.open.fec.gov/v1/candidates/",
                        params=params, timeout=15)
    if resp.status_code != 200:
        return existing_rows

    fec_cands = resp.json().get("results", [])
    existing_ids = {r.candidate_id for r in existing_rows}
    new_rows = list(existing_rows)

    for fc in fec_cands:
        cid = fc.get("candidate_id")
        if not cid or cid in existing_ids:
            continue
        party = fc.get("party")
        if not party:
            continue

        rc = RaceCandidate(
            candidate_id=cid,
            candidate_name=fc.get("name"),
            party=party,
            state=state,
            office=office,
            district=district or "",
            has_ie_spending=False,
        )
        session.add(rc)
        existing_ids.add(cid)
        # Mimic the row shape for the response
        new_rows.append(rc)

    session.commit()
    race_label = f"{state}-{'SEN' if office == 'S' else district or '??'}"
    print(f"[race_candidates] Backfilled {len(new_rows) - len(existing_rows)} candidates for {race_label} from FEC")
    return new_rows


@app.get("/api/category/pac/{committee_id}/candidates/{candidate_id}/industries")
def api_candidate_industry_attribution(
    committee_id: str,
    candidate_id: str,
    session: Session = Depends(get_session),
    limit: int = Query(default=50, ge=1, le=500),
):
    """Attributed industry spending for a candidate from a PAC."""
    # Total contributions to this PAC
    total_row = session.execute(text("""
        SELECT COALESCE(SUM(contribution_amount), 0) AS total
        FROM schedule_a
        WHERE committee_id = :committee_id
          AND contribution_amount > 0
    """), {"committee_id": committee_id}).first()

    total_contributions = float(total_row.total) if total_row and total_row.total else 0

    if total_contributions == 0:
        return {
            "committee_id": committee_id,
            "candidate_id": candidate_id,
            "total_contributions": 0,
            "total_ie_spending": 0,
            "results": [],
        }

    # IE spending on this candidate
    ie_row = session.execute(text("""
        SELECT COALESCE(SUM(amount), 0) AS total_ie
        FROM ie_schedule_e
        WHERE committee_id = :committee_id
          AND candidate_id = :candidate_id
          AND amount > 0
    """), {"committee_id": committee_id, "candidate_id": candidate_id}).first()

    total_ie = float(ie_row.total_ie) if ie_row else 0

    # Industry breakdown with proportional attribution
    rows = session.execute(text("""
        SELECT
            COALESCE(
                NULLIF(o.industry, ''),
                NULLIF(e.industry, ''),
                NULLIF(d.industry, ''),
                'Unclassified'
            ) AS industry,
            COALESCE(
                NULLIF(o.sector, ''),
                NULLIF(e.sector, ''),
                NULLIF(d.sector, ''),
                'Unclassified'
            ) AS sector,
            SUM(sa.contribution_amount) AS industry_contributions,
            COUNT(DISTINCT sa.contributor_name) AS donor_count
        FROM schedule_a sa
        LEFT JOIN donor_industries d ON UPPER(sa.contributor_name) = d.name_upper
        LEFT JOIN org_industries o ON UPPER(sa.contributor_name) = o.org_upper
        LEFT JOIN org_industries e ON UPPER(sa.contributor_employer) = e.org_upper
        WHERE sa.committee_id = :committee_id
          AND sa.contribution_amount > 0
        GROUP BY 1, 2
        ORDER BY industry_contributions DESC
        LIMIT :limit
    """), {"committee_id": committee_id, "limit": limit}).all()

    return {
        "committee_id": committee_id,
        "candidate_id": candidate_id,
        "total_contributions": total_contributions,
        "total_ie_spending": total_ie,
        "count": len(rows),
        "results": [
            {
                "industry": r.industry,
                "sector": r.sector,
                "industry_contributions": float(r.industry_contributions),
                "contribution_share": float(r.industry_contributions) / total_contributions,
                "attributed_ie_spending": (float(r.industry_contributions) / total_contributions) * total_ie,
                "donor_count": r.donor_count,
            }
            for r in rows
        ],
    }


@app.get("/config", response_class=HTMLResponse)
def config_page(
    request: Request,
    session: Session = Depends(get_session),
    _: str = Depends(verify_admin),
    message: Optional[str] = Query(default=None),
    message_type: Optional[str] = Query(default=None),
):
    """Password-protected config page for managing email recipients."""
    recipients = session.exec(
        select(EmailRecipient).order_by(EmailRecipient.created_at.desc())
    ).all()

    # Get pending/running backfill jobs
    backfill_jobs = session.exec(
        select(BackfillJob)
        .where(BackfillJob.status.in_(["pending", "running"]))
        .order_by(BackfillJob.started_at.desc())
    ).all()

    # Get memory usage
    memory_mb = _get_memory_usage_mb()

    # Get last cron run status
    last_cron_run = None
    cron_config = session.get(AppConfig, "last_cron_run")
    if cron_config and cron_config.value:
        try:
            last_cron_run = json.loads(cron_config.value)
        except json.JSONDecodeError:
            pass

    # Get max_new_per_run setting
    max_new_config = session.get(AppConfig, "max_new_per_run")
    max_new_per_run = DEFAULT_MAX_NEW_PER_RUN
    if max_new_config and max_new_config.value:
        try:
            max_new_per_run = int(max_new_config.value)
        except (ValueError, TypeError):
            pass

    # Get email_enabled setting
    email_enabled = get_email_enabled(session)

    # Get PTR email setting
    ptr_email_enabled = get_ptr_email_enabled(session)

    # Get PAC groups
    pac_groups = get_pac_groups(session)
    pac_groups_json = json.dumps(pac_groups, indent=2) if pac_groups else "[]"

    # SA/SB worker: recent skipped/failed filings (operational visibility).
    sa_sb_attention = session.exec(
        select(IngestionTask, FilingF3X.committee_name)
        .join(
            FilingF3X,
            FilingF3X.filing_id == IngestionTask.filing_id,
            isouter=True,
        )
        .where(IngestionTask.source_feed == "SA_SB")
        .where(IngestionTask.status.in_(["skipped", "failed"]))
        .order_by(IngestionTask.updated_at.desc())
        .limit(50)
    ).all()
    sa_sb_attention_rows = [
        {
            "filing_id": t.filing_id,
            "status": t.status,
            "skip_reason": t.skip_reason,
            "failed_step": t.failed_step,
            "error_message": t.error_message,
            "file_size_mb": t.file_size_mb,
            "updated_at": t.updated_at,
            "committee_name": cname,
            "fec_url": t.fec_url,
        }
        for t, cname in sa_sb_attention
    ]

    # SA/SB worker: latest run summaries (small + big).
    sa_sb_runs = {}
    for mode in ("small", "big"):
        cfg = session.get(AppConfig, f"last_sa_sb_run_{mode}")
        if cfg and cfg.value:
            try:
                sa_sb_runs[mode] = json.loads(cfg.value)
            except json.JSONDecodeError:
                pass

    return templates.TemplateResponse(
        "config.html",
        {
            "request": request,
            "recipients": recipients,
            "backfill_jobs": backfill_jobs,
            "memory_mb": memory_mb,
            "last_cron_run": last_cron_run,
            "max_new_per_run": max_new_per_run,
            "email_enabled": email_enabled,
            "ptr_email_enabled": ptr_email_enabled,
            "pac_groups_json": pac_groups_json,
            "sa_sb_attention_rows": sa_sb_attention_rows,
            "sa_sb_runs": sa_sb_runs,
            "message": message,
            "message_type": message_type,
        },
    )


@app.post("/config/recipients")
def add_recipient(
    session: Session = Depends(get_session),
    _: str = Depends(verify_admin),
    email: str = Form(...),
    committee_ids: str = Form(default=""),
):
    """Add a new email recipient."""
    existing = session.exec(
        select(EmailRecipient).where(EmailRecipient.email == email)
    ).first()

    if existing:
        return RedirectResponse(
            url="/config?message=Email already exists&message_type=error",
            status_code=303,
        )

    # Parse comma-separated committee IDs (empty string = all committees)
    cids = [c.strip() for c in committee_ids.split(",") if c.strip()] or None

    recipient = EmailRecipient(email=email, active=True, committee_ids=cids)
    session.add(recipient)
    session.commit()

    return RedirectResponse(
        url="/config?message=Recipient added&message_type=success",
        status_code=303,
    )


@app.post("/config/recipients/{recipient_id}/committees")
def update_recipient_committees(
    recipient_id: int,
    session: Session = Depends(get_session),
    _: str = Depends(verify_admin),
    committee_ids: str = Form(default=""),
):
    """Update committee filter for an email recipient."""
    recipient = session.get(EmailRecipient, recipient_id)
    if not recipient:
        return RedirectResponse(
            url="/config?message=Recipient not found&message_type=error",
            status_code=303,
        )

    cids = [c.strip() for c in committee_ids.split(",") if c.strip()] or None
    recipient.committee_ids = cids
    session.add(recipient)
    session.commit()

    label = ", ".join(cids) if cids else "all committees"
    return RedirectResponse(
        url=f"/config?message=Updated filter for {recipient.email} to {label}&message_type=success",
        status_code=303,
    )


@app.post("/config/recipients/{recipient_id}/delete")
def delete_recipient(
    recipient_id: int,
    session: Session = Depends(get_session),
    _: str = Depends(verify_admin),
):
    """Remove an email recipient."""
    recipient = session.get(EmailRecipient, recipient_id)

    if recipient:
        session.delete(recipient)
        session.commit()
        return RedirectResponse(
            url="/config?message=Recipient removed&message_type=success",
            status_code=303,
        )

    return RedirectResponse(
        url="/config?message=Recipient not found&message_type=error",
        status_code=303,
    )


@app.post("/config/settings/email_enabled")
def update_email_enabled(
    session: Session = Depends(get_session),
    _: str = Depends(verify_admin),
    enabled: bool = Form(False),
):
    """Toggle email alerts on/off. Admin-only."""
    config = session.get(AppConfig, "email_enabled")
    if config:
        config.value = "true" if enabled else "false"
        config.updated_at = datetime.now(timezone.utc)
    else:
        config = AppConfig(key="email_enabled", value="true" if enabled else "false")
    session.add(config)
    session.commit()

    status = "enabled" if enabled else "disabled"
    return RedirectResponse(
        url=f"/config?message=Email alerts {status}&message_type=success",
        status_code=303,
    )


@app.post("/config/settings/ptr_email_enabled")
def update_ptr_email_enabled(
    session: Session = Depends(get_session),
    _: str = Depends(verify_admin),
    enabled: bool = Form(False),
):
    """Toggle PTR email alerts on/off. Admin-only."""
    config = session.get(AppConfig, "ptr_email_enabled")
    if config:
        config.value = "true" if enabled else "false"
        config.updated_at = datetime.now(timezone.utc)
    else:
        config = AppConfig(key="ptr_email_enabled", value="true" if enabled else "false")
    session.add(config)
    session.commit()

    status = "enabled" if enabled else "disabled"
    return RedirectResponse(
        url=f"/config?message=PTR email alerts {status}&message_type=success",
        status_code=303,
    )


@app.post("/config/settings/max_new_per_run")
def update_max_new_per_run(
    session: Session = Depends(get_session),
    _: str = Depends(verify_admin),
    value: int = Form(...),
):
    """Update the max filings to process per run. Admin-only."""
    # Validate the value (must be between 1 and 500)
    if value < 1:
        value = 1
    elif value > 500:
        value = 500

    config = session.get(AppConfig, "max_new_per_run")
    if config:
        config.value = str(value)
        config.updated_at = datetime.now(timezone.utc)
    else:
        config = AppConfig(key="max_new_per_run", value=str(value))
    session.add(config)
    session.commit()

    return RedirectResponse(
        url=f"/config?message=Max filings per run updated to {value}&message_type=success",
        status_code=303,
    )


@app.post("/config/settings/pac_groups")
def update_pac_groups(
    session: Session = Depends(get_session),
    _: str = Depends(verify_admin),
    pac_groups_json: str = Form(...),
):
    """Update PAC groups config. Expects JSON string."""
    try:
        groups = json.loads(pac_groups_json)
        if not isinstance(groups, list):
            raise ValueError("Must be a JSON array")
        set_pac_groups(session, groups)
        session.commit()
        return RedirectResponse(
            url=f"/config?message=PAC groups updated ({len(groups)} groups)&message_type=success",
            status_code=303,
        )
    except (json.JSONDecodeError, ValueError) as e:
        return RedirectResponse(
            url=f"/config?message=Invalid JSON: {e}&message_type=error",
            status_code=303,
        )


@app.post("/config/test-sa-sb-alert")
def test_sa_sb_alert(
    session: Session = Depends(get_session),
    _: str = Depends(verify_admin),
):
    """Send a synthetic SA/SB attention alert to confirm the email
    pipeline is working. Picks up to 3 recent F3X filings for the
    skipped/streamed/failed sections so the email isn't empty."""
    from .email_service import send_sa_sb_attention_alert
    from sqlalchemy import desc

    # Use up to 3 recent filings as fixture content; if there are none,
    # the alert sender will short-circuit (which still confirms the path).
    recents = session.exec(
        select(FilingF3X)
        .order_by(desc(FilingF3X.filed_at_utc))
        .limit(3)
    ).all()
    fids = [f.filing_id for f in recents]

    sent = send_sa_sb_attention_alert(
        session,
        mode="test",
        skipped_filing_ids=fids[:1],
        streamed_filing_ids=fids[1:2],
        failed_filing_ids=fids[2:3],
    )
    if sent:
        msg = f"Test alert sent to {len(sent)} recipient(s)"
        msg_type = "success"
    else:
        msg = "Test alert NOT sent — check recipient list and email config"
        msg_type = "error"
    return RedirectResponse(
        url=f"/config?message={msg}&message_type={msg_type}",
        status_code=303,
    )


@app.post("/config/parse-sa")
def parse_sa_for_filing(
    session: Session = Depends(get_session),
    _: str = Depends(verify_admin),
    filing_id: int = Form(...),
):
    """Manually parse Schedule A items from a specific F3X filing."""
    import gc
    from .fec_parse import (
        download_fec_text, parse_fec_filing,
        extract_schedule_a_best_effort, sha256_hex, FileTooLargeError,
    )
    from .repo import claim_filing, update_filing_status, insert_sa_event
    from .schemas import ScheduleA

    # Look up the filing in filings_f3x
    filing = session.get(FilingF3X, filing_id)
    if not filing:
        return RedirectResponse(
            url=f"/config?message=Filing {filing_id} not found in filings_f3x&message_type=error",
            status_code=303,
        )

    # Claim with source_feed="SA" — if already parsed, skip
    already_done = not claim_filing(session, filing_id, source_feed="SA")
    if already_done:
        return RedirectResponse(
            url=f"/config?message=Filing {filing_id} already processed for Schedule A&message_type=error",
            status_code=303,
        )

    # Download full file
    update_filing_status(session, filing_id, "SA", "downloading")
    try:
        fec_text = download_fec_text(filing.fec_url, max_size_mb=50)
    except FileTooLargeError as e:
        update_filing_status(
            session, filing_id, "SA", "failed",
            failed_step="downloading",
            error_message=f"Too large: {e.size_mb:.1f}MB",
        )
        session.commit()
        return RedirectResponse(
            url=f"/config?message=Filing {filing_id} too large ({e.size_mb:.1f}MB)&message_type=error",
            status_code=303,
        )
    except Exception as e:
        update_filing_status(
            session, filing_id, "SA", "failed",
            failed_step="downloading", error_message=str(e)[:500],
        )
        session.commit()
        return RedirectResponse(
            url=f"/config?message=Download failed: {e}&message_type=error",
            status_code=303,
        )

    update_filing_status(session, filing_id, "SA", "downloaded")

    # Parse Schedule A items
    update_filing_status(session, filing_id, "SA", "parsing")
    try:
        parsed = parse_fec_filing(fec_text)
        count = 0
        for raw_line, fields in extract_schedule_a_best_effort(fec_text, parsed=parsed):
            event_id = sha256_hex(f"{filing_id}|{raw_line}")
            event = ScheduleA(
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
            if insert_sa_event(session, event):
                count += 1

        update_filing_status(session, filing_id, "SA", "ingested")
        session.commit()
    except Exception as e:
        update_filing_status(
            session, filing_id, "SA", "failed",
            failed_step="parsing", error_message=str(e)[:500],
        )
        session.commit()
        return RedirectResponse(
            url=f"/config?message=Parse failed for {filing_id}: {e}&message_type=error",
            status_code=303,
        )
    finally:
        del fec_text
        del parsed
        gc.collect()

    return RedirectResponse(
        url=f"/config?message=Parsed {count} Schedule A items from filing {filing_id} ({filing.committee_name})&message_type=success",
        status_code=303,
    )


@app.post("/config/backfill/{job_id}/delete")
def delete_backfill_job(
    job_id: int,
    session: Session = Depends(get_session),
    _: str = Depends(verify_admin),
):
    """Delete a backfill job. Admin-only."""
    job = session.get(BackfillJob, job_id)

    if job:
        session.delete(job)
        session.commit()
        return RedirectResponse(
            url="/config?message=Backfill job deleted&message_type=success",
            status_code=303,
        )

    return RedirectResponse(
        url="/config?message=Job not found&message_type=error",
        status_code=303,
    )


@app.post("/config/backfill/{year:int}/{month:int}/{day:int}/{filing_type}")
def trigger_backfill(
    year: int,
    month: int,
    day: int,
    filing_type: str,
    session: Session = Depends(get_session),
    _: str = Depends(verify_admin),
):
    """Manually trigger backfill for a specific date. Admin-only."""
    from .backfill import get_backfill_status, get_or_create_backfill_job

    if filing_type not in ("3x", "e"):
        return {"error": "Invalid filing_type. Must be '3x' or 'e'"}, 400

    try:
        target_date = date(year, month, day)
    except ValueError:
        return {"error": "Invalid date"}, 400

    today = datetime.now(timezone.utc).astimezone(ET).date()
    if target_date >= today:
        return {"error": "Cannot backfill current or future dates"}, 400

    # Check if already running or completed
    job = get_backfill_status(session, target_date, filing_type)
    if job and job.status == "running":
        # Auto-reset stale jobs (stuck for > 10 min)
        stale = datetime.utcnow() - timedelta(minutes=10)
        if job.started_at and job.started_at.replace(tzinfo=None) < stale:
            job.status = "pending"
            session.add(job)
            session.commit()
        else:
            return {"status": "already_running", "job_id": job.id}
    if job and job.status == "completed":
        return {"status": "already_completed", "filings_found": job.filings_found}

    # Create/reset job and trigger backfill
    job = get_or_create_backfill_job(session, target_date, filing_type)
    _trigger_backfill_async(target_date, filing_type)

    return {
        "status": "started",
        "date": target_date.isoformat(),
        "filing_type": filing_type,
        "check_status_at": f"/api/backfill/status/{year}/{month}/{day}/{filing_type}",
    }


# -------------------------
# Date-based Dashboards with Backfill
# -------------------------

def _date_utc_bounds(target_date: date) -> tuple[datetime, datetime]:
    """Return [start_utc, end_utc) for a specific date in ET."""
    start_et = datetime.combine(target_date, time(0, 0, 0), tzinfo=ET)
    end_et = datetime.combine(
        target_date + timedelta(days=1), time(0, 0, 0), tzinfo=ET)
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc)


def _trigger_backfill_async(target_date: date, filing_type: str):
    """Trigger backfill in background thread."""
    from .backfill import backfill_date_f3x, backfill_date_e

    def run():
        with Session(engine) as session:
            if filing_type == "3x":
                backfill_date_f3x(session, target_date)
            else:
                backfill_date_e(session, target_date)

    thread = threading.Thread(target=run, daemon=False)
    thread.start()


def _maybe_run_ingestion() -> bool:
    """Run ingestion if cooldown has passed. Returns True if ingestion ran."""
    global _last_ingestion_time

    now = datetime.now(timezone.utc)

    with _ingestion_lock:
        if _last_ingestion_time is not None:
            elapsed = (now - _last_ingestion_time).total_seconds() / 60
            if elapsed < INGESTION_COOLDOWN_MINUTES:
                return False
        _last_ingestion_time = now

    # Run ingestion in background thread to not block page load
    def run():
        from .ingest_f3x import run_f3x
        from .ingest_ie import run_ie_schedule_e

        with Session(engine) as bg_session:
            try:
                run_f3x(
                    bg_session,
                    feed_url=settings.f3x_feed,
                    receipts_threshold=settings.receipts_threshold,
                )
            except Exception:
                pass
            try:
                run_ie_schedule_e(bg_session, feed_urls=settings.ie_feeds)
            except Exception:
                pass

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return True


@app.get("/{year:int}/{month:int}/{day:int}/3x", response_class=HTMLResponse)
def dashboard_date_3x(
    request: Request,
    year: int,
    month: int,
    day: int,
    session: Session = Depends(get_session),
    threshold: float = Query(default=50_000, ge=0),
    limit: int = Query(default=500, ge=1, le=5000),
):
    """Date-based F3X dashboard. Backfill must be triggered manually via /config."""
    try:
        target_date = date(year, month, day)
    except ValueError:
        return HTMLResponse("Invalid date", status_code=400)

    today = datetime.now(timezone.utc).astimezone(ET).date()

    # For current day, trigger rate-limited ingestion
    if target_date == today:
        _maybe_run_ingestion()

    start_utc, end_utc = _date_utc_bounds(target_date)
    stmt = (
        select(FilingF3X)
        .where(FilingF3X.filed_at_utc >= start_utc)
        .where(FilingF3X.filed_at_utc < end_utc)
    )
    if threshold > 0:
        stmt = stmt.where(FilingF3X.total_receipts != None)  # noqa: E711
        stmt = stmt.where(FilingF3X.total_receipts >= threshold)
    stmt = stmt.order_by(FilingF3X.filed_at_utc.desc()).limit(limit)
    rows = session.exec(stmt).all()
    sa_sb_status = _sa_sb_status_map(
        session, [r.filing_id for r in rows])

    if threshold > 0:
        label = f"F3X Filings (≥${threshold:,.0f})"
    else:
        label = "F3X Filings (All)"

    return templates.TemplateResponse(
        "dashboard_date.html",
        {
            "request": request,
            "rows": rows,
            "sa_sb_status": sa_sb_status,
            "target_date": target_date,
            "prev_date": target_date - timedelta(days=1),
            "next_date": target_date + timedelta(days=1),
            "today": today,
            "nav_date": target_date,
            "threshold": threshold,
            "filing_type": "3x",
            "filing_type_label": label,
        },
    )


@app.get("/{year:int}/{month:int}/{day:int}/e", response_class=HTMLResponse)
def dashboard_date_e(
    request: Request,
    year: int,
    month: int,
    day: int,
    session: Session = Depends(get_session),
    limit: int = Query(default=500, ge=1, le=5000),
):
    """Date-based Schedule E dashboard. Backfill must be triggered manually via /config."""
    try:
        target_date = date(year, month, day)
    except ValueError:
        return HTMLResponse("Invalid date", status_code=400)

    today = datetime.now(timezone.utc).astimezone(ET).date()

    # For current day, trigger rate-limited ingestion
    if target_date == today:
        _maybe_run_ingestion()

    # Query events for this date
    start_utc, end_utc = _date_utc_bounds(target_date)
    stmt = (
        select(IEScheduleE)
        .where(IEScheduleE.filed_at_utc >= start_utc)
        .where(IEScheduleE.filed_at_utc < end_utc)
        .order_by(IEScheduleE.filed_at_utc.desc())
        .limit(limit)
    )
    rows = session.exec(stmt).all()

    return templates.TemplateResponse(
        "dashboard_date.html",
        {
            "request": request,
            "rows": rows,
            "target_date": target_date,
            "prev_date": target_date - timedelta(days=1),
            "next_date": target_date + timedelta(days=1),
            "today": today,
            "nav_date": target_date,
            "filing_type": "e",
            "filing_type_label": "Schedule E Events",
        },
    )


@app.get("/api/backfill/status/{year:int}/{month:int}/{day:int}/{filing_type}")
def api_backfill_status(
    year: int,
    month: int,
    day: int,
    filing_type: str,
    session: Session = Depends(get_session),
):
    """Get backfill status for polling."""
    from .backfill import get_backfill_status

    if filing_type not in ("3x", "e"):
        return {"error": "Invalid filing_type"}, 400

    try:
        target_date = date(year, month, day)
    except ValueError:
        return {"error": "Invalid date"}, 400

    job = get_backfill_status(session, target_date, filing_type)
    if job is None:
        return {"status": "not_started"}

    return {
        "status": job.status,
        "filings_found": job.filings_found,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "error_message": job.error_message,
    }


# -------------------------
# Cron Endpoint
# -------------------------

def _save_cron_status(session: Session, results: dict):
    """Save cron run results to AppConfig."""
    config_entry = session.get(AppConfig, "last_cron_run")
    if config_entry:
        config_entry.value = json.dumps(results)
        config_entry.updated_at = datetime.now(timezone.utc)
    else:
        config_entry = AppConfig(key="last_cron_run", value=json.dumps(results))
    session.add(config_entry)
    session.commit()


def _log_memory(label: str, results: dict):
    """Log memory usage at a checkpoint."""
    mb = _get_memory_usage_mb()
    if "memory_log" not in results:
        results["memory_log"] = []
    results["memory_log"].append({"step": label, "memory_mb": round(mb, 1)})
    print(f"[MEMORY] {label}: {mb:.1f} MB")


@app.get("/api/cron/check-new")
def cron_check_new(session: Session = Depends(get_session)):
    """Cron endpoint to check for new filings and send email alerts.

    This endpoint is designed to be called by a scheduled task (e.g., Cloud Scheduler).
    It runs the ingestion process and sends email alerts if new filings are found.
    """
    import gc
    from fastapi.responses import JSONResponse
    from .ingest_f3x import run_f3x
    from .ingest_ie import run_ie_schedule_e

    started_at = datetime.now(timezone.utc)

    results = {
        "started_at": started_at.isoformat(),
        "completed_at": None,
        "http_status": 200,
        "status": "running",
        "f3x_new": 0,
        "ie_filings_new": 0,
        "ie_events_new": 0,
        "email_sent": False,
        "emails_sent_to": [],
    }

    _log_memory("start", results)

    # Save "running" status immediately so we can see if it crashes mid-run
    _save_cron_status(session, results)

    try:
        # Run F3X ingestion
        _log_memory("before_f3x_ingestion", results)
        try:
            f3x_result = run_f3x(
                session,
                feed_url=settings.f3x_feed,
                receipts_threshold=settings.receipts_threshold,
            )
            results["f3x_new"] = f3x_result.new_count
            if f3x_result.failed_count > 0:
                results["f3x_error"] = (
                    f"{f3x_result.failed_count} filing(s) failed: "
                    f"{f3x_result.last_error}"
                )
        except Exception as e:
            results["f3x_error"] = str(e)
        _log_memory("after_f3x_ingestion", results)
        gc.collect()
        _log_memory("after_f3x_gc", results)

        # Run IE Schedule E ingestion
        _log_memory("before_ie_ingestion", results)
        try:
            ie_filings, ie_events = run_ie_schedule_e(
                session,
                feed_urls=settings.ie_feeds,
            )
            results["ie_filings_new"] = ie_filings
            results["ie_events_new"] = ie_events
        except Exception as e:
            results["ie_error"] = str(e)
        _log_memory("after_ie_ingestion", results)
        gc.collect()
        _log_memory("after_ie_gc", results)

        # NOTE: Email alerts are handled by the ingest job (scripts/ingest_job.py),
        # not this cron endpoint. The job uses emailed_at tracking to avoid duplicates.

        # Determine final status
        has_errors = "f3x_error" in results or "ie_error" in results
        results["status"] = "completed_with_errors" if has_errors else "success"
        results["http_status"] = 200  # We still return 200 for partial errors
        results["completed_at"] = datetime.now(timezone.utc).isoformat()

        gc.collect()
        _log_memory("final", results)

        _save_cron_status(session, results)
        return results

    except Exception as e:
        # Unexpected crash - save error state
        results["status"] = "crashed"
        results["http_status"] = 500
        results["crash_error"] = str(e)
        results["completed_at"] = datetime.now(timezone.utc).isoformat()

        _save_cron_status(session, results)
        return JSONResponse(status_code=500, content=results)


# -------------------------
# Root Redirect
# -------------------------

@app.get("/")
def root_redirect():
    """Redirect root to the main dashboard."""
    return RedirectResponse(url="/dashboard/3x", status_code=302)
