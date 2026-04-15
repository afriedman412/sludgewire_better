# app/schemas.py
from __future__ import annotations

from datetime import datetime, date, timezone
from typing import Optional, Dict, Any

from sqlmodel import SQLModel, Field, Column
from sqlalchemy import BigInteger, Text, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import JSONB


class Committee(SQLModel, table=True):
    __tablename__ = "committees"

    committee_id: str = Field(primary_key=True, index=True)
    committee_name: str = Field(index=True)

    committee_type: Optional[str] = Field(default=None, index=True)   # CMTE_TP
    designation: Optional[str] = Field(
        default=None, index=True)      # CMTE_DSGN
    filing_freq: Optional[str] = Field(
        default=None, index=True)      # CMTE_FILING_FREQ

    # CMTE_CITY
    city: Optional[str] = None
    state: Optional[str] = Field(default=None, index=True)            # CMTE_ST

    treasurer_name: Optional[str] = None                              # TRES_NM
    candidate_id: Optional[str] = Field(default=None, index=True)     # CAND_ID

    raw_meta: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSONB)
    )

    # True if added from filing form (not from official FEC CSV)
    provisional: bool = Field(default=False)

    updated_at_utc: datetime = Field(
        default_factory=datetime.utcnow, index=True)


class IngestionTask(SQLModel, table=True):
    __tablename__ = "ingestion_tasks"
    __table_args__ = (PrimaryKeyConstraint("filing_id", "source_feed"),)

    filing_id: int = Field(sa_column=Column(BigInteger))
    source_feed: str = Field(default="")

    # Pipeline status: claimed → downloading → downloaded → parsing → ingested
    # Terminal states: failed, skipped
    status: str = Field(default="claimed", index=True)

    # Which substep failed (downloading, parsing, etc.) — null if successful
    failed_step: Optional[str] = None

    # Error details
    error_message: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))

    # Skip metadata (replaces skipped_filings table)
    skip_reason: Optional[str] = None
    file_size_mb: Optional[float] = None
    fec_url: Optional[str] = None

    # Email tracking (completes the pipeline: ingested → emailed)
    emailed_at: Optional[datetime] = Field(default=None)

    # Queue priority (higher = processed first). Used by the SA_SB worker.
    # Default 0; user-triggered bumps write a larger value.
    priority: int = Field(default=0, index=True)

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class FilingF3X(SQLModel, table=True):
    __tablename__ = "filings_f3x"

    filing_id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    committee_id: str = Field(index=True)
    committee_name: Optional[str] = Field(default=None, index=True)
    form_type: Optional[str] = None
    report_type: Optional[str] = None

    coverage_from: Optional[date] = None
    coverage_through: Optional[date] = None
    filed_at_utc: Optional[datetime] = Field(default=None, index=True)

    fec_url: str = Field(sa_column=Column(Text, nullable=False))

    total_receipts: Optional[float] = None
    total_disbursements: Optional[float] = None
    threshold_flag: bool = Field(default=False, index=True)

    raw_meta: Optional[Dict[str, Any]] = Field(
        default=None, sa_column=Column(JSONB))

    first_seen_utc: datetime = Field(default_factory=datetime.utcnow)
    updated_at_utc: datetime = Field(default_factory=datetime.utcnow)
    emailed_at: Optional[datetime] = Field(default=None)


class IEScheduleE(SQLModel, table=True):
    __tablename__ = "ie_schedule_e"

    event_id: str = Field(primary_key=True)

    filing_id: int = Field(sa_column=Column(
        BigInteger, index=True, nullable=False))
    filer_id: Optional[str] = Field(default=None, index=True)
    committee_id: Optional[str] = Field(default=None, index=True)
    committee_name: Optional[str] = Field(default=None)
    form_type: Optional[str] = None
    report_type: Optional[str] = None

    coverage_from: Optional[date] = None
    coverage_through: Optional[date] = None
    filed_at_utc: Optional[datetime] = Field(default=None, index=True)

    expenditure_date: Optional[date] = Field(default=None, index=True)
    amount: Optional[float] = Field(default=None, index=True)
    support_oppose: Optional[str] = None

    candidate_id: Optional[str] = Field(default=None, index=True)
    candidate_name: Optional[str] = None
    candidate_office: Optional[str] = None
    candidate_state: Optional[str] = None
    candidate_district: Optional[str] = None
    candidate_party: Optional[str] = None

    election_code: Optional[str] = None
    purpose: Optional[str] = None
    payee_name: Optional[str] = None

    fec_url: Optional[str] = None
    raw_line: str = Field(sa_column=Column(Text, nullable=False))

    first_seen_utc: datetime = Field(default_factory=datetime.utcnow)
    emailed_at: Optional[datetime] = Field(default=None)


class ScheduleA(SQLModel, table=True):
    __tablename__ = "schedule_a"

    event_id: str = Field(primary_key=True)  # SHA256("{filing_id}|{raw_line}")

    filing_id: int = Field(sa_column=Column(
        BigInteger, index=True, nullable=False))
    committee_id: str = Field(index=True)
    committee_name: Optional[str] = Field(default=None)
    form_type: Optional[str] = None
    report_type: Optional[str] = None

    coverage_from: Optional[date] = None
    coverage_through: Optional[date] = None
    filed_at_utc: Optional[datetime] = Field(default=None, index=True)

    # Contributor fields (no city/state/zip)
    contributor_name: Optional[str] = Field(default=None, index=True)
    contributor_employer: Optional[str] = None
    contributor_occupation: Optional[str] = None
    contribution_amount: Optional[float] = Field(default=None, index=True)
    contribution_date: Optional[date] = Field(default=None, index=True)
    receipt_description: Optional[str] = None
    contributor_type: Optional[str] = None    # entity_type: IND, ORG, COM
    memo_text: Optional[str] = None

    fec_url: Optional[str] = None
    raw_line: str = Field(sa_column=Column(Text, nullable=False))

    first_seen_utc: datetime = Field(default_factory=datetime.utcnow)


class ScheduleB(SQLModel, table=True):
    __tablename__ = "schedule_b"

    event_id: str = Field(primary_key=True)  # SHA256("{filing_id}|{raw_line}")

    filing_id: int = Field(sa_column=Column(
        BigInteger, index=True, nullable=False))
    committee_id: str = Field(index=True)
    committee_name: Optional[str] = Field(default=None)
    form_type: Optional[str] = None
    report_type: Optional[str] = None

    coverage_from: Optional[date] = None
    coverage_through: Optional[date] = None
    filed_at_utc: Optional[datetime] = Field(default=None, index=True)

    # Payee / recipient fields
    payee_name: Optional[str] = Field(default=None, index=True)
    payee_employer: Optional[str] = None
    payee_occupation: Optional[str] = None
    disbursement_amount: Optional[float] = Field(default=None, index=True)
    disbursement_date: Optional[date] = Field(default=None, index=True)
    disbursement_description: Optional[str] = None
    payee_type: Optional[str] = None  # entity_type: IND, ORG, COM, PAC, CCM
    purpose: Optional[str] = None
    category_code: Optional[str] = None
    memo_text: Optional[str] = None

    # If this disbursement is attributed to a candidate (e.g. contribution
    # to a candidate's committee), store the linkage.
    beneficiary_candidate_id: Optional[str] = Field(default=None, index=True)
    beneficiary_candidate_name: Optional[str] = None

    fec_url: Optional[str] = None
    raw_line: str = Field(sa_column=Column(Text, nullable=False))

    first_seen_utc: datetime = Field(default_factory=datetime.utcnow)


class RaceCandidate(SQLModel, table=True):
    """FEC candidate data for races with IE spending — includes opponents."""
    __tablename__ = "race_candidates"

    candidate_id: str = Field(primary_key=True)
    candidate_name: Optional[str] = None
    party: Optional[str] = None
    state: Optional[str] = Field(default=None, index=True)
    office: Optional[str] = None  # S or H
    district: Optional[str] = None
    has_ie_spending: bool = Field(default=False)  # True if in ie_schedule_e


class DonorIndustry(SQLModel, table=True):
    __tablename__ = "donor_industries"

    contrib_id: str = Field(primary_key=True)
    contrib_name: Optional[str] = None
    name_upper: str = Field(index=True)
    org_name: Optional[str] = None
    real_code: Optional[str] = None
    cat_name: Optional[str] = None
    industry: Optional[str] = Field(default=None, index=True)
    sector: Optional[str] = Field(default=None, index=True)


class OrgIndustry(SQLModel, table=True):
    __tablename__ = "org_industries"

    id: Optional[int] = Field(default=None, primary_key=True)
    org_name: Optional[str] = None
    org_upper: str = Field(index=True)
    cmte_id: Optional[str] = Field(default=None, index=True)
    pac_short: Optional[str] = None
    prim_code: Optional[str] = None
    cat_name: Optional[str] = None
    industry: Optional[str] = Field(default=None, index=True)
    sector: Optional[str] = Field(default=None, index=True)


class EmailRecipient(SQLModel, table=True):
    __tablename__ = "email_recipients"

    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = Field(index=True, unique=True)
    active: bool = Field(default=True)
    committee_ids: Optional[list] = Field(
        default=None, sa_column=Column(JSONB, nullable=True),
    )  # None = all committees
    created_at: datetime = Field(default_factory=datetime.utcnow)


class BackfillJob(SQLModel, table=True):
    __tablename__ = "backfill_jobs"

    id: Optional[int] = Field(default=None, primary_key=True)
    target_date: date = Field(index=True)
    filing_type: str = Field(index=True)  # "3x" or "e"
    status: str = Field(default="pending", index=True)  # pending, running, completed, failed
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    filings_found: int = Field(default=0)
    error_message: Optional[str] = None


class PtrFiling(SQLModel, table=True):
    """House financial disclosure PTR filing index (from bulk ZIP)."""
    __tablename__ = "ptr_filings"

    doc_id: str = Field(primary_key=True)  # from FD ZIP
    first_name: str
    last_name: str
    prefix: Optional[str] = None
    suffix: Optional[str] = None
    state_district: Optional[str] = Field(default=None, index=True)
    filing_year: int = Field(index=True)
    filing_date: Optional[date] = Field(default=None, index=True)
    filing_type: str = Field(default="P", index=True)  # P=PTR, A=amendment, etc.

    # Ingestion status: pending → downloading → parsing → ingested | failed | skipped
    status: str = Field(default="pending", index=True)
    error_message: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))

    pdf_url: Optional[str] = None
    emailed_at: Optional[datetime] = Field(default=None)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PtrTransaction(SQLModel, table=True):
    """Individual transaction from a House PTR filing."""
    __tablename__ = "ptr_transactions"

    id: Optional[int] = Field(default=None, primary_key=True)

    doc_id: str = Field(index=True)  # FK to ptr_filings
    filer_name: str = Field(index=True)
    state_district: Optional[str] = Field(default=None, index=True)

    owner: Optional[str] = None  # SP, JT, DC
    asset: str  # full asset description
    ticker: Optional[str] = Field(default=None, index=True)  # extracted ticker symbol
    asset_type: Optional[str] = None  # ST, GS, OP, etc.
    transaction_type: Optional[str] = Field(default=None, index=True)  # P, S, E
    transaction_date: Optional[date] = Field(default=None, index=True)
    notification_date: Optional[date] = None
    amount: Optional[str] = None  # range string e.g. "$1,001 - $15,000"
    cap_gains_over_200: Optional[bool] = None
    description: Optional[str] = None
    subholding_of: Optional[str] = None
    filing_status: Optional[str] = None  # New, etc.

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AppConfig(SQLModel, table=True):
    __tablename__ = "app_config"

    key: str = Field(primary_key=True)
    value: Optional[str] = None
    updated_at: datetime = Field(default_factory=datetime.utcnow)


