-- Migration: add total_disbursements, schedule_b, priority col for SA/SB queue
-- Feature branch: feat/disbursements-and-sa-sb-queue
-- Run with: psql "$POSTGRES_URL" -f scripts/migrations/001_disbursements_and_sa_sb_queue.sql

BEGIN;

-- 1. Add total_disbursements to filings_f3x
ALTER TABLE filings_f3x
    ADD COLUMN IF NOT EXISTS total_disbursements DOUBLE PRECISION;

-- 2. Add priority to ingestion_tasks (for SA/SB worker queue)
ALTER TABLE ingestion_tasks
    ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS ix_ingestion_tasks_priority
    ON ingestion_tasks (priority);

-- 3. Create schedule_b table (disbursements / recipients)
CREATE TABLE IF NOT EXISTS schedule_b (
    event_id                    VARCHAR PRIMARY KEY,
    filing_id                   BIGINT       NOT NULL,
    committee_id                VARCHAR      NOT NULL,
    committee_name              VARCHAR,
    form_type                   VARCHAR,
    report_type                 VARCHAR,
    coverage_from               DATE,
    coverage_through            DATE,
    filed_at_utc                TIMESTAMP,
    payee_name                  VARCHAR,
    payee_employer              VARCHAR,
    payee_occupation            VARCHAR,
    disbursement_amount         DOUBLE PRECISION,
    disbursement_date           DATE,
    disbursement_description    VARCHAR,
    payee_type                  VARCHAR,
    purpose                     VARCHAR,
    category_code               VARCHAR,
    memo_text                   VARCHAR,
    beneficiary_candidate_id    VARCHAR,
    beneficiary_candidate_name  VARCHAR,
    fec_url                     VARCHAR,
    raw_line                    TEXT         NOT NULL,
    first_seen_utc              TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_schedule_b_filing_id
    ON schedule_b (filing_id);
CREATE INDEX IF NOT EXISTS ix_schedule_b_committee_id
    ON schedule_b (committee_id);
CREATE INDEX IF NOT EXISTS ix_schedule_b_filed_at_utc
    ON schedule_b (filed_at_utc);
CREATE INDEX IF NOT EXISTS ix_schedule_b_payee_name
    ON schedule_b (payee_name);
CREATE INDEX IF NOT EXISTS ix_schedule_b_disbursement_amount
    ON schedule_b (disbursement_amount);
CREATE INDEX IF NOT EXISTS ix_schedule_b_disbursement_date
    ON schedule_b (disbursement_date);
CREATE INDEX IF NOT EXISTS ix_schedule_b_beneficiary_candidate_id
    ON schedule_b (beneficiary_candidate_id);

COMMIT;
