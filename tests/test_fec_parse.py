"""Tests for Schedule B extractor + streaming fallback parser.

Schedule A extraction is exercised indirectly by the existing SA ingestion
tests; here we focus on the new SB parser and the streaming fallback
used for files too large for fecfile.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest

from app.fec_parse import (
    extract_schedule_b_best_effort,
    _stream_parse_line,
    _looks_like_amount,
    _best_amount,
    _first_date,
    _first_name_like,
)


# -------------------------------------------------------
# extract_schedule_b_best_effort (fecfile-based)
# -------------------------------------------------------

class TestExtractScheduleB:
    def _fake_parsed(self, sb_items):
        return {"filing": {}, "itemizations": {"Schedule B": sb_items}}

    def test_yields_nothing_when_no_sb_items(self):
        parsed = self._fake_parsed([])
        # No SB in fecfile output and no raw SB lines => no yield.
        results = list(
            extract_schedule_b_best_effort("", parsed=parsed))
        assert results == []

    def test_extracts_org_payee(self):
        parsed = self._fake_parsed([{
            "payee_organization_name": "ACME CORP",
            "expenditure_amount": "5000.00",
            "expenditure_date": "2024-03-10",
            "expenditure_purpose_descrip": "Consulting",
            "entity_type": "ORG",
        }])
        results = list(
            extract_schedule_b_best_effort("", parsed=parsed))
        assert len(results) == 1
        raw, fields = results[0]
        assert fields["payee_name"] == "ACME CORP"
        assert fields["disbursement_amount"] == 5000.00
        assert fields["disbursement_date"] == date(2024, 3, 10)
        assert fields["purpose"] == "Consulting"
        assert fields["payee_type"] == "ORG"

    def test_extracts_individual_payee(self):
        parsed = self._fake_parsed([{
            "payee_first_name": "Jane",
            "payee_middle_name": "Q",
            "payee_last_name": "Doe",
            "payee_suffix": "Jr",
            "expenditure_amount": 1250.5,
            "expenditure_date": "2024-01-15",
        }])
        results = list(
            extract_schedule_b_best_effort("", parsed=parsed))
        raw, fields = results[0]
        assert fields["payee_name"] == "Jane Q Doe Jr"
        assert fields["disbursement_amount"] == 1250.5

    def test_beneficiary_candidate_linkage(self):
        parsed = self._fake_parsed([{
            "payee_organization_name": "VENDOR",
            "expenditure_amount": "100",
            "expenditure_date": "2024-05-01",
            "beneficiary_candidate_id_number": "H0NY12345",
            "beneficiary_candidate_first_name": "Pat",
            "beneficiary_candidate_last_name": "Smith",
        }])
        raw, fields = next(
            extract_schedule_b_best_effort("", parsed=parsed))
        assert fields["beneficiary_candidate_id"] == "H0NY12345"
        assert fields["beneficiary_candidate_name"] == "Pat Smith"

    def test_empty_strings_become_none(self):
        parsed = self._fake_parsed([{
            "payee_organization_name": "X CORP",
            "payee_employer": "",
            "payee_occupation": "   ",
            "expenditure_amount": "100",
        }])
        raw, fields = next(
            extract_schedule_b_best_effort("", parsed=parsed))
        assert fields["payee_employer"] is None
        assert fields["payee_occupation"] is None

    def test_falls_back_to_raw_when_fecfile_fails(self):
        # Simulate fecfile raising by passing parsed=None and broken text;
        # parser should still yield SB rows via raw path.
        raw_text = (
            "HDR|FEC|8.4\n"
            "F3X|C00000001|FILER NAME\n"
            "SB21B|C00000001|ORG|FALLBACK CORP|123 MAIN|CITY|CA|94103"
            "|04/01/2024|750.50|Supplies\n"
        )
        with patch("app.fec_parse._get_fecfile") as gf:
            gf.return_value.loads.side_effect = RuntimeError("broken")
            results = list(extract_schedule_b_best_effort(raw_text))
        assert len(results) == 1
        _, fields = results[0]
        assert fields["disbursement_amount"] == 750.50
        assert fields["disbursement_date"] == date(2024, 4, 1)


# -------------------------------------------------------
# Streaming helpers — positional heuristics
# -------------------------------------------------------

class TestLooksLikeAmount:
    def test_decimal_amount(self):
        assert _looks_like_amount("2500.00") == 2500.00

    def test_comma_separated(self):
        assert _looks_like_amount("1,234.56") == 1234.56

    def test_rejects_5_digit_zip(self):
        assert _looks_like_amount("10001") is None

    def test_rejects_9_digit_zip(self):
        assert _looks_like_amount("100014567") is None

    def test_accepts_5_digit_with_decimal(self):
        # Decimal disambiguates: 10001.50 is clearly money, not a ZIP.
        assert _looks_like_amount("10001.50") == 10001.50

    def test_rejects_empty(self):
        assert _looks_like_amount("") is None

    def test_rejects_alpha(self):
        assert _looks_like_amount("abc") is None

    def test_rejects_zero(self):
        assert _looks_like_amount("0") is None
        assert _looks_like_amount("0.00") is None

    def test_accepts_negative(self):
        assert _looks_like_amount("-500.00") == -500.00


class TestBestAmount:
    def test_prefers_decimal_over_plain_int(self):
        row = ["SA11A1", "C1", "10001", "2500.00"]  # ZIP then amount
        assert _best_amount(row) == 2500.00

    def test_scans_backward_when_no_decimal(self):
        row = ["SA", "C1", "IND", "JANE DOE", "2500"]
        assert _best_amount(row) == 2500.0

    def test_skips_zips_in_backward_scan(self):
        # Both ZIP-shaped but amount 1500 is the trailing field.
        row = ["SA", "C1", "JOHN", "90210", "1500"]
        assert _best_amount(row) == 1500.0

    def test_none_when_no_amounts(self):
        assert _best_amount(["SA", "C1", "NAME", "STREET"]) is None


class TestFirstDate:
    def test_finds_mmddyyyy(self):
        assert _first_date(
            ["SA", "C1", "NAME", "02/15/2024"]) == date(2024, 2, 15)

    def test_none_when_no_date(self):
        assert _first_date(["SA", "C1", "NAME"]) is None


class TestFirstNameLike:
    def test_picks_first_long_alpha(self):
        row = ["SA", "C1", "IND", "SMITH, JOHN", "..."]
        assert _first_name_like(row) == "SMITH, JOHN"

    def test_skips_state_code(self):
        row = ["SA", "C1", "NY", "JANE DOE"]
        assert _first_name_like(row) == "JANE DOE"

    def test_skips_short_tokens(self):
        row = ["SA", "C1", "IN", "ACME"]
        # "IN" is 2 chars (rejected as state-code-like); ACME is 4.
        assert _first_name_like(row) == "ACME"

    def test_none_when_no_names(self):
        assert _first_name_like(["SA", "C1"]) is None


# -------------------------------------------------------
# _stream_parse_line (end-to-end single-line)
# -------------------------------------------------------

class TestStreamParseLine:
    def test_sa_with_zip_returns_correct_amount(self):
        line = (
            "SA11A1|C00000001|IND|SMITH, JOHN|123 MAIN ST|ANYTOWN|NY"
            "|10001|02/15/2024|2500.00|SMITH CO|ENGINEER"
        )
        kind, raw, fields = _stream_parse_line(line)
        assert kind == "SA"
        assert fields["contribution_amount"] == 2500.00
        assert fields["contribution_date"] == date(2024, 2, 15)
        assert fields["contributor_name"] == "SMITH, JOHN"

    def test_sb_line(self):
        line = (
            "SB21B|C00000001|ORG|ACME CORP||||"
            "|03/10/2024|5000.00|Consulting"
        )
        kind, raw, fields = _stream_parse_line(line)
        assert kind == "SB"
        assert fields["disbursement_amount"] == 5000.00
        assert fields["disbursement_date"] == date(2024, 3, 10)
        assert fields["payee_name"] == "ACME CORP"

    def test_non_sa_sb_returns_none(self):
        assert _stream_parse_line("HDR|FEC|8.4") is None
        assert _stream_parse_line("F3X|C0|NAME") is None
        assert _stream_parse_line("SE|something") is None

    def test_empty_returns_none(self):
        assert _stream_parse_line("") is None
        assert _stream_parse_line("   ") is None

    def test_embedded_quoted_pipe(self):
        # csv.reader handles quoted delimiters; naive .split would not.
        line = 'SB21B|C1|ORG|"VENDOR, LLC | CONSULTANTS"|||03/10/2024|500.00'
        result = _stream_parse_line(line)
        assert result is not None
        kind, _, fields = result
        assert kind == "SB"
        assert fields["payee_name"] == "VENDOR, LLC | CONSULTANTS"

    @pytest.mark.parametrize("prefix", ["SA11A1", "SA15", "SA17"])
    def test_sa_subtypes(self, prefix):
        line = (
            f"{prefix}|C1|IND|DOE, JANE|||||90210"
            f"|01/01/2024|100.00|CORP|TITLE"
        )
        result = _stream_parse_line(line)
        assert result is not None
        assert result[0] == "SA"

    @pytest.mark.parametrize("prefix", ["SB21B", "SB23", "SB29"])
    def test_sb_subtypes(self, prefix):
        line = f"{prefix}|C1|ORG|VENDOR|||||02/01/2024|300.00"
        result = _stream_parse_line(line)
        assert result is not None
        assert result[0] == "SB"
