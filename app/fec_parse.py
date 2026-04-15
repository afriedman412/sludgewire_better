from __future__ import annotations

import csv
import io
import re
import hashlib
from datetime import date, datetime
from typing import Iterable, Optional, Tuple, List

import requests

# Lazy import fecfile - it's heavy and loads pandas/numpy
_fecfile = None

def _get_fecfile():
    global _fecfile
    if _fecfile is None:
        import fecfile
        _fecfile = fecfile
    return _fecfile


class FileTooLargeError(Exception):
    """Raised when a file exceeds the size limit."""
    def __init__(self, url: str, size_mb: float, limit_mb: float):
        self.url = url
        self.size_mb = size_mb
        self.limit_mb = limit_mb
        super().__init__(f"File {url} is {size_mb:.1f}MB, exceeds limit of {limit_mb}MB")


def check_file_size(fec_url: str, timeout: int = 10) -> Optional[float]:
    """Check file size via HEAD request. Returns size in MB, or None if unknown."""
    try:
        r = requests.head(fec_url, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        content_length = r.headers.get("Content-Length")
        if content_length:
            return int(content_length) / (1024 * 1024)  # Convert to MB
    except Exception:
        pass
    return None


def download_fec_text(fec_url: str, max_size_mb: float = None) -> str:
    """Download FEC file text. Optionally check size first."""
    if max_size_mb is not None:
        size_mb = check_file_size(fec_url)
        if size_mb is not None and size_mb > max_size_mb:
            raise FileTooLargeError(fec_url, size_mb, max_size_mb)

    r = requests.get(fec_url, timeout=60)
    r.raise_for_status()
    return r.text


def download_fec_header(fec_url: str, max_bytes: int = 50_000) -> str:
    """Stream just the first max_bytes of an FEC file (for header-only parsing).

    F3X header parsing only needs the first ~100 lines. Streaming avoids
    downloading 100MB+ files when we only need the first 50KB.
    """
    r = requests.get(fec_url, timeout=30, stream=True)
    r.raise_for_status()
    raw = b""
    for chunk in r.iter_content(chunk_size=8192):
        raw += chunk
        if len(raw) >= max_bytes:
            break
    r.close()
    return raw.decode("utf-8", errors="replace")


def parse_fec_filing(fec_text: str) -> dict:
    """Parse FEC filing text and return the parsed dict."""
    return _get_fecfile().loads(fec_text)


def parse_f3x_header_only(fec_text: str) -> dict:
    """
    Parse just the F3X header to extract summary fields.
    Much lighter than fecfile.loads() - doesn't load itemizations.

    Returns dict with 'filing' key containing header fields.
    """
    result = {"filing": {}}
    filing = result["filing"]

    # F3X files are pipe-delimited. Header is in first few lines.
    lines = fec_text.split('\n')

    for line in lines[:100]:  # Header is in first ~100 lines max
        if not line.strip():
            continue
        parts = line.split('|')
        if not parts:
            continue

        rec_type = parts[0].strip().upper()

        # F3X/F3XN/F3XA/F3XT record has committee name in position 2
        if rec_type.startswith('F3X') and len(parts) > 2:
            filing["form_type"] = parts[0].strip()
            filing["committee_name"] = parts[1].strip()
            filing["filer_committee_id_name"] = parts[1].strip()

        # Summary line (SA/SB) or look for "6a" type field which has total receipts
        # Actually, the totals are in specific summary records
        # In F3X, look for the line that starts with total receipts indicator

        # Try to find total receipts - usually in a line starting with specific codes
        # or we can try fecfile just on the first chunk
        if rec_type in ('F3X', 'F3XN', 'F3XA', 'F3XT'):
            # Try common positions for col_a_total_receipts (position varies by version)
            # In newer versions it's around position 6-7
            for pos in range(4, min(20, len(parts))):
                val_str = parts[pos].strip()
                if val_str and val_str.replace('.', '').replace('-', '').isdigit():
                    try:
                        val = float(val_str)
                        # Total receipts is usually a large positive number
                        if val >= 0 and "col_a_total_receipts" not in filing:
                            filing["col_a_total_receipts"] = val_str
                            break
                    except ValueError:
                        pass

    # Fallback: use fecfile on truncated text to fill in whatever is missing.
    # The positional scan above can find col_a_total_receipts but cannot
    # reliably distinguish disbursements by position, so we run fecfile to
    # get canonical values. Limited to first 200 lines (header/summary).
    need_receipts = "col_a_total_receipts" not in filing
    need_disbursements = "col_a_total_disbursements" not in filing
    if need_receipts or need_disbursements:
        try:
            truncated = '\n'.join(fec_text.split('\n')[:200])
            full_parsed = _get_fecfile().loads(truncated)
            filing_data = full_parsed.get("filing", {})
            if "col_a_total_receipts" in filing_data:
                filing["col_a_total_receipts"] = filing_data["col_a_total_receipts"]
            if "col_a_total_disbursements" in filing_data:
                filing["col_a_total_disbursements"] = filing_data["col_a_total_disbursements"]
            if "committee_name" not in filing and "committee_name" in filing_data:
                filing["committee_name"] = filing_data["committee_name"]
            if "filer_committee_id_name" in filing_data:
                filing["filer_committee_id_name"] = filing_data["filer_committee_id_name"]
            del full_parsed
            del truncated
        except Exception:
            pass

    return result


def f3x_total_receipts(fec_text: str) -> Optional[float]:
    parsed = _get_fecfile().loads(fec_text)
    val = parsed.get("filing", {}).get("col_a_total_receipts")
    if val in (None, ""):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def extract_committee_name(parsed: dict) -> Optional[str]:
    """
    Extract committee name from parsed FEC filing.
    Checks common field locations in fecfile parsed output.
    """
    filing = parsed.get("filing", {})

    # Try common field names for committee name
    for key in ("committee_name", "filer_committee_id_name", "filer_name"):
        val = filing.get(key)
        if val and isinstance(val, str) and val.strip():
            return val.strip()

    return None


def iter_pipe_rows(fec_text: str) -> Iterable[List[str]]:
    f = io.StringIO(fec_text)
    reader = csv.reader(f, delimiter="|")
    for row in reader:
        if row:
            yield row


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def extract_schedule_e_best_effort(fec_text: str, parsed: dict = None) -> Iterable[Tuple[str, dict]]:
    """
    Yields (raw_line, extracted_fields_dict).

    Uses fecfile parsed output for structured field extraction.
    Falls back to raw line parsing if fecfile fails.

    Args:
        fec_text: Raw FEC filing text
        parsed: Optional pre-parsed dict from _get_fecfile().loads() to avoid double-parsing
    """
    # First, try to get structured data from fecfile
    try:
        if parsed is None:
            parsed = _get_fecfile().loads(fec_text)
        filing = parsed.get("filing", {})
        itemizations = parsed.get("itemizations", {})

        # fecfile uses "Schedule E" as the key
        se_items = itemizations.get(
            "Schedule E", []) or itemizations.get("SE", [])

        for item in se_items:
            # Build raw_line from item for deduplication
            raw_line = "|".join(str(v) for v in item.values() if v)

            # Extract expenditure date (dissemination_date or disbursement_date)
            expenditure_date = None
            for date_field in ("dissemination_date", "disbursement_date", "expenditure_date"):
                val = item.get(date_field)
                if val:
                    if isinstance(val, datetime):
                        expenditure_date = val.date()
                    elif isinstance(val, date):
                        expenditure_date = val
                    elif isinstance(val, str):
                        expenditure_date = _parse_date_flexible(val)
                    if expenditure_date:
                        break

            # Extract amount
            amount = None
            amount_val = item.get("expenditure_amount")
            if amount_val is not None:
                try:
                    amount = float(amount_val)
                except (TypeError, ValueError):
                    pass

            # Build candidate name
            candidate_parts = [
                item.get("candidate_first_name", ""),
                item.get("candidate_middle_name", ""),
                item.get("candidate_last_name", ""),
            ]
            candidate_name = " ".join(p.strip()
                                      for p in candidate_parts if p and p.strip())

            # Build payee name
            payee_org = item.get("payee_organization_name", "")
            if payee_org and payee_org.strip():
                payee_name = payee_org.strip()
            else:
                payee_parts = [
                    item.get("payee_first_name", ""),
                    item.get("payee_last_name", ""),
                ]
                payee_name = " ".join(p.strip()
                                      for p in payee_parts if p and p.strip())

            yield raw_line, {
                "expenditure_date": expenditure_date,
                "amount": amount,
                "support_oppose": item.get("support_oppose_code"),
                "candidate_id": item.get("candidate_id_number"),
                "candidate_name": candidate_name or None,
                "candidate_office": item.get("candidate_office"),
                "candidate_state": item.get("candidate_state"),
                "candidate_district": item.get("candidate_district"),
                "candidate_party": item.get("so_cand_pty_affiliation"),
                "election_code": item.get("election_code"),
                "purpose": item.get("expenditure_purpose_descrip"),
                "payee_name": payee_name or None,
            }

        # If we got items from fecfile, we're done
        if se_items:
            return

    except Exception:
        pass  # Fall back to raw parsing

    # Fallback: raw line parsing for SE records
    for row in iter_pipe_rows(fec_text):
        rec = row[0]
        if not rec.startswith("SE"):
            continue

        raw_line = "|".join(row)

        # Heuristic extraction
        amount = None
        expenditure_date = None
        support_oppose = None

        for token in row:
            if expenditure_date is None:
                d = _parse_mmddyyyy(token)
                if d:
                    expenditure_date = d
            if amount is None:
                t = token.replace(",", "").strip()
                if re.fullmatch(r"-?\d+(\.\d+)?", t):
                    try:
                        val = float(t)
                        if abs(val) >= 1.0:
                            amount = val
                    except ValueError:
                        pass

        for token in row:
            if token in ("S", "O"):
                support_oppose = token
                break

        yield raw_line, {
            "expenditure_date": expenditure_date,
            "amount": amount,
            "support_oppose": support_oppose,
            "candidate_id": None,
            "candidate_name": None,
            "candidate_office": None,
            "candidate_state": None,
            "candidate_district": None,
            "candidate_party": None,
            "election_code": None,
            "purpose": None,
            "payee_name": None,
        }


def extract_schedule_a_best_effort(fec_text: str, parsed: dict = None) -> Iterable[Tuple[str, dict]]:
    """
    Yields (raw_line, extracted_fields_dict) for Schedule A items.

    Uses fecfile parsed output for structured field extraction.
    Falls back to raw line parsing if fecfile fails.
    """
    try:
        if parsed is None:
            parsed = _get_fecfile().loads(fec_text)
        itemizations = parsed.get("itemizations", {})

        sa_items = itemizations.get(
            "Schedule A", []) or itemizations.get("SA", [])

        for item in sa_items:
            raw_line = "|".join(str(v) for v in item.values() if v)

            # Build contributor name (org or individual)
            org_name = item.get("contributor_organization_name", "")
            if org_name and org_name.strip():
                contributor_name = org_name.strip()
            else:
                name_parts = [
                    item.get("contributor_first_name", ""),
                    item.get("contributor_middle_name", ""),
                    item.get("contributor_last_name", ""),
                ]
                contributor_name = " ".join(
                    p.strip() for p in name_parts if p and p.strip())
                suffix = item.get("contributor_suffix", "")
                if suffix and suffix.strip():
                    contributor_name += f" {suffix.strip()}"

            # Extract amount
            amount = None
            for amount_field in ("contribution_amount", "contribution_receipt_amount"):
                amount_val = item.get(amount_field)
                if amount_val is not None:
                    try:
                        amount = float(amount_val)
                        break
                    except (TypeError, ValueError):
                        pass

            # Extract date
            contribution_date = None
            for date_field in ("contribution_date", "contribution_receipt_date"):
                val = item.get(date_field)
                if val:
                    if isinstance(val, datetime):
                        contribution_date = val.date()
                    elif isinstance(val, date):
                        contribution_date = val
                    elif isinstance(val, str):
                        contribution_date = _parse_date_flexible(val)
                    if contribution_date:
                        break

            yield raw_line, {
                "contributor_name": contributor_name or None,
                "contributor_employer": (item.get("contributor_employer") or "").strip() or None,
                "contributor_occupation": (item.get("contributor_occupation") or "").strip() or None,
                "contribution_amount": amount,
                "contribution_date": contribution_date,
                "receipt_description": (item.get("receipt_description") or "").strip() or None,
                "contributor_type": item.get("entity_type"),
                "memo_text": (item.get("memo_text_description") or item.get("memo_text") or "").strip() or None,
            }

        if sa_items:
            return

    except Exception:
        pass

    # Fallback: raw line parsing for SA records
    for row in iter_pipe_rows(fec_text):
        rec = row[0]
        if not rec.startswith("SA"):
            continue

        raw_line = "|".join(row)

        amount = None
        contribution_date = None
        for token in row:
            if contribution_date is None:
                d = _parse_mmddyyyy(token)
                if d:
                    contribution_date = d
            if amount is None:
                t = token.replace(",", "").strip()
                if re.fullmatch(r"-?\d+(\.\d+)?", t):
                    try:
                        val = float(t)
                        if abs(val) >= 0.01:
                            amount = val
                    except ValueError:
                        pass

        yield raw_line, {
            "contributor_name": None,
            "contributor_employer": None,
            "contributor_occupation": None,
            "contribution_amount": amount,
            "contribution_date": contribution_date,
            "receipt_description": None,
            "contributor_type": None,
            "memo_text": None,
        }


def extract_schedule_b_best_effort(fec_text: str, parsed: dict = None) -> Iterable[Tuple[str, dict]]:
    """
    Yields (raw_line, extracted_fields_dict) for Schedule B (disbursement)
    items. Uses fecfile parsed output for structured field extraction.
    Falls back to raw line parsing if fecfile fails.

    The returned dict contains: payee_name, payee_employer, payee_occupation,
    disbursement_amount, disbursement_date, disbursement_description,
    payee_type, purpose, category_code, memo_text, beneficiary_candidate_id,
    beneficiary_candidate_name.
    """
    try:
        if parsed is None:
            parsed = _get_fecfile().loads(fec_text)
        itemizations = parsed.get("itemizations", {})

        sb_items = itemizations.get(
            "Schedule B", []) or itemizations.get("SB", [])

        for item in sb_items:
            raw_line = "|".join(str(v) for v in item.values() if v)

            # Build payee name (org or individual)
            org_name = item.get("payee_organization_name", "")
            if org_name and org_name.strip():
                payee_name = org_name.strip()
            else:
                name_parts = [
                    item.get("payee_first_name", ""),
                    item.get("payee_middle_name", ""),
                    item.get("payee_last_name", ""),
                ]
                payee_name = " ".join(
                    p.strip() for p in name_parts if p and p.strip())
                suffix = item.get("payee_suffix", "")
                if suffix and suffix.strip():
                    payee_name += f" {suffix.strip()}"

            amount = None
            for amount_field in (
                "expenditure_amount",
                "disbursement_amount",
            ):
                amount_val = item.get(amount_field)
                if amount_val is not None:
                    try:
                        amount = float(amount_val)
                        break
                    except (TypeError, ValueError):
                        pass

            disbursement_date = None
            for date_field in (
                "expenditure_date",
                "disbursement_date",
            ):
                val = item.get(date_field)
                if val:
                    if isinstance(val, datetime):
                        disbursement_date = val.date()
                    elif isinstance(val, date):
                        disbursement_date = val
                    elif isinstance(val, str):
                        disbursement_date = _parse_date_flexible(val)
                    if disbursement_date:
                        break

            purpose = (
                item.get("expenditure_purpose_descrip")
                or item.get("purpose_of_disbursement")
                or ""
            ).strip() or None

            beneficiary_parts = [
                item.get("beneficiary_candidate_first_name", ""),
                item.get("beneficiary_candidate_last_name", ""),
            ]
            beneficiary_name = " ".join(
                p.strip() for p in beneficiary_parts if p and p.strip()
            ) or None

            yield raw_line, {
                "payee_name": payee_name or None,
                "payee_employer": (
                    item.get("payee_employer") or ""
                ).strip() or None,
                "payee_occupation": (
                    item.get("payee_occupation") or ""
                ).strip() or None,
                "disbursement_amount": amount,
                "disbursement_date": disbursement_date,
                "disbursement_description": (
                    item.get("disbursement_description") or ""
                ).strip() or None,
                "payee_type": item.get("entity_type"),
                "purpose": purpose,
                "category_code": (
                    item.get("category_code") or ""
                ).strip() or None,
                "memo_text": (
                    item.get("memo_text_description")
                    or item.get("memo_text")
                    or ""
                ).strip() or None,
                "beneficiary_candidate_id": (
                    item.get("beneficiary_candidate_id_number") or ""
                ).strip() or None,
                "beneficiary_candidate_name": beneficiary_name,
            }

        if sb_items:
            return

    except Exception:
        pass

    # Fallback: raw line parsing for SB records
    for row in iter_pipe_rows(fec_text):
        rec = row[0]
        if not rec.startswith("SB"):
            continue

        raw_line = "|".join(row)
        amount = _best_amount(row)
        disbursement_date = _first_date(row)
        payee_name = _first_name_like(row)

        yield raw_line, {
            "payee_name": payee_name,
            "payee_employer": None,
            "payee_occupation": None,
            "disbursement_amount": amount,
            "disbursement_date": disbursement_date,
            "disbursement_description": None,
            "payee_type": None,
            "purpose": None,
            "category_code": None,
            "memo_text": None,
            "beneficiary_candidate_id": None,
            "beneficiary_candidate_name": None,
        }


def stream_sa_sb_from_url(
    fec_url: str,
    timeout: int = 120,
) -> Iterable[Tuple[str, str, dict]]:
    """
    Stream a large FEC filing directly from URL, yielding SA and SB items
    without loading the full file into memory. Bounded memory regardless
    of file size.

    Yields (kind, raw_line, fields) tuples where kind is "SA" or "SB".
    Field accuracy is lower than fecfile — memo_text, employer, occupation,
    beneficiary_candidate_id are typically None. Amount, date, payee/
    contributor name (via CSV-indexed read) are extracted.

    Use this only when fecfile would OOM (filings > ~300MB).
    """
    r = requests.get(
        fec_url,
        timeout=(30, timeout),
        stream=True,
    )
    r.raise_for_status()

    # iter_lines with a generous decode; FEC files are latin-1 safe
    buf = io.StringIO()
    for chunk in r.iter_content(chunk_size=65536, decode_unicode=False):
        if not chunk:
            continue
        buf.write(chunk.decode("utf-8", errors="replace"))
        buf.seek(0)
        remaining = buf.read()
        lines = remaining.split("\n")
        # Last item may be an incomplete line — hold it back in the buffer.
        incomplete = lines[-1]
        complete_lines = lines[:-1]
        for line in complete_lines:
            parsed = _stream_parse_line(line)
            if parsed is not None:
                yield parsed
        buf = io.StringIO()
        buf.write(incomplete)
    # Flush any final buffered line.
    final = buf.getvalue()
    if final.strip():
        parsed = _stream_parse_line(final)
        if parsed is not None:
            yield parsed
    r.close()


def _stream_parse_line(line: str) -> Optional[Tuple[str, str, dict]]:
    """
    Parse one pipe-delimited record line. Returns (kind, raw_line, fields)
    for SA/SB records, None otherwise. Uses csv.reader to handle embedded
    quoting and delimiters correctly.
    """
    if not line or not line.strip():
        return None
    # csv.reader expects an iterable of lines
    try:
        row = next(csv.reader([line], delimiter="|"))
    except Exception:
        return None
    if not row:
        return None

    rec = row[0].strip()
    if rec.startswith("SA"):
        return ("SA", "|".join(row), _extract_sa_fields_positional(row))
    if rec.startswith("SB"):
        return ("SB", "|".join(row), _extract_sb_fields_positional(row))
    return None


_ZIP_RE = re.compile(r"^\d{5}(\d{4})?$")


def _looks_like_amount(tok: str) -> Optional[float]:
    """Return the float if the token looks like a monetary amount, else None.
    Rejects 5/9-digit integers (ZIP codes) and zero."""
    t = tok.replace(",", "").strip()
    if not t or not re.fullmatch(r"-?\d+(\.\d+)?", t):
        return None
    if "." not in t and _ZIP_RE.match(t):
        return None
    try:
        val = float(t)
    except ValueError:
        return None
    if abs(val) < 0.01:
        return None
    return val


def _best_amount(row: List[str]) -> Optional[float]:
    """Scan backwards — transaction amount is typically in a late field,
    after name/address/city/state/zip. Prefers tokens with decimal points."""
    # First pass: decimal-containing amounts (almost certainly money).
    for tok in reversed(row):
        t = (tok or "").replace(",", "").strip()
        if "." in t:
            val = _looks_like_amount(tok)
            if val is not None:
                return val
    # Second pass: any amount-shaped token (skipping ZIPs).
    for tok in reversed(row):
        val = _looks_like_amount(tok)
        if val is not None:
            return val
    return None


def _first_date(row: List[str]) -> Optional[date]:
    for tok in row:
        d = _parse_mmddyyyy(tok)
        if d:
            return d
    return None


def _first_name_like(row: List[str]) -> Optional[str]:
    """First long alphabetic token past the record-type/committee-id slots."""
    for i in range(2, min(len(row), 15)):
        token = (row[i] or "").strip()
        if not token or len(token) <= 3:
            continue
        if not re.search(r"[A-Za-z]", token):
            continue
        if _parse_mmddyyyy(token):
            continue
        if re.fullmatch(r"[A-Z]{2}", token):  # state code
            continue
        return token
    return None


def _extract_sa_fields_positional(row: List[str]) -> dict:
    """
    Extract SA fields heuristically from a raw pipe-delimited row.
    Finds the first parseable date and the first reasonable amount.
    Contributor name is taken from the first long non-numeric field
    after the filer committee id (position 1-2 typically).
    """
    contribution_date = _first_date(row)
    amount = _best_amount(row)
    contributor_name = _first_name_like(row)

    return {
        "contributor_name": contributor_name,
        "contributor_employer": None,
        "contributor_occupation": None,
        "contribution_amount": amount,
        "contribution_date": contribution_date,
        "receipt_description": None,
        "contributor_type": None,
        "memo_text": None,
    }


def _extract_sb_fields_positional(row: List[str]) -> dict:
    """
    Extract SB fields heuristically from a raw pipe-delimited row.
    Same approach as SA: first parseable date, first reasonable amount,
    first long alphabetic token as payee name.
    """
    disbursement_date = _first_date(row)
    amount = _best_amount(row)
    payee_name = _first_name_like(row)

    return {
        "payee_name": payee_name,
        "payee_employer": None,
        "payee_occupation": None,
        "disbursement_amount": amount,
        "disbursement_date": disbursement_date,
        "disbursement_description": None,
        "payee_type": None,
        "purpose": None,
        "category_code": None,
        "memo_text": None,
        "beneficiary_candidate_id": None,
        "beneficiary_candidate_name": None,
    }


def _parse_date_flexible(s: str) -> Optional[date]:
    """Parse date from various formats."""
    if not s:
        return None
    s = s.strip()
    # Try ISO format first (from fecfile datetime objects converted to string)
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S%z"):
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            pass
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def _parse_mmddyyyy(s: str) -> Optional[date]:
    s = (s or "").strip()
    try:
        return datetime.strptime(s, "%m/%d/%Y").date()
    except ValueError:
        return None
