import time
from datetime import datetime, timezone
from typing import List, Optional

from kaiano_common_utils import logger as log

from floor_trials_processor.state import SpreadsheetState


def parse_utc_datetime(value: str) -> Optional[datetime]:
    """Parse a UTC datetime string, forgiving of ' UTC' or 'Z' suffixes."""
    if not value:
        return None
    value = value.strip()

    # Normalize variants
    value = value.replace("Z", "").replace("UTC", "").replace("GMT", "").strip()

    for fmt in (
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    log.warning(f"Could not parse UTC datetime '{value}'")
    return None


def fetch_sheet_values(
    service, spreadsheet_id: str, range_name: str
) -> List[List[str]]:
    """Fetch values from a Google Sheet range."""
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_name)
            .execute()
        )
        return result.get("values", [])
    except Exception as e:
        log.error(f"Error fetching range {range_name}: {e}")
        return []


def get_single_cell(service, spreadsheet_id: str, cell_range: str) -> str:
    """Fetch a single cell value from a Google Sheet."""
    result = fetch_sheet_values(service, spreadsheet_id, cell_range)
    return result[0][0] if result and result[0] else ""


def get_value(service, spreadsheet_id: str, range_: str) -> str:
    """Get the first cell value from a range safely."""
    try:
        rows = fetch_sheet_values(service, spreadsheet_id, range_)
        return rows[0][0] if rows and rows[0] else ""
    except Exception as e:
        log.warning(f"Error getting value from {range_}: {e}")
        return ""


def write_sheet_value(
    service,
    spreadsheet_id: str,
    sheet_range: str,
    values,
    value_input_option: str = "RAW",
):
    """
    Write a single value or row of values to a specified range in a Google Sheet.
    Accepts optional value_input_option ('RAW' or 'USER_ENTERED').
    """
    if not isinstance(values, list):
        values = [[values]]
    elif values and not isinstance(values[0], list):
        values = [values]

    body = {"values": values}
    try:
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=sheet_range,
            valueInputOption=value_input_option,
            body=body,
        ).execute()
    except Exception as e:
        log.error(
            f"write_sheet_value: Failed to write to {sheet_range}: {e}", exc_info=True
        )
        raise


def names_match(l1: str, f1: str, d1: str, l2: str, f2: str, d2: str) -> bool:
    """Compare two leader/follower/division triplets ignoring last names."""

    def first_word(name: str) -> str:
        return name.strip().split(" ")[0].lower() if name else ""

    return (
        first_word(l1) == first_word(l2)
        and first_word(f1) == first_word(f2)
        and (d1 or "").strip().lower() == (d2 or "").strip().lower()
    )


def retry_on_exception(fn, *args, retries: int = 3, delay: float = 1.0, **kwargs):
    """Retry function on exception with exponential backoff."""
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            log.warning(f"Attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (2**attempt))
            else:
                log.error(f"All {retries} retries failed for {fn.__name__}")
                raise


def format_duration(seconds: float) -> str:
    """Format seconds into a human-readable string (1.2s)."""
    return f"{seconds:.1f}s"


# ---------------------------------------------------------------------
# Helper: Clean and compact queue for in-memory queue sections
# ---------------------------------------------------------------------


def clean_and_compact_queue(data: List[List[str]], name: str) -> List[List[str]]:
    cleaned = [[str(c).strip() for c in r] for r in data]
    non_empty = [
        r[:5] + [""] * (5 - len(r))
        for r in cleaned
        if any(r and str(c).strip() for c in r)
    ]
    empty = [[""] * 5 for _ in range(len(data) - len(non_empty))]
    compacted = non_empty + empty
    log.info(
        f"clean_and_compact_queue: {name} — {len(non_empty)} non-empty, {len(empty)} empty rows after cleaning."
    )
    return compacted


# ---------------------------------------------------------------------
# Helper: Audit queues for ghost gaps (empty row above non-empty)
# ---------------------------------------------------------------------
def audit_queues(state: "SpreadsheetState"):
    for name in ["priority_queue", "non_priority_queue", "current_queue"]:
        data = state.sections[name]["data"]
        for idx, row in enumerate(data[:-1]):
            if not any(str(c).strip() for c in row) and any(
                str(c).strip() for c in data[idx + 1]
            ):
                log.warning(
                    f"⚠️ Gap detected in {name} between rows {idx+1} and {idx+2}"
                )


def increment_run_count_in_memory(
    state: "SpreadsheetState", leader: str, follower: str, division: str
) -> bool:
    """
    Increment the run count for the given (leader, follower, division) in-memory.
    Matches the specific leader+follower+division triplet from the processed item
    against each report row; leader and follower are *not* compared to each other.
    """
    if "reports" not in state.sections:
        log.warning("No 'reports' section found in state; cannot increment run count.")
        return False

    try:
        report_data = state.sections["reports"]["data"]
        # Normalize inputs once (names_match handles first-token comparison).
        leader_n = (leader or "").strip()
        follower_n = (follower or "").strip()
        division_n = (division or "").strip()

        for idx, row in enumerate(report_data):
            r_leader = str(row[0]).strip() if len(row) > 0 else ""
            r_follower = str(row[1]).strip() if len(row) > 1 else ""
            r_division = str(row[2]).strip() if len(row) > 2 else ""

            if names_match(
                r_leader, r_follower, r_division, leader_n, follower_n, division_n
            ):
                # Ensure row has at least 5 columns (A..E) where E is run count
                if len(row) < 5:
                    row.extend([""] * (5 - len(row)))
                try:
                    count = int(str(row[4]).strip()) if str(row[4]).strip() else 0
                except Exception:
                    count = 0
                row[4] = str(count + 1)
                state.mark_dirty("reports")
                log.info(
                    f"--------------------Incremented in-memory run count for {leader_n}/{follower_n}/{division_n} (row {idx+1})"
                )
                return True

        log.debug(
            f"--------------------No matching reports row found for {leader_n}/{follower_n}/{division_n}; run count not incremented."
        )
        return False

    except Exception as e:
        log.error(
            f"--------------------increment_run_count_in_memory: Error updating run count: {e}",
            exc_info=True,
        )
        return False
