import time
from typing import Any, List

from kaiano_common_utils import logger as log

from floor_trials_processor.state import SpreadsheetState


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
    service, spreadsheet_id: str, cell_range: str, value: Any
) -> None:
    """Write a single value or row of values to the specified cell range."""
    try:
        if isinstance(value, list):
            body = {"values": [value]}
        else:
            body = {"values": [[value]]}
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=cell_range,
            valueInputOption="RAW",
            body=body,
        ).execute()
        log.debug(f"✅ Wrote value(s) to {cell_range}: {value}")
    except Exception as e:
        log.error(f"❌ Failed to write value to {cell_range}: {e}")


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
