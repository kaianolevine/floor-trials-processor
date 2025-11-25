import time
from datetime import datetime, timezone
from typing import List, Optional

from kaiano_common_utils import logger as log

from floor_trials_processor import config
from floor_trials_processor.state import SpreadsheetState


def parse_utc_datetime(value: str) -> Optional[datetime]:
    """
    Parse a UTC datetime string, forgiving of ' UTC', 'Z', or 'GMT' suffixes.

    Args:
        value (str): The datetime string to parse.

    Returns:
        Optional[datetime]: Parsed datetime with UTC timezone, or None if parsing fails.
    """
    if not value:
        return None
    value = value.strip()

    # Normalize variants by removing timezone suffixes
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

    log.warning(f"⚠️ WARNING: Could not parse UTC datetime '{value}'")
    return None


def fetch_sheet_values(
    service, spreadsheet_id: str, range_name: str
) -> List[List[str]]:
    """
    Fetch values from a Google Sheet range.

    Args:
        service: Google Sheets API service instance.
        spreadsheet_id (str): Spreadsheet identifier.
        range_name (str): Range string to fetch.

    Returns:
        List[List[str]]: List of rows with cell values; empty list on error.
    """
    try:

        def _get_call():
            return (
                service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=range_name)
                .execute()
            )

        result = retry_on_exception(_get_call)
        return result.get("values", [])
    except Exception as e:
        log.error(f"❌ ERROR: Error fetching range {range_name}: {e}")
        return []


def get_single_cell(service, spreadsheet_id: str, cell_range: str) -> str:
    """
    Fetch a single cell value from a Google Sheet.

    Args:
        service: Google Sheets API service instance.
        spreadsheet_id (str): Spreadsheet identifier.
        cell_range (str): Single cell range string.

    Returns:
        str: The cell value or empty string if unavailable.
    """
    result = retry_on_exception(fetch_sheet_values, service, spreadsheet_id, cell_range)
    return result[0][0] if result and result[0] else ""


def get_value(service, spreadsheet_id: str, range_: str) -> str:
    """
    Get the first cell value from a range safely.

    Args:
        service: Google Sheets API service instance.
        spreadsheet_id (str): Spreadsheet identifier.
        range_ (str): Range string to fetch.

    Returns:
        str: The first cell value or empty string on error.
    """
    try:
        rows = retry_on_exception(fetch_sheet_values, service, spreadsheet_id, range_)
        return rows[0][0] if rows and rows[0] else ""
    except Exception as e:
        log.warning(f"⚠️ WARNING: Error getting value from {range_}: {e}")
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

    Args:
        service: Google Sheets API service instance.
        spreadsheet_id (str): Spreadsheet identifier.
        sheet_range (str): Range string to write to.
        values: Single value or list of values (single row or multiple rows).
        value_input_option (str): 'RAW' or 'USER_ENTERED' input mode.

    Raises:
        Exception: Propagates exceptions from the API call.
    """
    if not isinstance(values, list):
        values = [[values]]
    elif values and not isinstance(values[0], list):
        values = [values]

    body = {"values": values}
    try:

        def _update_call():
            return (
                service.spreadsheets()
                .values()
                .update(
                    spreadsheetId=spreadsheet_id,
                    range=sheet_range,
                    valueInputOption=value_input_option,
                    body=body,
                )
                .execute()
            )

        retry_on_exception(_update_call)
    except Exception as e:
        log.error(
            f"❌ ERROR: write_sheet_value failed to write to {sheet_range}: {e}",
            exc_info=True,
        )
        raise


def names_match(l1: str, f1: str, d1: str, l2: str, f2: str, d2: str) -> bool:
    """
    Compare two leader/follower/division triplets ignoring last names.

    Args:
        l1, f1, d1 (str): Leader, follower, division for first entry.
        l2, f2, d2 (str): Leader, follower, division for second entry.

    Returns:
        bool: True if first words of leader and follower match and divisions match case-insensitively.
    """

    def first_word(name: str) -> str:
        return name.strip().split(" ")[0].lower() if name else ""

    return (
        first_word(l1) == first_word(l2)
        and first_word(f1) == first_word(f2)
        and (d1 or "").strip().lower() == (d2 or "").strip().lower()
    )


def retry_on_exception(fn, *args, retries: int = 3, delay: float = 1.0, **kwargs):
    """
    Retry function on exception with exponential backoff.

    Args:
        fn: Function to execute.
        *args: Positional arguments for fn.
        retries (int): Number of retry attempts.
        delay (float): Initial delay between retries in seconds.
        **kwargs: Keyword arguments for fn.

    Returns:
        Any: The return value of fn if successful.

    Raises:
        Exception: Last exception raised if all retries fail.
    """
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            log.warning(f"⚠️ WARNING: Attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (2**attempt))
            else:
                log.error(f"❌ ERROR: All {retries} retries failed for {fn.__name__}")
                raise


def format_duration(seconds: float) -> str:
    """
    Format seconds into a human-readable string (e.g., '1.2s').

    Args:
        seconds (float): Duration in seconds.

    Returns:
        str: Formatted duration string.
    """
    return f"{seconds:.1f}s"


# ---------------------------------------------------------------------
# Helper: Clean and compact queue for in-memory queue sections
# ---------------------------------------------------------------------


def clean_and_compact_queue(data: List[List[str]], name: str) -> List[List[str]]:
    """
    Clean and compact queue data by trimming whitespace and moving empty rows to the end.

    Args:
        data (List[List[str]]): Raw queue data rows.
        name (str): Name of the queue for logging.

    Returns:
        List[List[str]]: Cleaned and compacted queue data.
    """
    cleaned = [[str(c).strip() for c in r] for r in data]
    non_empty = [
        r[:5] + [""] * (5 - len(r))
        for r in cleaned
        if any(r and str(c).strip() for c in r)
    ]
    empty = [[""] * 5 for _ in range(len(data) - len(non_empty))]
    compacted = non_empty + empty
    log.info(
        f"✅ INFO: clean_and_compact_queue: {name} — "
        f"{len(non_empty)} non-empty, {len(empty)} empty rows after cleaning."
    )
    return compacted


# ---------------------------------------------------------------------
# Helper: Audit queues for ghost gaps (empty row above non-empty)
# ---------------------------------------------------------------------


def audit_queues(state: "SpreadsheetState"):
    """
    Audit queues for ghost gaps: empty rows immediately preceding non-empty rows.

    Args:
        state (SpreadsheetState): Current spreadsheet state.
    """
    for name in ["priority_queue", "non_priority_queue", "current_queue"]:
        data = state.sections[name]["data"]
        for idx, row in enumerate(data[:-1]):
            if not any(str(c).strip() for c in row) and any(
                str(c).strip() for c in data[idx + 1]
            ):
                log.warning(
                    f"⚠️ WARNING: Gap detected in {name} between rows {idx+1} and {idx+2}"
                )


def increment_run_count_in_memory(
    state: "SpreadsheetState", leader: str, follower: str, division: str
) -> bool:
    """
    Increment the run count for the given (leader, follower, division) in-memory.

    Matches the specific leader+follower+division triplet from the processed item
    against each report row; leader and follower are compared by first word only.

    Args:
        state (SpreadsheetState): Current spreadsheet state.
        leader (str): Leader name.
        follower (str): Follower name.
        division (str): Division name.

    Returns:
        bool: True if run count incremented; False otherwise.
    """
    if "reports" not in state.sections:
        log.warning(
            "⚠️ WARNING: No 'reports' section found in state; cannot increment run count."
        )
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
                    f"✅ INFO: Incremented in-memory run count for "
                    f"{leader_n}/{follower_n}/{division_n} (row {idx+1})"
                )
                return True

        log.debug(
            f"🧩 DEBUG: No matching reports row found for "
            f"{leader_n}/{follower_n}/{division_n}; run count not incremented."
        )
        return False

    except Exception as e:
        log.error(
            f"❌ ERROR: increment_run_count_in_memory failed updating run count: {e}",
            exc_info=True,
        )
        return False


def update_floor_trial_status(service, spreadsheet_id):
    """
    Update Floor Trial status using UTC datetimes in config-defined cells.

    Args:
        service: Google Sheets API service instance.
        spreadsheet_id (str): Spreadsheet identifier.

    Returns:
        bool: True if status is 'in progress', False otherwise or on error.
    """
    try:
        ranges = [
            config.FLOOR_OPEN_RANGE,
            config.FLOOR_START_RANGE,
            config.FLOOR_END_RANGE,
        ]
        result = (
            service.spreadsheets()
            .values()
            .batchGet(spreadsheetId=spreadsheet_id, ranges=ranges)
            .execute()
        )
        open_val = result.get("valueRanges", [])[0].get("values", [])
        start_val = result.get("valueRanges", [])[1].get("values", [])
        end_val = result.get("valueRanges", [])[2].get("values", [])
        open_str = open_val[0][0].strip() if open_val and open_val[0] else ""
        start_str = start_val[0][0].strip() if start_val and start_val[0] else ""
        end_str = end_val[0][0].strip() if end_val and end_val[0] else ""

        dt_open = parse_utc_datetime(open_str)
        dt_start = parse_utc_datetime(start_str)
        dt_end = parse_utc_datetime(end_str)
        now_utc = datetime.now(timezone.utc)

        log.info(
            f"✅ INFO: update_floor_trial_status: Open={dt_open}, Start={dt_start}, "
            f"End={dt_end}, Now={now_utc}"
        )

        status = config.STATUS_NOT_ACTIVE
        if dt_start and dt_open and dt_end:
            if dt_start <= now_utc <= dt_end:
                status = config.STATUS_IN_PROGRESS
            elif dt_open <= now_utc < dt_start:
                status = config.STATUS_OPEN
            elif now_utc > dt_end:
                status = config.STATUS_NOT_ACTIVE

        # Use write_sheet_value; fallback to direct API call if needed
        try:
            write_sheet_value(
                service, spreadsheet_id, config.FLOOR_STATUS_RANGE, [status, "", ""]
            )
        except Exception:
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=config.FLOOR_STATUS_RANGE,
                valueInputOption="RAW",
                body={"values": [[status, "", ""]]},
            ).execute()

        log.info(f"✅ Updated status: '{status}'")
        if status == config.STATUS_IN_PROGRESS:
            return True
        else:
            return False
    except Exception as e:
        log.error(f"❌ ERROR: update_floor_trial_status exception: {e}", exc_info=True)
        return False


def get_floor_trial_times(service, spreadsheet_id):
    """
    Retrieve UTC datetimes for floor trial open, start, and end times.

    Args:
        service: Google Sheets API service instance.
        spreadsheet_id (str): Spreadsheet identifier.

    Returns:
        dict: {
            "open": datetime or None,
            "start": datetime or None,
            "end": datetime or None
        }
    """
    try:
        ranges = [
            config.FLOOR_OPEN_RANGE,
            config.FLOOR_START_RANGE,
            config.FLOOR_END_RANGE,
        ]
        result = (
            service.spreadsheets()
            .values()
            .batchGet(spreadsheetId=spreadsheet_id, ranges=ranges)
            .execute()
        )
        open_val = result.get("valueRanges", [])[0].get("values", [])
        start_val = result.get("valueRanges", [])[1].get("values", [])
        end_val = result.get("valueRanges", [])[2].get("values", [])
        open_str = open_val[0][0].strip() if open_val and open_val[0] else ""
        start_str = start_val[0][0].strip() if start_val and start_val[0] else ""
        end_str = end_val[0][0].strip() if end_val and end_val[0] else ""

        dt_open = parse_utc_datetime(open_str)
        dt_start = parse_utc_datetime(start_str)
        dt_end = parse_utc_datetime(end_str)

        log.info(
            f"✅ INFO: get_floor_trial_times: Open={dt_open}, Start={dt_start}, End={dt_end}"
        )

        return {"open": dt_open, "start": dt_start, "end": dt_end}
    except Exception as e:
        log.error(f"❌ ERROR: get_floor_trial_times exception: {e}", exc_info=True)
        return {"open": None, "start": None, "end": None}
