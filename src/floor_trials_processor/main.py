#!/usr/bin/env python3
"""
main.py — Continuous Google Sheets watcher + processor utilising kaiano-common-utils.

This script will run for a configurable time, poll a configurable sheet range at a configurable interval,
and when values change in the watched sheet it will trigger processing (moving rows etc.).
"""


import copy
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional

from kaiano_common_utils import google_sheets as sheets
from kaiano_common_utils import logger as log

# === CONFIGURATION CONSTANTS ===
SHEET_ID = "1JsWQDnxHis79dHcdlZZlgO1HS5Ldlq8zvLD1FdEwBQ4"

CURRENT_SHEET = "Current"
PRIORITY_SHEET = "Priority"
NON_PRIORITY_SHEET = "NonPriority"
REPORTS_SHEET = "Reports"
REJECTED_SHEET = "RejectedSubmissions"
EXTERNAL_SHEET_ID = "193QJBSQKkW1-c2Z3WHv3o2rbX-zZwn9fnmEACj88cEw"

# Ranges
CURRENT_QUEUE_RANGE = "Current!E6:I12"
PRIORITY_QUEUE_RANGE = "Priority!B3:F"
NON_PRIORITY_QUEUE_RANGE = "NonPriority!B3:F"
REPORTS_RANGE = "Reports!A4:E"
REJECTED_RANGE = "RejectedSubmissions!B4:H"

# Time-related ranges
FLOOR_OPEN_RANGE = "Current!B15"
FLOOR_START_RANGE = "Current!B16"
FLOOR_END_RANGE = "Current!B17"
FLOOR_STATUS_RANGE = "Current!BCD19"
CURRENT_UTC_CELL = "Current!D2"

MAX_RAW_BATCH_SIZE = 20
MAX_RUN_COUNT_FOR_PRIORITY = 3
SYNC_INTERVAL_SECONDS = 60

# === New runtime and start delay constants ===
MAX_RUNTIME_HOURS = 5
MAX_START_DELAY_HOURS = 1.5
def get_value(service, spreadsheet_id, range_):
    try:
        rows = fetch_sheet_values(service, spreadsheet_id, range_)
        return rows[0][0] if rows and rows[0] else ""
    except Exception as e:
        log.warning(f"Error getting value from {range_}: {e}")
        return ""


# ---------------------------------------------------------------------
# Helper: Check if next floor trial is within MAX_START_DELAY_HOURS
# ---------------------------------------------------------------------
def should_start_run(service, spreadsheet_id):
    """Return False if the next floor trial starts more than 1.5 hours away."""
    from datetime import timedelta, datetime, timezone
    start_str = get_value(service, spreadsheet_id, FLOOR_START_RANGE)
    dt_start = None
    if start_str:
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                dt_start = datetime.strptime(start_str.strip(), fmt).replace(tzinfo=timezone.utc)
                break
            except Exception:
                continue
    if not dt_start:
        log.warning("No valid floor trial start time — exiting gracefully.")
        return False

    now_utc = datetime.now(timezone.utc)
    if dt_start > now_utc + timedelta(hours=MAX_START_DELAY_HOURS):
        log.info(f"Floor trial starts at {dt_start} (more than {MAX_START_DELAY_HOURS} hours away) — exiting early.")
        return False
    return True

# Status text
STATUS_NOT_ACTIVE = "Floor Trials Not Active"
STATUS_IN_PROGRESS = "Floor Trials In Progress"
STATUS_OPEN = "Floor Trials Open for Submissions"
STATUS_CLOSED = "Floor Trials Closed"

# Additional sheet/range constants (for backward compatibility)
SHEET_RANGE = f"{CURRENT_SHEET}!A:Z"
INTERVAL_SECONDS = 10
DURATION_MINUTES = 60
MONITOR_RANGE = f"{CURRENT_SHEET}!C6:C11"
HISTORY_SHEET_NAME = "History"
RAW_SUBMISSIONS_RANGE = "RawSubmissions!C4:F"
REPORT_RANGE = "Report!A4:E"
COMPACTION_RANGE = "Current!E6:J12"
FLOOR_TRIAL_STATUS_RANGE = "Current!B19:D19"
FLOOR_TRIAL_DATE_CELL = "Current!D15"
FLOOR_TRIAL_START_CELL = "Current!C17"
FLOOR_TRIAL_END_CELL = "Current!D17"
AUTOMATION_CONTROL_CELL = "Current!H2"
PRIORITY_DIVISION_RANGE = "Current!F16:F30"
PRIORITY_FLAG_RANGE = "Current!G16:G30"
REJECTED_SUBMISSIONS_RANGE = "RejectedSubmissions!B:H"
EXTERNAL_SOURCE_RANGE = "Form Responses 1!A2:H"
DEBUG_UTC_MODE = True  # Set to False to disable verbose UTC verification


def load_state_from_sheets(service, spreadsheet_id):
    def get_values(range_name, expected_cols):
        try:
            result = (
                service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=range_name)
                .execute()
            )
            values = result.get("values", [])
            return [row + [""] * (expected_cols - len(row)) for row in values]
        except Exception as e:
            log.error(f"Error loading range {range_name}: {e}")
            return []

    log.info("Loading spreadsheet state from Google Sheets...")

    current = get_values(CURRENT_QUEUE_RANGE, 5)
    priority = get_values(PRIORITY_QUEUE_RANGE, 5)
    nonpriority = get_values(NON_PRIORITY_QUEUE_RANGE, 5)
    report = get_values(REPORTS_RANGE, 5)
    rejected = get_values(REJECTED_RANGE, 7)

    log.info(
        f"Loaded state: {len(current)} current, {len(priority)} priority, "
        f"{len(nonpriority)} nonpriority, {len(report)} reports, {len(rejected)} rejected"
    )

    state = SpreadsheetState()
    state.sections["current_queue"]["data"] = current
    state.sections["priority_queue"]["data"] = priority
    state.sections["non_priority_queue"]["data"] = nonpriority
    state.sections["report"]["data"] = report
    state.sections["rejected_submissions"]["data"] = rejected

    # --- Compact all queues after loading ---
    for queue_name in ["current_queue", "priority_queue", "non_priority_queue"]:
        data = state.sections[queue_name]["data"]
        if not data:
            continue
        # Ensure each row has exactly 5 columns (pad or truncate)
        data = [r + [""] * (5 - len(r)) if len(r) < 5 else r[:5] for r in data]
        # Move all non-empty rows up, empty rows down, preserve order, pad to same total rows
        non_empty = [r for r in data if any(str(c).strip() for c in r)]
        empty = [r for r in data if not any(str(c).strip() for c in r)]
        compacted = non_empty + empty
        # Pad if needed to preserve row count
        while len(compacted) < len(data):
            # Use correct column count for padding
            pad_cols = 5
            compacted.append([""] * pad_cols)
        state.sections[queue_name]["data"] = compacted
        log.info(
            f"Compacted {queue_name} on startup — {len(non_empty)} non-empty rows pushed up and padded to 5 columns."
        )

    return state


def names_match(l1: str, f1: str, d1: str, l2: str, f2: str, d2: str) -> bool:
    def first_word(name: str) -> str:
        return name.strip().split(" ")[0].lower() if name else ""

    return (
        first_word(l1) == first_word(l2)
        and first_word(f1) == first_word(f2)
        and (d1 or "").strip().lower() == (d2 or "").strip().lower()
    )


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


def verify_utc_timing(service, sheet_id):
    """Log UTC-based timing diagnostics for the Floor Trial schedule."""
    try:
        ranges = [FLOOR_TRIAL_DATE_CELL, FLOOR_TRIAL_START_CELL, FLOOR_TRIAL_END_CELL]
        result = (
            service.spreadsheets()
            .values()
            .batchGet(spreadsheetId=sheet_id, ranges=ranges)
            .execute()
        )
        date_val = result["valueRanges"][0].get("values", [[""]])[0][0]
        start_val = result["valueRanges"][1].get("values", [[""]])[0][0]
        end_val = result["valueRanges"][2].get("values", [[""]])[0][0]

        dt_start = parse_trial_datetime(date_val, start_val)
        dt_end = parse_trial_datetime(date_val, end_val)
        now_utc = datetime.now(timezone.utc)

        log.info("=== UTC Verification — Floor Trial Timing ===")
        log.info(f"Trial Date (D15): {date_val}")
        log.info(f"Start Time (C17): {start_val}")
        log.info(f"End Time (D17):   {end_val}")
        log.info(f"Parsed UTC Start: {dt_start}")
        log.info(f"Parsed UTC End:   {dt_end}")
        log.info(f"Current UTC Now:  {now_utc}")

        if dt_start and dt_end:
            if dt_start <= now_utc <= dt_end:
                log.info("✅ Floor Trial is IN PROGRESS (UTC)")
            elif now_utc < dt_start:
                log.info("⏳ Floor Trial has NOT STARTED yet (UTC)")
            else:
                log.info("🏁 Floor Trial is FINISHED (UTC)")
        else:
            log.warning("⚠️ Could not parse trial date/time — check sheet values")

        update_floor_trial_status(service, sheet_id)
        log.info("✅ UTC Verification complete — proceeding to queue processing")
    except Exception as e:
        log.error(f"Error verifying UTC timing: {e}")


def process_actions_in_memory(
    state: "SpreadsheetState", action_values: "List[List[str]]"
):
    """
    Processes action commands from action_values (corresponding to C6:C11).
    Modifies state.sections["current_queue"] and state.sections["history"] in memory.
    """

    # The current_queue is always 6 rows, 5 columns (E6:I11)
    current_queue = state.sections["current_queue"]["data"]
    history = state.sections["history"]["data"]
    # Normalize current_queue to 6 rows, 5 cols
    while len(current_queue) < 6:
        current_queue.append([""] * 5)
    for idx in range(len(current_queue)):
        row = current_queue[idx]
        if len(row) < 5:
            current_queue[idx] += [""] * (5 - len(row))
        elif len(row) > 5:
            current_queue[idx] = row[:5]
    # Normalize history rows to at least 5 columns (timestamp + 4 data)
    for i in range(len(history)):
        if len(history[i]) < 5:
            history[i] += [""] * (5 - len(history[i]))
        elif len(history[i]) > 5:
            history[i] = history[i][:5]
    # Prepare for deferred "-" rows
    deferred_minus_rows = []
    # Process each action (C6:C11 maps to current_queue rows 0-5)
    for idx, row in enumerate(action_values):
        action = row[0].strip() if row and len(row) > 0 else ""
        action_lc = action.lower()
        if action_lc == "o":
            # Move row to history with timestamp, then clear row in current_queue
            queue_row = current_queue[idx]
            if any(str(cell).strip() for cell in queue_row):
                timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                # Only include timestamp, leader, follower, division columns
                new_history_row = [timestamp, queue_row[1], queue_row[2], queue_row[3]]
                # Pad to 5 columns if needed
                if len(new_history_row) < 5:
                    new_history_row += [""] * (5 - len(new_history_row))
                history.append(new_history_row)
                # --- Increment run count in report section ---
                try:
                    report_data = state.sections["report"]["data"]
                    leader = str(queue_row[1]).strip()
                    follower = str(queue_row[2]).strip()
                    division = str(queue_row[3]).strip()
                    found = False
                    for report_row in report_data:
                        # Defensive: ensure at least 5 columns
                        if len(report_row) < 5:
                            report_row += [""] * (5 - len(report_row))
                        # Report columns: [Leader, Follower, Division, Cue, RunCount]
                        r_leader = str(report_row[0]).strip()
                        r_follower = str(report_row[1]).strip()
                        r_division = str(report_row[2]).strip()
                        if names_match(
                            r_leader, r_follower, r_division, leader, follower, division
                        ):
                            # Column 4 (index 4) is run count
                            try:
                                count = (
                                    int(str(report_row[4]).strip())
                                    if str(report_row[4]).strip()
                                    else 0
                                )
                            except Exception:
                                count = 0
                            report_row[4] = str(count + 1)
                            state.mark_dirty("report")
                            log.info(
                                f"Incremented run count for {leader}/{follower}/{division}"
                            )
                            found = True
                            break
                    # Optionally log if not found in report
                    if not found:
                        log.debug(
                            f"No matching report row found for {leader}/{follower}/{division} to increment run count."
                        )
                    # Sort report data by leader name (column 0)
                    report_data.sort(
                        key=lambda r: str(r[0]).lower() if len(r) > 0 else ""
                    )
                    state.sections["report"]["data"] = report_data
                    state.mark_dirty("report")
                    log.info(
                        "Sorted report data by leader name after run count update."
                    )
                except Exception as e:
                    log.error(
                        f"Error incrementing run count in report: {e}", exc_info=True
                    )
                # --- End increment run count ---
            # Clear the row in current_queue
            current_queue[idx] = [""] * 5
            # Clear the action so it doesn't repeat
            action_values[idx][0] = ""
        elif action_lc == "x":
            # Just clear the row in current_queue
            current_queue[idx] = [""] * 5
            # Clear the action so it doesn't repeat
            action_values[idx][0] = ""
        elif action_lc == "-":
            # Store the row, clear it, append later after compaction
            row_copy = copy.deepcopy(current_queue[idx])
            if any(str(cell).strip() for cell in row_copy):
                deferred_minus_rows.append(row_copy)
            current_queue[idx] = [""] * 5
            # Clear the action so it doesn't repeat
            action_values[idx][0] = ""
        else:
            # Unrecognized or empty: do nothing
            continue
    # Compact current_queue: move all non-empty rows up, empty rows down, preserve order
    non_empty = [r for r in current_queue if any(str(cell).strip() for cell in r)]
    compacted = list(non_empty)
    # After compaction, append deferred "-" rows to end of compacted non-empty rows
    compacted += deferred_minus_rows
    # Truncate to 6 rows, pad with empty if needed
    compacted = compacted[:6]
    while len(compacted) < 6:
        compacted.append([""] * 5)
    # Update current_queue in state
    state.sections["current_queue"]["data"] = compacted
    state.sections["history"]["data"] = history
    # Mark both as dirty
    state.mark_dirty("current_queue")
    state.mark_dirty("history")
    # Push cleared actions back to Google Sheets after processing
    service = sheets.get_sheets_service()
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=MONITOR_RANGE,
        valueInputOption="RAW",
        body={"values": action_values},
    ).execute()
    log.info("process_actions_in_memory: Cleared commands written back to sheet.")
    # Optionally mark actions as cleared or log debug note
    log.debug(
        "process_actions_in_memory: Cleared processed commands from action_values after handling."
    )


# ---------------------------------------------------------------------
# Helper: Parse trial date and time into datetime
# ---------------------------------------------------------------------
def parse_trial_datetime(date_str, time_str):
    """
    Parse a combined date/time or standalone UTC datetime string into a datetime.
    Accepts either:
      - date in M/D/YYYY and time in H:MM AM/PM, or
      - full UTC datetime (YYYY-MM-DD HH:MM[:SS])
    """

    try:
        # Case 1: Combined full UTC datetime
        full_str = (date_str or "") + " " + (time_str or "")
        full_str = full_str.strip()
        if not full_str:
            raise ValueError("No datetime provided")

        # Try known UTC formats
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(full_str, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                continue

        # Case 2: Legacy separate date + time fields
        if date_str and time_str:
            for date_fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                try:
                    date_obj = datetime.strptime(date_str.strip(), date_fmt).date()
                    break
                except Exception:
                    continue
            else:
                raise ValueError(f"Could not parse date '{date_str}'")

            for time_fmt in ("%I:%M %p", "%H:%M", "%H:%M:%S"):
                try:
                    time_obj = datetime.strptime(time_str.strip(), time_fmt).time()
                    break
                except Exception:
                    continue
            else:
                raise ValueError(f"Could not parse time '{time_str}'")

            return datetime.combine(date_obj, time_obj).replace(tzinfo=timezone.utc)

        raise ValueError(f"Unrecognized datetime format: '{full_str}'")

    except Exception as e:
        log.warning(f"Failed to parse trial datetime: '{date_str}' '{time_str}' — {e}")
        return None


# ---------------------------------------------------------------------
# SpreadsheetState: In-memory state manager for all sheet sections
# ---------------------------------------------------------------------
class SpreadsheetState:
    """
    Manages in-memory data storage for all relevant sheet sections.
    Provides methods to load all data from Google Sheets, mark dirty sections,
    sync back, and visualize the current state.
    """

    def __init__(self):
        # Store all section data here
        self.sections = {
            "current_queue": {
                "range": CURRENT_QUEUE_RANGE,
                "data": [],
            },
            "priority_queue": {
                "range": PRIORITY_QUEUE_RANGE,
                "data": [],
            },
            "non_priority_queue": {
                "range": NON_PRIORITY_QUEUE_RANGE,
                "data": [],
            },
            "raw_submissions": {
                "range": RAW_SUBMISSIONS_RANGE,
                "data": [],
            },
            "rejected_submissions": {
                "range": REJECTED_SUBMISSIONS_RANGE,
                "data": [],
            },
            "report": {
                "range": REPORT_RANGE,
                "data": [],
            },
            "history": {
                "range": f"{HISTORY_SHEET_NAME}!A6:E",
                "data": [],
            },
            "priority_division_map": {
                "range": PRIORITY_DIVISION_RANGE,
                "data": [],
            },
            "priority_flag_map": {
                "range": PRIORITY_FLAG_RANGE,
                "data": [],
            },
        }
        # Set of section names with unsynced changes
        self.dirty_sections = set()

    def load_from_sheets(self, service, spreadsheet_id):
        """
        Fetches all configured section data in a single batchGet where possible,
        and stores it in memory.
        """
        ranges = [self.sections[name]["range"] for name in self.sections]
        try:
            result = (
                service.spreadsheets()
                .values()
                .batchGet(spreadsheetId=spreadsheet_id, ranges=ranges)
                .execute()
            )
            value_ranges = result.get("valueRanges", [])
            for idx, name in enumerate(self.sections):
                vals = (
                    value_ranges[idx].get("values", [])
                    if idx < len(value_ranges)
                    else []
                )
                self.sections[name]["data"] = vals
            log.info("SpreadsheetState: Loaded all sections from sheets.")
        except Exception as e:
            log.error(
                f"SpreadsheetState: Error loading from sheets: {e}", exc_info=True
            )

    def sync_to_sheets(self, service, spreadsheet_id):
        """
        Pushes updates for all dirty sections to Google Sheets.
        """
        for name in list(self.dirty_sections):
            section = self.sections.get(name)
            if not section:
                log.warning(
                    f"SpreadsheetState: Unknown section '{name}' - cannot sync."
                )
                continue
            rng = section["range"]
            data = section["data"]
            try:
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=rng,
                    valueInputOption="RAW",
                    body={"values": data},
                ).execute()
                log.info(f"SpreadsheetState: Synced section '{name}' to range '{rng}'.")
                self.dirty_sections.discard(name)
            except Exception as e:
                log.error(
                    f"SpreadsheetState: Error syncing '{name}': {e}", exc_info=True
                )
            # After each section sync, audit queues for ghost gaps
            audit_queues(self)

    def mark_dirty(self, section_name):
        """
        Mark a section as dirty (modified, needs sync).
        """
        if section_name in self.sections:
            self.dirty_sections.add(section_name)
            log.debug(f"SpreadsheetState: Marked section '{section_name}' as dirty.")
        else:
            log.warning(
                f"SpreadsheetState: Tried to mark unknown section '{section_name}' as dirty."
            )

    def visualize(self):
        """
        Prints a well-formatted representation of all in-memory data to the console.
        """
        for name, section in self.sections.items():
            rng = section["range"]
            data = section["data"]
            title = f"=== {name.replace('_', ' ').upper()} ==="
            log.info(title)
            log.info(rng)
            # Print header
            if not data:
                log.info("No data.")
                continue
            # Compute max width for each column
            num_cols = max(len(row) for row in data)
            col_widths = [0] * num_cols
            for row in data:
                for i in range(num_cols):
                    cell = str(row[i]) if i < len(row) else ""
                    col_widths[i] = max(col_widths[i], len(cell))
            # Print rows with aligned columns
            header_row = "Row | " + " | ".join(f"C{i+1}" for i in range(num_cols))
            sep_row = "----|" + "|".join(
                "-" * (col_widths[i] + 2) for i in range(num_cols)
            )
            log.info(header_row)
            log.info(sep_row)
            for idx, row in enumerate(data):
                rownum = idx + 1
                row_cells = []
                for i in range(num_cols):
                    cell = str(row[i]) if i < len(row) else ""
                    row_cells.append(cell.ljust(col_widths[i]))
                log.info(f"{rownum:<4}| " + " | ".join(row_cells))
            log.info("")  # Blank line between sections


def import_external_submissions(
    service, state: SpreadsheetState, source_spreadsheet_id: str
):
    """
    Imports new submissions from external spreadsheet into in-memory state.
    - Reads from external Form Responses sheet.
    - Maps A, C, D, E, F, G → RawSubmissions (B–G equivalent in memory).
    - Normalizes HasStartingCue field to 'Yes'/'No'.
    - Marks processed rows (H = X) in source.
    - Updates state.sections['raw_submissions']['data'] and marks dirty.
    """
    log.info(
        "import_external_submissions: Starting import from external source (in-memory mode)."
    )

    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=source_spreadsheet_id, range=EXTERNAL_SOURCE_RANGE)
            .execute()
        )
        values = result.get("values", [])
        if not values:
            log.info("No data found in external source.")
            return

        new_rows = []
        rows_to_mark = []

        for idx, row in enumerate(values):
            row_padded = row + [""] * (8 - len(row)) if len(row) < 8 else row[:8]
            if str(row_padded[7]).strip().lower() == "x":
                continue

            # --- Patch: handle blank timestamp ---
            timestamp_val = (
                row_padded[0].strip() if len(row_padded) > 0 and row_padded[0] else ""
            )
            if not timestamp_val:
                timestamp_val = datetime.now().strftime("%m/%d/%Y %H:%M:%S")

            has_starting_cue = (
                "Yes" if "specific cue" in str(row_padded[5]).lower() else "No"
            )

            mapped_row = [
                timestamp_val,  # Timestamp (or current time if blank)
                row_padded[2],  # Leader
                row_padded[3],  # Follower
                row_padded[4],  # Division
                has_starting_cue,
                row_padded[6],  # Cue description
            ]

            new_rows.append(mapped_row)
            rows_to_mark.append(idx)

        if not new_rows:
            log.info("No new unprocessed rows to import.")
            return

        # Append to in-memory RawSubmissions data
        raw_data = state.sections["raw_submissions"]["data"]
        raw_data.extend(new_rows)
        state.sections["raw_submissions"]["data"] = raw_data
        state.mark_dirty("raw_submissions")

        log.info(f"Added {len(new_rows)} rows to in-memory RawSubmissions data.")

        # Mark processed rows with 'X' in the external source
        updates = [
            [
                (
                    "X"
                    if idx in rows_to_mark
                    else (values[idx][7] if len(values[idx]) > 7 else "")
                )
            ]
            for idx in range(len(values))
        ]
        update_range = f"Form Responses 1!H2:H{len(values)+1}"

        service.spreadsheets().values().update(
            spreadsheetId=source_spreadsheet_id,
            range=update_range,
            valueInputOption="RAW",
            body={"values": updates},
        ).execute()

        log.info(f"Marked {len(rows_to_mark)} external rows as processed.")
        log.debug(f"Sample imported rows: {new_rows[:3]}")

    except Exception as e:
        log.error(f"import_external_submissions: Error occurred: {e}", exc_info=True)


def retry_on_exception(fn, *args, **kwargs):
    # simple version
    return fn(*args, **kwargs)


def format_duration(seconds: float) -> str:
    return f"{seconds:.1f}s"


# ---------------------------------------------------------------------
# Core watcher logic
# ---------------------------------------------------------------------
def fetch_sheet_values(
    service, spreadsheet_id: str, range_name: str
) -> List[List[Any]]:
    """Fetch values from a given range_name (e.g., 'Current!C6:C11')."""
    log.debug(f"Fetching sheet values for range: {range_name}")
    sheet = service.spreadsheets()
    result = (
        sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    )
    values = result.get("values", [])
    log.debug(f"Fetched {len(values)} rows from range {range_name}")
    return values


def detect_changes(
    previous: Optional[List[List[Any]]], current: List[List[Any]]
) -> bool:
    log.debug(
        f"Detecting changes: previous length = {len(previous) if previous else 'None'}, current length = {len(current)}"
    )
    # Example simple logic; can be improved.
    if previous is None:
        log.debug("No previous data, no changes detected.")
        return False
    # You can integrate deeper util from common-utils here (e.g., diff util)
    changed = previous != current
    log.debug(f"Change detected: {changed}")
    return changed


def process_actions(
    service, spreadsheet_id: str, monitor_range: str, current_values: List[List[Any]]
):
    log.info("Detected change — starting processing ...")
    # monitor_range like "Current!C6:C11"
    # current_values: list of lists, each is a row (1 cell per row, since C6:C11)
    try:
        # Parse monitor_range for sheet and row info
        # e.g., "Current!C6:C11"

        m = re.match(r"([^!]+)!(\w+)(\d+):\w+(\d+)", monitor_range)
        if not m:
            log.error(f"Unable to parse monitor_range: {monitor_range}")
            return
        sheet_name, col, start_row, end_row = (
            m.group(1),
            m.group(2),
            int(m.group(3)),
            int(m.group(4)),
        )
        log.debug(
            f"Processing changes on sheet '{sheet_name}', rows {start_row} to {end_row}, column {col}"
        )

        # Define allowed modification ranges for "Current" sheet
        allowed_monitor_col = "C"
        allowed_monitor_rows = range(6, 12)  # 6 to 11 inclusive
        allowed_data_cols = ["E", "F", "G", "H", "I"]
        allowed_data_rows = range(5, 13)  # 5 to 12 inclusive

        def can_modify_range(range_str: str) -> bool:
            # Parse range like "Current!F7:I7" or "Current!C6"
            m_range = re.match(r"([^!]+)!(\w)(\d+):?(\w)?(\d+)?", range_str)
            if not m_range:
                return False
            sheet = m_range.group(1)
            if sheet != "Current":
                # No restriction on other sheets
                return True
            start_col = m_range.group(2)
            start_row_ = int(m_range.group(3))
            end_col = m_range.group(4) if m_range.group(4) else start_col
            end_row_ = int(m_range.group(5)) if m_range.group(5) else start_row_

            # Convert columns to ordinals for range check
            start_col_ord = ord(start_col.upper())
            end_col_ord = ord(end_col.upper())

            for col_ord in range(start_col_ord, end_col_ord + 1):
                col_letter = chr(col_ord)
                for row in range(start_row_, end_row_ + 1):
                    if not (
                        (
                            col_letter == allowed_monitor_col
                            and row in allowed_monitor_rows
                        )
                        or (
                            col_letter in allowed_data_cols and row in allowed_data_rows
                        )
                    ):
                        return False
            return True

        # Ensure History sheet exists before writing
        sheets.ensure_sheet_exists(service, spreadsheet_id, HISTORY_SHEET_NAME)

        deferred_minus_rows = []  # for storing rows to append after compaction
        deferred_minus_count = 0
        for idx, valrow in enumerate(current_values):
            val = valrow[0] if valrow else ""
            val_lc = str(val).strip().lower()
            row_num = start_row + idx
            if val_lc == "o":
                log.info(f"Action 'O' detected at row {row_num}")
                fg_range = f"{sheet_name}!F{row_num}:I{row_num}"
                fg_values = fetch_sheet_values(service, spreadsheet_id, fg_range)
                fg_values = (
                    fg_values[0] if fg_values and len(fg_values) > 0 else [""] * 4
                )
                timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
                log.info(
                    f"Inserting data into '{HISTORY_SHEET_NAME}' sheet at next open row: timestamp={timestamp}, values={fg_values}"
                )
                # Fetch current rows in History sheet to find next open row starting at row 6
                history_range = f"{HISTORY_SHEET_NAME}!A6:E"
                history_values = fetch_sheet_values(
                    service, spreadsheet_id, history_range
                )
                append_row = 6 + len(history_values) if history_values else 6
                new_row = [timestamp] + fg_values
                log.debug(
                    f"Appending to {HISTORY_SHEET_NAME} at row {append_row}: {new_row}"
                )
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"{HISTORY_SHEET_NAME}!A{append_row}:E{append_row}",
                    valueInputOption="RAW",
                    body={"values": [new_row]},
                ).execute()
                clear_fi_range = f"{sheet_name}!F{row_num}:I{row_num}"
                if can_modify_range(clear_fi_range):
                    log.info(
                        f"Clearing range {clear_fi_range} after processing 'O' action at row {row_num}"
                    )
                    service.spreadsheets().values().clear(
                        spreadsheetId=spreadsheet_id, range=clear_fi_range, body={}
                    ).execute()
                else:
                    log.warning(
                        f"Attempted to clear out-of-bounds range {clear_fi_range}, skipping."
                    )
                # Always clear the triggering monitored cell (column C)
                clear_c_range = f"{sheet_name}!{col}{row_num}"
                if can_modify_range(clear_c_range):
                    log.info(
                        f"Clearing monitored cell {clear_c_range} after processing 'O' action at row {row_num}"
                    )
                    service.spreadsheets().values().clear(
                        spreadsheetId=spreadsheet_id, range=clear_c_range, body={}
                    ).execute()
                else:
                    log.warning(
                        f"Attempted to clear out-of-bounds range {clear_c_range}, skipping."
                    )
            elif val_lc == "x":
                log.info(
                    f"Action 'X' detected at row {row_num} — clearing only the data in that row."
                )
                clear_fi_range = f"{sheet_name}!F{row_num}:I{row_num}"
                if can_modify_range(clear_fi_range):
                    log.info(
                        f"Clearing data in range {clear_fi_range} for 'X' action at row {row_num}"
                    )
                    service.spreadsheets().values().clear(
                        spreadsheetId=spreadsheet_id, range=clear_fi_range, body={}
                    ).execute()
                else:
                    log.warning(
                        f"Attempted to clear out-of-bounds range {clear_fi_range}, skipping."
                    )
                # Always clear the triggering monitored cell (column C)
                clear_c_range = f"{sheet_name}!{col}{row_num}"
                if can_modify_range(clear_c_range):
                    log.info(
                        f"Clearing monitored cell {clear_c_range} for 'X' action at row {row_num}"
                    )
                    service.spreadsheets().values().clear(
                        spreadsheetId=spreadsheet_id, range=clear_c_range, body={}
                    ).execute()
                else:
                    log.warning(
                        f"Attempted to clear out-of-bounds range {clear_c_range}, skipping."
                    )
            elif val_lc == "-":
                # Instead of moving to row 12 immediately, defer appending
                log.info(
                    f"Deferring move of E:I data from row {row_num} for later append after compaction"
                )
                ei_range = f"{sheet_name}!E{row_num}:I{row_num}"
                ei_values = fetch_sheet_values(service, spreadsheet_id, ei_range)
                ei_values = (
                    ei_values[0] if ei_values and len(ei_values) > 0 else [""] * 5
                )
                # Store for later appending after compaction
                deferred_minus_rows.append(ei_values)
                deferred_minus_count += 1
                # Clear original E:I and C cell
                if can_modify_range(ei_range):
                    log.debug(f"Clearing E:I in original row {row_num}")
                    service.spreadsheets().values().clear(
                        spreadsheetId=spreadsheet_id, range=ei_range, body={}
                    ).execute()
                else:
                    log.warning(
                        f"Attempted to clear out-of-bounds range {ei_range}, skipping."
                    )
                clear_c_range = f"{sheet_name}!{col}{row_num}"
                if can_modify_range(clear_c_range):
                    log.info(
                        f"Clearing monitored cell {clear_c_range} for '-' action at row {row_num}"
                    )
                    service.spreadsheets().values().clear(
                        spreadsheetId=spreadsheet_id, range=clear_c_range, body={}
                    ).execute()
                else:
                    log.warning(
                        f"Attempted to clear out-of-bounds range {clear_c_range}, skipping."
                    )
            elif str(val).strip() != "":
                # Unrecognized but non-empty value: just clear the triggering cell (column C)
                clear_c_range = f"{sheet_name}!{col}{row_num}"
                if can_modify_range(clear_c_range):
                    log.info(
                        f"Clearing monitored cell {clear_c_range} for unrecognized value '{val}' at row {row_num}"
                    )
                    service.spreadsheets().values().clear(
                        spreadsheetId=spreadsheet_id, range=clear_c_range, body={}
                    ).execute()
                else:
                    log.warning(
                        f"Attempted to clear out-of-bounds range {clear_c_range}, skipping."
                    )
            else:
                # Empty cell: nothing to do
                continue

        # After all actions, compact E5:I12 upward (preserving order)
        compaction_range = COMPACTION_RANGE
        compaction_data = fetch_sheet_values(service, spreadsheet_id, compaction_range)
        # Ensure all rows have length 5 (E:I)
        compaction_data = [
            row + [""] * (5 - len(row)) if len(row) < 5 else row[:5]
            for row in compaction_data
        ]
        # Identify non-empty rows (at least one non-empty cell)
        non_empty_rows = [
            row for row in compaction_data if any(str(cell).strip() for cell in row)
        ]
        empty_rows = [
            row for row in compaction_data if not any(str(cell).strip() for cell in row)
        ]
        log.info(
            f"Compaction: found {len(non_empty_rows)} non-empty rows and {len(empty_rows)} empty rows in E5:I12"
        )
        # Pad with empty rows so total rows matches original
        total_rows = len(compaction_data)
        compacted = list(non_empty_rows)  # preserve order
        while len(compacted) < total_rows:
            compacted.append([""] * 5)
        # Only write if allowed by can_modify_range
        if can_modify_range(compaction_range):
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=compaction_range,
                valueInputOption="RAW",
                body={"values": compacted},
            ).execute()
            log.info(f"Compaction complete: {compaction_range} compacted upward.")
        else:
            log.warning(
                f"Attempted to update out-of-bounds range {compaction_range}, skipping compaction."
            )

        # After compaction, handle deferred '-' rows by reinserting them in memory and updating the whole range at once
        if deferred_minus_rows:
            # Fetch compacted data and normalize
            compacted_data = fetch_sheet_values(
                service, spreadsheet_id, compaction_range
            )
            compacted_data = [
                row + [""] * (5 - len(row)) if len(row) < 5 else row[:5]
                for row in compacted_data
            ]
            # Filter out empty deferred rows
            deferred_minus_rows = [
                r for r in deferred_minus_rows if any(str(c).strip() for c in r)
            ]
            if not deferred_minus_rows:
                log.warning(
                    "All deferred '-' rows were empty; nothing to reinsert after compaction."
                )
            else:
                # Combine non-empty rows from compacted_data with deferred_minus_rows
                non_empty_compacted = [
                    row
                    for row in compacted_data
                    if any(str(cell).strip() for cell in row)
                ]
                combined_rows = non_empty_compacted + deferred_minus_rows
                # Truncate to max 8 rows (E5:I12)
                combined_rows = combined_rows[:8]
                # Pad if fewer than 8 rows
                while len(combined_rows) < 8:
                    combined_rows.append([""] * 5)
                # Write back the entire combined range at once
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=compaction_range,
                    valueInputOption="RAW",
                    body={"values": combined_rows},
                ).execute()
                log.info(
                    f"Deferred '-' actions: {len(deferred_minus_rows)} row(s) successfully reinserted after compaction with single update."
                )
                log.info("Compaction and reinsertion of deferred '-' rows complete.")
        else:
            log.info("No '-' actions to append after compaction.")
    except Exception as e:
        log.error(f"Error in process_changes: {e}", exc_info=True)
    log.info("Processing complete.")


# ---------------------------------------------------------------------
# New queue processing functions
# ---------------------------------------------------------------------
def process_priority(service, spreadsheet_id, state):
    """
    Read from in-memory Priority queue, find first non-empty row, log and clear, return as 5-cell list or None.
    """
    section_name = "priority_queue"
    values = state.sections[section_name]["data"]
    taken_row = None
    # Pad all rows to 5 cells
    values = [
        row + [""] * (5 - len(row)) if len(row) < 5 else row[:5] for row in values
    ]
    for idx, row in enumerate(values):
        if any(str(cell).strip() for cell in row):
            log.info(f"process_priority: Taking row {idx+3} from Priority queue: {row}")
            taken_row = row
            # Clear the row after taking (set to empty)
            values[idx] = [""] * 5
            break
    if taken_row is not None:
        # Clean and compact the queue after clearing the row
        compacted = clean_and_compact_queue(values, section_name)
        state.sections[section_name]["data"] = compacted
        state.mark_dirty(section_name)
        return taken_row
    log.info("process_priority: No non-empty rows found in Priority queue.")
    return None


def process_non_priority(service, spreadsheet_id, state):
    """
    Read from in-memory NonPriority queue, find first non-empty row, log and clear, return as 5-cell list or None.
    """
    section_name = "non_priority_queue"
    values = state.sections[section_name]["data"]
    taken_row = None
    # Pad all rows to 5 cells
    values = [
        row + [""] * (5 - len(row)) if len(row) < 5 else row[:5] for row in values
    ]
    for idx, row in enumerate(values):
        if any(str(cell).strip() for cell in row):
            log.info(
                f"process_non_priority: Taking row {idx+3} from NonPriority queue: {row}"
            )
            taken_row = row
            # Clear the row after taking (set to empty)
            values[idx] = [""] * 5
            break
    if taken_row is not None:
        # Clean and compact the queue after clearing the row
        compacted = clean_and_compact_queue(values, section_name)
        state.sections[section_name]["data"] = compacted
        state.mark_dirty(section_name)
        return taken_row
    log.info("process_non_priority: No non-empty rows found in NonPriority queue.")
    return None


def fill_current_from_queues(service, spreadsheet_id, state):
    """
    Fill empty rows in the in-memory Current queue from Priority and NonPriority queues using only SpreadsheetState.
    For each empty row in the current queue:
      - Try to get a row from the in-memory priority queue.
      - If that returns nothing, try the in-memory non-priority queue.
      - If a row is returned, insert it into the first available empty slot in the current_queue memory structure.
      - Mark current_queue, priority_queue, or non_priority_queue as dirty if modified.
      - Log the source queue and compaction status after each operation.
    Returns True if any rows were filled, False otherwise.
    """
    current_section = state.sections["current_queue"]
    current_data = current_section["data"]
    # Always 6 rows, 5 cols (E–I, no placeholder)
    while len(current_data) < 6:
        current_data.append([""] * 5)
    for idx in range(len(current_data)):
        if len(current_data[idx]) < 5:
            current_data[idx] += [""] * (5 - len(current_data[idx]))
        elif len(current_data[idx]) > 5:
            current_data[idx] = current_data[idx][:5]

    # Helper: filled row is all 5 columns non-empty (or any non-empty cell in all 5 columns)
    def is_filled(row):
        # Consider filled if any of the 5 columns have data
        return any(str(cell).strip() for cell in row[:5])

    changes_made = False
    for idx, row in enumerate(current_data):
        row_num = 6 + idx
        if is_filled(row):
            continue
        # Normalize priority_queue rows to 5 columns
        pq = state.sections["priority_queue"]["data"]
        pq_rows = [r + [""] * (5 - len(r)) if len(r) < 5 else r[:5] for r in pq]
        taken_priority = None
        for pq_idx, pq_row in enumerate(pq_rows):
            if any(str(cell).strip() for cell in pq_row):
                taken_priority = pq_row
                pq_rows[pq_idx] = [""] * 5
                log.info(
                    f"fill_current_from_queues: Taking row {pq_idx+3} from Priority queue: {taken_priority}"
                )
                break
        if taken_priority is not None:
            # Compact Priority queue in memory
            non_empty = [r for r in pq_rows if any(str(cell).strip() for cell in r)]
            empty = [r for r in pq_rows if not any(str(cell).strip() for cell in r)]
            total = len(pq_rows)
            compacted = list(non_empty)
            while len(compacted) < total:
                compacted.append([""] * 5)
            state.sections["priority_queue"]["data"] = compacted
            state.mark_dirty("priority_queue")
            log.info(
                f"fill_current_from_queues: Compacted Priority queue — {len(non_empty)} non-empty, {len(empty)} empty rows"
            )
            # Insert taken row directly (5 columns, E–I)
            current_data[idx] = taken_priority
            # Pad to 5 columns in case
            if len(current_data[idx]) < 5:
                current_data[idx] += [""] * (5 - len(current_data[idx]))
            elif len(current_data[idx]) > 5:
                current_data[idx] = current_data[idx][:5]
            state.mark_dirty("current_queue")
            log.info(
                f"fill_current_from_queues: Filled Current queue row {row_num} (cols E–I) from Priority queue: {taken_priority}"
            )
            changes_made = True
            continue
        # If not Priority, try NonPriority, but only for rows 6–9 (row_num 6,7,8,9)
        if row_num > 9:
            log.info(
                f"fill_current_from_queues: Skipping NonPriority for row {row_num} (bottom two slots must be Priority only)."
            )
            log.debug(
                f"fill_current_from_queues: Only Priority queue may fill Current rows 10 and 11 (row {row_num})."
            )
            log.info(
                f"fill_current_from_queues: No data available to fill row {row_num}."
            )
            continue
        npq = state.sections["non_priority_queue"]["data"]
        npq_rows = [r + [""] * (5 - len(r)) if len(r) < 5 else r[:5] for r in npq]
        taken_nonpriority = None
        for npq_idx, npq_row in enumerate(npq_rows):
            if any(str(cell).strip() for cell in npq_row):
                taken_nonpriority = npq_row
                npq_rows[npq_idx] = [""] * 5
                log.info(
                    f"fill_current_from_queues: Taking row {npq_idx+3} from NonPriority queue: {taken_nonpriority}"
                )
                break
        if taken_nonpriority is not None:
            # Compact NonPriority queue in memory
            non_empty = [r for r in npq_rows if any(str(cell).strip() for cell in r)]
            empty = [r for r in npq_rows if not any(str(cell).strip() for cell in r)]
            total = len(npq_rows)
            compacted = list(non_empty)
            while len(compacted) < total:
                compacted.append([""] * 5)
            state.sections["non_priority_queue"]["data"] = compacted
            state.mark_dirty("non_priority_queue")
            log.info(
                f"fill_current_from_queues: Compacted NonPriority queue — {len(non_empty)} non-empty, {len(empty)} empty rows"
            )
            # Insert taken row directly (5 columns, E–I)
            current_data[idx] = taken_nonpriority
            if len(current_data[idx]) < 5:
                current_data[idx] += [""] * (5 - len(current_data[idx]))
            elif len(current_data[idx]) > 5:
                current_data[idx] = current_data[idx][:5]
            state.mark_dirty("current_queue")
            log.info(
                f"fill_current_from_queues: Filled Current queue row {row_num} (cols E–I) from NonPriority queue: {taken_nonpriority}"
            )
            changes_made = True
            continue
        log.info(f"fill_current_from_queues: No data available to fill row {row_num}.")
    if not changes_made:
        log.info("fill_current_from_queues: No empty rows filled.")
    else:
        log.info(
            "fill_current_from_queues: Current queue now starts at column F (E is first column of range); all rows are 5 columns: [E, F, G, H, I]."
        )
    return changes_made


# ---------------------------------------------------------------------
# RawSubmissions processing function
# ---------------------------------------------------------------------
def process_raw_submissions_in_memory(state: SpreadsheetState):
    """
    Process raw submissions entirely in memory.
    Moves rows from 'raw_submissions' into 'priority_queue' or 'non_priority_queue'
    depending on division mappings. Updates 'report' and 'rejected_submissions'
    sections and marks dirty sections for sync.
    """
    log.info("process_raw_submissions_in_memory: Starting processing in-memory")

    # Configurable max number of Priority runs per couple
    MAX_PRIORITY_RUNS = 3

    service = sheets.get_sheets_service()

    # --- Retrieve new UTC Floor Trial times from Current sheet ---
    def get_value(service, spreadsheet_id, range_):
        try:
            rows = fetch_sheet_values(service, spreadsheet_id, range_)
            return rows[0][0] if rows and rows[0] else ""
        except Exception as e:
            log.warning(f"Error getting value from {range_}: {e}")
            return ""

    open_time = get_value(service, SHEET_ID, "Current!B15")
    start_time = get_value(service, SHEET_ID, "Current!B16")
    end_time = get_value(service, SHEET_ID, "Current!B17")

    def parse_utc_string(s):
        if not s:
            return None
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                continue
        return None

    trial_open = parse_utc_string(open_time)
    trial_start = parse_utc_string(start_time)
    trial_end = parse_utc_string(end_time)

    raw_data = state.sections["raw_submissions"]["data"]
    # Build priority_names and all_known_divs from the row-by-row mapping of division and flag
    priority_names = []
    all_known_divs = []
    div_rows = state.sections["priority_division_map"]["data"]
    flag_rows = state.sections["priority_flag_map"]["data"]
    for div_row, flag_row in zip(div_rows, flag_rows):
        division = div_row[0].strip() if div_row and len(div_row) > 0 else ""
        flag = flag_row[0].strip() if flag_row and len(flag_row) > 0 else ""
        if not division:
            continue
        all_known_divs.append(division)
        if flag.lower() == "x":
            priority_names.append(division)

    priority_data = state.sections["priority_queue"]["data"]
    nonpriority_data = state.sections["non_priority_queue"]["data"]
    report_data = state.sections["report"]["data"]
    rejected_data = state.sections["rejected_submissions"]["data"]

    # Normalize all rows to 4–6 columns
    raw_data = [r + [""] * (6 - len(r)) if len(r) < 6 else r[:6] for r in raw_data]

    processed = 0
    for row in list(raw_data):  # iterate over a copy
        if not any(str(c).strip() for c in row):
            continue

        timestamp, leader, follower, division, has_cue, cue_desc = row
        division = str(division).strip()

        # --- Parse submission timestamp and check if within window ---
        try:
            submission_time = datetime.strptime(
                str(timestamp).strip(), "%m/%d/%Y %H:%M:%S"
            ).replace(tzinfo=timezone.utc)
        except Exception as e:
            log.warning(f"Could not parse submission time '{timestamp}': {e}")
            continue
        if (
            trial_open
            and trial_end
            and (submission_time < trial_open or submission_time > trial_end)
        ):
            reason = "Outside accepted submission window"
            rejected_data.append(row + [reason])
            continue

        # Duplicate check (existing queues)
        found_in_priority = False
        found_in_nonpriority = False
        for r in priority_data:
            if any(str(x).strip() for x in r):
                r_leader = str(r[1]).strip() if len(r) > 1 else ""
                r_follower = str(r[2]).strip() if len(r) > 2 else ""
                r_division = str(r[3]).strip() if len(r) > 3 else ""
                if names_match(
                    r_leader, r_follower, r_division, leader, follower, division
                ):
                    found_in_priority = True
                    break
        if not found_in_priority:
            for r in nonpriority_data:
                if any(str(x).strip() for x in r):
                    r_leader = str(r[1]).strip() if len(r) > 1 else ""
                    r_follower = str(r[2]).strip() if len(r) > 2 else ""
                    r_division = str(r[3]).strip() if len(r) > 3 else ""
                    if names_match(
                        r_leader, r_follower, r_division, leader, follower, division
                    ):
                        found_in_nonpriority = True
                        break
        if found_in_priority or found_in_nonpriority:
            log.info(f"Duplicate found for {leader} / {follower} / {division}")
            if found_in_priority:
                rejected_data.append(row + ["Duplicate entry in Priority queue"])
            elif found_in_nonpriority:
                rejected_data.append(row + ["Duplicate entry in NonPriority queue"])
            else:
                rejected_data.append(row + ["Duplicate entry in queue"])
            continue

        # Also check in current_queue (prevent duplicate in current queue)
        current_queue = state.sections["current_queue"]["data"]
        found_in_current = False
        for r in current_queue:
            if any(str(x).strip() for x in r):
                r_leader = str(r[1]).strip() if len(r) > 1 else ""
                r_follower = str(r[2]).strip() if len(r) > 2 else ""
                r_division = str(r[3]).strip() if len(r) > 3 else ""
                if names_match(
                    r_leader, r_follower, r_division, leader, follower, division
                ):
                    found_in_current = True
                    break
        if found_in_current:
            log.info(
                f"Duplicate found for {leader} / {follower} / {division} in Current queue"
            )
            rejected_data.append(row + ["Duplicate entry in Current queue"])
            continue

        # Determine destination
        is_priority = False
        for pname in priority_names:
            if pname and (division.startswith(pname) or pname.startswith(division)):
                is_priority = True
                break

        # If is_priority, check run count in report_data and override if at limit
        if is_priority:
            # Find matching row in report_data
            matching_report_row = None
            for report_row in report_data:
                if len(report_row) < 3:
                    continue
                r_leader = str(report_row[0]).strip()
                r_follower = str(report_row[1]).strip()
                r_division = str(report_row[2]).strip()
                if names_match(
                    r_leader, r_follower, r_division, leader, follower, division
                ):
                    matching_report_row = report_row
                    break
            if matching_report_row is not None:
                # Column 4 (index 4) is run count
                try:
                    run_count = (
                        int(str(matching_report_row[4]).strip())
                        if len(matching_report_row) > 4
                        and str(matching_report_row[4]).strip()
                        else 0
                    )
                except Exception:
                    run_count = 0
                if run_count >= MAX_PRIORITY_RUNS:
                    is_priority = False
                    log.info(
                        f"Moved {leader}/{follower}/{division} to NonPriority due to run count limit (>= {MAX_PRIORITY_RUNS})."
                    )

        if is_priority:
            target_queue = priority_data
            dest = "Priority"
        else:
            target_queue = nonpriority_data
            dest = "NonPriority"

        # --- Clean and compact target queue before appending ---
        compacted_before = clean_and_compact_queue(target_queue, f"{dest}_queue")
        target_queue[:] = compacted_before
        # Add as 5 columns: ["", leader, follower, division, cue_desc]
        target_queue.append(["", leader, follower, division, cue_desc])
        # Pad the queue to a consistent minimum length, e.g. 20 rows
        while len(target_queue) < 20:
            target_queue.append([""] * 5)
        # --- Clean and compact again after appending ---
        compacted_after = clean_and_compact_queue(target_queue, f"{dest}_queue")
        target_queue[:] = compacted_after

        # Before appending to report, check for duplicate in report_data
        found_in_report = False
        for r in report_data:
            if len(r) < 3:
                continue
            r_leader = str(r[0]).strip()
            r_follower = str(r[1]).strip()
            r_division = str(r[2]).strip()
            if names_match(
                r_leader, r_follower, r_division, leader, follower, division
            ):
                found_in_report = True
                break
        if found_in_report:
            log.info(f"Skipping duplicate in report for {leader}/{follower}/{division}")
        else:
            # Append to report as [leader, follower, division, cue_desc, 0]
            report_data.append([leader, follower, division, cue_desc, 0])
        processed += 1
        log.info(f"Moved row to {dest}: {row}")

    # Sort report by leader name (column 0)
    report_data.sort(key=lambda r: str(r[0]).lower() if r and len(r) > 0 else "")

    # Clear processed rows
    state.sections["raw_submissions"]["data"] = []
    # Clean and compact queues before updating state (final time)
    state.sections["priority_queue"]["data"] = clean_and_compact_queue(
        priority_data, "priority_queue"
    )
    state.sections["non_priority_queue"]["data"] = clean_and_compact_queue(
        nonpriority_data, "non_priority_queue"
    )
    state.sections["report"]["data"] = report_data
    state.sections["rejected_submissions"]["data"] = rejected_data

    # Mark all affected sections dirty
    for sec in [
        "raw_submissions",
        "priority_queue",
        "non_priority_queue",
        "report",
        "rejected_submissions",
    ]:
        state.mark_dirty(sec)

    log.info(f"process_raw_submissions_in_memory: Processed {processed} rows in-memory")


def update_floor_trial_status(service, spreadsheet_id):
    """
    Update Floor Trial status (Current!B19:D19) using UTC datetimes in:
      - Current!B15 (Open UTC)
      - Current!B16 (Start UTC)
      - Current!B17 (End UTC)
    """
    try:
        ranges = ["Current!B15", "Current!B16", "Current!B17"]
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

        def parse_utc(s):

            if not s:
                return None
            for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
                except Exception:
                    continue
            return None

        dt_open = parse_utc(open_str)
        dt_start = parse_utc(start_str)
        dt_end = parse_utc(end_str)
        now_utc = datetime.now(timezone.utc)

        log.info(
            f"update_floor_trial_status: Open={dt_open}, Start={dt_start}, End={dt_end}, Now={now_utc}"
        )

        status = "Floor Trial Not Active"
        if dt_start and dt_end:
            if dt_start <= now_utc <= dt_end:
                status = "Floor Trial In Progress"
            # elif now_utc < dt_start:
            #    status = "Floor Trials Open for Submissions"
            # elif now_utc > dt_end:
            #    status = "Floor Trials Closed"

        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range="Current!B19:D19",
            valueInputOption="RAW",
            body={"values": [[status, "", ""]]},
        ).execute()
        log.info(f"update_floor_trial_status: Updated status to '{status}'")
        return "In Progress" in status
    except Exception as e:
        log.error(f"update_floor_trial_status: Exception occurred: {e}", exc_info=True)
        return False


def isRunning(service, spreadsheet_id: str) -> bool:
    # Check automation control cell value before running watcher loop
    try:
        h2_value_rows = fetch_sheet_values(
            service, spreadsheet_id, AUTOMATION_CONTROL_CELL
        )
        h2_val = h2_value_rows[0][0] if h2_value_rows and h2_value_rows[0] else ""
        log.info(f"Fetched {AUTOMATION_CONTROL_CELL} value: '{h2_val}'")
        if str(h2_val).strip().lower() != "runautomations":
            log.warning(
                f"Automation stopped: {AUTOMATION_CONTROL_CELL} is not RunAutomations (value was '{h2_val}')"
            )
            return False
        else:
            log.info(
                f"{AUTOMATION_CONTROL_CELL} is 'RunAutomations' (case-insensitive) — proceeding with watcher."
            )
        return True
    except Exception as e:
        log.error(f"Error fetching {AUTOMATION_CONTROL_CELL} value: {e}", exc_info=True)
        log.warning("Automation stopped: Could not fetch automation control value.")
        return False


# ---------------------------------------------------------------------
# New UTC-based run_watcher implementation
# ---------------------------------------------------------------------
def run_watcher(
    spreadsheet_id: str,
    sheet_range: str,
    interval_seconds: int,
    duration_minutes: int,
    monitor_range: str,
):
    """
    Main watcher loop for Floor Trials, using UTC time for all polling and timing.
    - Watches the sheet for a fixed duration (duration_minutes) from UTC now.
    - Polls at interval_seconds using UTC time.
    - Syncs in-memory state to Google Sheets every SYNC_INTERVAL_SECONDS.
    - Updates Current!D2 with UTC timestamp after each sync.
    - Handles actions, imports, and queue population in UTC.
    """
    log.info("Watcher starting (UTC-based)")
    service = sheets.get_sheets_service()

    if not isRunning(service, spreadsheet_id):
        return

    # Initialize SpreadsheetState and load from Sheets
    state = SpreadsheetState()
    state.load_from_sheets(service, spreadsheet_id)
    state.visualize()

    utc_end_time = datetime.now(timezone.utc) + timedelta(minutes=duration_minutes)
    log.info(
        f"Watcher starting: spreadsheet_id={spreadsheet_id}, range={sheet_range}, "
        f"interval={interval_seconds}s, duration={duration_minutes}min (UTC), monitor_range={monitor_range}"
    )
    iteration = 0
    last_sync_time = time.time()
    ACTION_RANGE = MONITOR_RANGE
    while datetime.now(timezone.utc) < utc_end_time:
        iteration += 1
        log.debug(f"Poll iteration {iteration} start (UTC)")
        poll_start = time.time()
        try:
            # Update Floor Trial status before processing submissions/actions
            in_progress = update_floor_trial_status(service, spreadsheet_id)
            # Import external submissions before processing raw submissions
            import_external_submissions(service, state, EXTERNAL_SOURCE_SHEET_ID)
            # --- Process actions in memory before raw submissions ---
            action_values = fetch_sheet_values(service, spreadsheet_id, ACTION_RANGE)
            process_actions_in_memory(state, action_values)
            # --- End new logic ---
            process_raw_submissions_in_memory(state)
            current_values = fetch_sheet_values(service, spreadsheet_id, monitor_range)
            # Instead of change detection, process if any monitored cell is non-empty
            any_nonempty = any(
                (row and str(row[0]).strip() != "") for row in current_values
            )
            if any_nonempty:
                log.debug(
                    "Detected at least one non-empty monitored cell; processing changes."
                )
                # Optionally: process_actions(service, spreadsheet_id, monitor_range, current_values)
            else:
                log.debug("No non-empty values in monitored cells this poll iteration.")
            # Fill empty rows in Current!E6:I11 from queues after processing actions
            if in_progress:
                fill_current_from_queues(service, spreadsheet_id, state)
            else:
                log.info(
                    "⏸️ Floor Trial not in progress — skipping current queue population"
                )
        except Exception as e:
            log.error(f"Error during polling: {e}", exc_info=True)
        elapsed = time.time() - poll_start
        sleep_time = max(0, interval_seconds - elapsed)
        state.visualize()
        # Heartbeat: write current UTC time to Current!D2 every loop for accuracy
        try:
            utc_now_str_iter = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=CURRENT_UTC_CELL,
                valueInputOption="RAW",
                body={"values": [[utc_now_str_iter]]},
            ).execute()
            log.debug(f"Heartbeat -> {CURRENT_UTC_CELL} set to {utc_now_str_iter}")
        except Exception as e:
            log.error(f"Failed to update Current!D2 heartbeat: {e}", exc_info=True)
        # Periodic in-memory sync to Sheets every SYNC_INTERVAL_SECONDS
        now = time.time()
        if now - last_sync_time >= SYNC_INTERVAL_SECONDS:
            log.info(
                f"Periodic sync: Writing in-memory state to Sheets (every {SYNC_INTERVAL_SECONDS}s, UTC-based)"
            )
            try:
                state.sync_to_sheets(service, spreadsheet_id)
                # --- Update Current!D2 with UTC timestamp after spreadsheet sync ---
                try:
                    utc_now_str = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S UTC"
                    )
                    service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=CURRENT_UTC_CELL,
                        valueInputOption="RAW",
                        body={"values": [[utc_now_str]]},
                    ).execute()
                    log.info(f"Updated {CURRENT_UTC_CELL} with UTC timestamp {utc_now_str}")
                except Exception as e:
                    log.error(
                        f"Failed to update {CURRENT_UTC_CELL} timestamp: {e}", exc_info=True
                    )
                last_sync_time = now
            except Exception as e:
                log.error(f"Periodic sync failed: {e}", exc_info=True)
        log.debug(
            f"Poll iteration {iteration} took {format_duration(elapsed)}; sleeping for {format_duration(sleep_time)} (UTC) …"
        )
        time.sleep(sleep_time)
        log.debug(f"Poll iteration {iteration} end (UTC)")

    # Final sync in-memory state to Sheets before finishing
    state.sync_to_sheets(service, spreadsheet_id)
    # --- Update Current!D2 with UTC timestamp after final spreadsheet sync ---
    try:
        utc_now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=CURRENT_UTC_CELL,
            valueInputOption="RAW",
            body={"values": [[utc_now_str]]},
        ).execute()
        log.info(f"Updated {CURRENT_UTC_CELL} with UTC timestamp {utc_now_str}")
    except Exception as e:
        log.error(f"Failed to update {CURRENT_UTC_CELL} timestamp: {e}", exc_info=True)
    log.info("Watcher finished — runtime limit reached (UTC).")


def main():
    log.info("Starting main function")
    log.info(
        f"Configuration loaded: SHEET_ID={SHEET_ID}, SHEET_RANGE={SHEET_RANGE}, INTERVAL_SECONDS={INTERVAL_SECONDS}, DURATION_MINUTES={DURATION_MINUTES}, MONITOR_RANGE={MONITOR_RANGE}"
    )
    service = sheets.get_sheets_service()
    # UTC verification diagnostics (if enabled)
    if DEBUG_UTC_MODE:
        verify_utc_timing(service, SHEET_ID)
    # Check if we should start this run (based on next floor trial start time)
    if not should_start_run(service, SHEET_ID):
        log.info("No active or near-future floor trial — stopping run.")
        return
    # Load state from sheets at startup
    load_state_from_sheets(service, SHEET_ID)
    # Start time for max runtime check
    start_time = datetime.now(timezone.utc)
    # Main watcher loop with runtime check
    # Instead of calling run_watcher directly, inline the runtime check logic
    # (For legacy compatibility, but you can also add runtime check inside run_watcher if desired)
    # We'll call run_watcher, but patch it here for runtime check in the watcher loop.
    # So, instead, add a runtime check inside run_watcher below.
    run_watcher_with_runtime(
        SHEET_ID, SHEET_RANGE, INTERVAL_SECONDS, DURATION_MINUTES, MONITOR_RANGE, start_time
    )
    log.info("Main function complete")


# --- Wrap run_watcher to add runtime check ---
def run_watcher_with_runtime(
    spreadsheet_id: str,
    sheet_range: str,
    interval_seconds: int,
    duration_minutes: int,
    monitor_range: str,
    start_time,
):
    """
    Wraps run_watcher and adds a runtime limit check (MAX_RUNTIME_HOURS).
    """
    log.info("Starting run_watcher with runtime limit")
    service = sheets.get_sheets_service()

    if not isRunning(service, spreadsheet_id):
        return

    # Initialize SpreadsheetState and load from Sheets
    state = SpreadsheetState()
    state.load_from_sheets(service, spreadsheet_id)
    state.visualize()

    utc_end_time = datetime.now(timezone.utc) + timedelta(minutes=duration_minutes)
    log.info(
        f"Watcher starting: spreadsheet_id={spreadsheet_id}, range={sheet_range}, "
        f"interval={interval_seconds}s, duration={duration_minutes}min (UTC), monitor_range={monitor_range}"
    )
    iteration = 0
    last_sync_time = time.time()
    ACTION_RANGE = MONITOR_RANGE
    while datetime.now(timezone.utc) < utc_end_time:
        iteration += 1
        log.debug(f"Poll iteration {iteration} start (UTC)")
        poll_start = time.time()
        try:
            # Update Floor Trial status before processing submissions/actions
            in_progress = update_floor_trial_status(service, spreadsheet_id)
            # Import external submissions before processing raw submissions
            import_external_submissions(service, state, EXTERNAL_SOURCE_SHEET_ID)
            # --- Process actions in memory before raw submissions ---
            action_values = fetch_sheet_values(service, spreadsheet_id, ACTION_RANGE)
            process_actions_in_memory(state, action_values)
            # --- End new logic ---
            process_raw_submissions_in_memory(state)
            current_values = fetch_sheet_values(service, spreadsheet_id, monitor_range)
            # Instead of change detection, process if any monitored cell is non-empty
            any_nonempty = any(
                (row and str(row[0]).strip() != "") for row in current_values
            )
            if any_nonempty:
                log.debug(
                    "Detected at least one non-empty monitored cell; processing changes."
                )
                # Optionally: process_actions(service, spreadsheet_id, monitor_range, current_values)
            else:
                log.debug("No non-empty values in monitored cells this poll iteration.")
            # Fill empty rows in Current!E6:I11 from queues after processing actions
            if in_progress:
                fill_current_from_queues(service, spreadsheet_id, state)
            else:
                log.info(
                    "⏸️ Floor Trial not in progress — skipping current queue population"
                )
        except Exception as e:
            log.error(f"Error during polling: {e}", exc_info=True)
        elapsed = time.time() - poll_start
        sleep_time = max(0, interval_seconds - elapsed)
        state.visualize()
        # Heartbeat: write current UTC time to Current!D2 every loop for accuracy
        try:
            utc_now_str_iter = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=CURRENT_UTC_CELL,
                valueInputOption="RAW",
                body={"values": [[utc_now_str_iter]]},
            ).execute()
            log.debug(f"Heartbeat -> {CURRENT_UTC_CELL} set to {utc_now_str_iter}")
        except Exception as e:
            log.error(f"Failed to update Current!D2 heartbeat: {e}", exc_info=True)
        # Periodic in-memory sync to Sheets every SYNC_INTERVAL_SECONDS
        now = time.time()
        if now - last_sync_time >= SYNC_INTERVAL_SECONDS:
            log.info(
                f"Periodic sync: Writing in-memory state to Sheets (every {SYNC_INTERVAL_SECONDS}s, UTC-based)"
            )
            try:
                state.sync_to_sheets(service, spreadsheet_id)
                # --- Update Current!D2 with UTC timestamp after spreadsheet sync ---
                try:
                    utc_now_str = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S UTC"
                    )
                    service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=CURRENT_UTC_CELL,
                        valueInputOption="RAW",
                        body={"values": [[utc_now_str]]},
                    ).execute()
                    log.info(f"Updated {CURRENT_UTC_CELL} with UTC timestamp {utc_now_str}")
                except Exception as e:
                    log.error(
                        f"Failed to update {CURRENT_UTC_CELL} timestamp: {e}", exc_info=True
                    )
                last_sync_time = now
            except Exception as e:
                log.error(f"Periodic sync failed: {e}", exc_info=True)
        # --- Runtime check ---
        elapsed_hours = (datetime.now(timezone.utc) - start_time).total_seconds() / 3600
        if elapsed_hours >= MAX_RUNTIME_HOURS:
            log.info(f"Reached max runtime of {MAX_RUNTIME_HOURS} hours — exiting gracefully.")
            break
        log.debug(
            f"Poll iteration {iteration} took {format_duration(elapsed)}; sleeping for {format_duration(sleep_time)} (UTC) …"
        )
        time.sleep(sleep_time)
        log.debug(f"Poll iteration {iteration} end (UTC)")

    # Final sync in-memory state to Sheets before finishing
    state.sync_to_sheets(service, spreadsheet_id)
    # --- Update Current!D2 with UTC timestamp after final spreadsheet sync ---
    try:
        utc_now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=CURRENT_UTC_CELL,
            valueInputOption="RAW",
            body={"values": [[utc_now_str]]},
        ).execute()
        log.info(f"Updated {CURRENT_UTC_CELL} with UTC timestamp {utc_now_str}")
    except Exception as e:
        log.error(f"Failed to update {CURRENT_UTC_CELL} timestamp: {e}", exc_info=True)
    log.info("Watcher finished — runtime limit reached (UTC).")


if __name__ == "__main__":
    main()
