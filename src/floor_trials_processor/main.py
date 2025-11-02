#!/usr/bin/env python3
"""
main.py — Continuous Google Sheets watcher + processor utilising kaiano-common-utils.

This script will run for a configurable time, poll a configurable sheet range at a configurable interval,
and when values change in the watched sheet it will trigger processing (moving rows etc.).
"""

import time
from datetime import datetime, timedelta
def process_actions_in_memory(state: "SpreadsheetState", action_values: "List[List[str]]"):
    """
    Processes action commands from action_values (corresponding to C6:C11).
    Modifies state.sections["current_queue"] and state.sections["history"] in memory.
    """
    import copy
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
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
                        if (
                            r_leader.lower() == leader.lower()
                            and r_follower.lower() == follower.lower()
                            and r_division.lower() == division.lower()
                        ):
                            # Column 4 (index 4) is run count
                            try:
                                count = int(str(report_row[4]).strip()) if str(report_row[4]).strip() else 0
                            except Exception:
                                count = 0
                            report_row[4] = str(count + 1)
                            state.mark_dirty("report")
                            log.info(f"Incremented run count for {leader}/{follower}/{division}")
                            found = True
                            break
                    # Optionally log if not found in report
                    if not found:
                        log.debug(f"No matching report row found for {leader}/{follower}/{division} to increment run count.")
                    # Sort report data by leader name (column 0)
                    report_data.sort(key=lambda r: str(r[0]).lower() if len(r) > 0 else "")
                    state.sections["report"]["data"] = report_data
                    state.mark_dirty("report")
                    log.info("Sorted report data by leader name after run count update.")
                except Exception as e:
                    log.error(f"Error incrementing run count in report: {e}", exc_info=True)
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
    empty = [r for r in current_queue if not any(str(cell).strip() for cell in r)]
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
        range="Current!C6:C11",
        valueInputOption="RAW",
        body={"values": action_values},
    ).execute()
    log.info("process_actions_in_memory: Cleared commands written back to sheet.")
    # Optionally mark actions as cleared or log debug note
    log.debug("process_actions_in_memory: Cleared processed commands from action_values after handling.")
from typing import Any, List, Optional

from kaiano_common_utils import google_sheets as sheets
from kaiano_common_utils import logger as log

# ---------------------------------------------------------------------
# CONFIGURATION CONSTANTS
# ---------------------------------------------------------------------
# Google Sheet ID for the Floor Trials Processor
SHEET_ID = "1JsWQDnxHis79dHcdlZZlgO1HS5Ldlq8zvLD1FdEwBQ4"
# Main range to monitor in the Current sheet (all columns)
SHEET_RANGE = "Current!A:Z"
# Polling interval in seconds for the watcher
INTERVAL_SECONDS = 10
# Periodic sync interval in seconds for writing memory to Sheets
SYNC_INTERVAL_SECONDS = 10
# Total duration to run the watcher in minutes
DURATION_MINUTES = 60
# Range of monitored cells (actions) in the Current sheet
MONITOR_RANGE = "Current!C6:C11"
# Sheet name for the History log
HISTORY_SHEET_NAME = "History"
# Sheet name for the Priority queue
PRIORITY_SHEET_NAME = "Priority"
# Sheet name for the NonPriority queue
NON_PRIORITY_SHEET_NAME = "NonPriority"
# Range for raw submissions to process
RAW_SUBMISSIONS_RANGE = "RawSubmissions!C4:F"
# Report sheet configuration
REPORT_SHEET_NAME = "Report"
REPORT_RANGE = "Report!A4:E"
# Range for the current queue in the Current sheet
CURRENT_QUEUE_RANGE = "Current!E6:I11"
# Range for compaction in the Current sheet
COMPACTION_RANGE = "Current!E6:J12"
# Range for Floor Trial status display
FLOOR_TRIAL_STATUS_RANGE = "Current!B19:D19"
# Cell for Floor Trial date
FLOOR_TRIAL_DATE_CELL = "Current!D15"
# Cell for Floor Trial start time
FLOOR_TRIAL_START_CELL = "Current!B17"
# Cell for Floor Trial end time
FLOOR_TRIAL_END_CELL = "Current!D17"
# Cell controlling automation run/stop
AUTOMATION_CONTROL_CELL = "Current!H2"
# Range for Priority division list
PRIORITY_DIVISION_RANGE = "Current!F16:F30"
# Range for Priority flag (X) list
PRIORITY_FLAG_RANGE = "Current!G16:G30"
# Range for Priority queue
PRIORITY_QUEUE_RANGE = "Priority!B3:F"
# Range for NonPriority queue
NON_PRIORITY_QUEUE_RANGE = "NonPriority!B3:F"
# Range for rejected submissions
REJECTED_SUBMISSIONS_RANGE = "RejectedSubmissions!B:H"

# External source spreadsheet for importing new submissions
EXTERNAL_SOURCE_SHEET_ID = "193QJBSQKkW1-c2Z3WHv3o2rbX-zZwn9fnmEACj88cEw"
EXTERNAL_SOURCE_RANGE = "Form Responses 1!A2:H"

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
                vals = value_ranges[idx].get("values", []) if idx < len(value_ranges) else []
                self.sections[name]["data"] = vals
            log.info("SpreadsheetState: Loaded all sections from sheets.")
        except Exception as e:
            log.error(f"SpreadsheetState: Error loading from sheets: {e}", exc_info=True)

    def sync_to_sheets(self, service, spreadsheet_id):
        """
        Pushes updates for all dirty sections to Google Sheets.
        """
        for name in list(self.dirty_sections):
            section = self.sections.get(name)
            if not section:
                log.warning(f"SpreadsheetState: Unknown section '{name}' - cannot sync.")
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
                log.error(f"SpreadsheetState: Error syncing '{name}': {e}", exc_info=True)

    def mark_dirty(self, section_name):
        """
        Mark a section as dirty (modified, needs sync).
        """
        if section_name in self.sections:
            self.dirty_sections.add(section_name)
            log.debug(f"SpreadsheetState: Marked section '{section_name}' as dirty.")
        else:
            log.warning(f"SpreadsheetState: Tried to mark unknown section '{section_name}' as dirty.")

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
            sep_row = "----|" + "|".join("-" * (col_widths[i] + 2) for i in range(num_cols))
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
def import_external_submissions(service, state: SpreadsheetState, source_spreadsheet_id: str):
    """
    Imports new submissions from external spreadsheet into in-memory state.
    - Reads from external Form Responses sheet.
    - Maps A, C, D, E, F, G → RawSubmissions (B–G equivalent in memory).
    - Normalizes HasStartingCue field to 'Yes'/'No'.
    - Marks processed rows (H = X) in source.
    - Updates state.sections['raw_submissions']['data'] and marks dirty.
    """
    log.info("import_external_submissions: Starting import from external source (in-memory mode).")

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

            has_starting_cue = "Yes" if "specific cue" in str(row_padded[5]).lower() else "No"

            mapped_row = [
                row_padded[0],  # Timestamp
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
        updates = [["X" if idx in rows_to_mark else (values[idx][7] if len(values[idx]) > 7 else "")]
                   for idx in range(len(values))]
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


TARGET_SHEET_NAME = "Processed"  # Placeholder for future configuration


def process_actions(
    service, spreadsheet_id: str, monitor_range: str, current_values: List[List[Any]]
):
    log.info("Detected change — starting processing ...")
    # monitor_range like "Current!C6:C11"
    # current_values: list of lists, each is a row (1 cell per row, since C6:C11)
    try:
        # Parse monitor_range for sheet and row info
        # e.g., "Current!C6:C11"
        import re

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
                timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
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
        # Compact the sheet after clearing the row
        # Remove all empty rows, preserve order, pad to same total rows
        non_empty = [r for r in values if any(str(cell).strip() for cell in r)]
        empty = [r for r in values if not any(str(cell).strip() for cell in r)]
        total_rows = len(values)
        compacted = list(non_empty)
        while len(compacted) < total_rows:
            compacted.append([""] * 5)
        # Logging compaction
        log.info(
            f"process_priority: Compacted Priority!B3:F — {len(non_empty)} non-empty, {len(empty)} empty rows"
        )
        # Update in-memory state and mark dirty
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
        # Compact the sheet after clearing the row
        non_empty = [r for r in values if any(str(cell).strip() for cell in r)]
        empty = [r for r in values if not any(str(cell).strip() for cell in r)]
        total_rows = len(values)
        compacted = list(non_empty)
        while len(compacted) < total_rows:
            compacted.append([""] * 5)
        log.info(
            f"process_non_priority: Compacted NonPriority!B3:F — {len(non_empty)} non-empty, {len(empty)} empty rows"
        )
        # Update in-memory state and mark dirty
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
        filled_from = None
        # Normalize priority_queue rows to 5 columns
        pq = state.sections["priority_queue"]["data"]
        pq_rows = [r + [""] * (5 - len(r)) if len(r) < 5 else r[:5] for r in pq]
        taken_priority = None
        for pq_idx, pq_row in enumerate(pq_rows):
            if any(str(cell).strip() for cell in pq_row):
                taken_priority = pq_row
                pq_rows[pq_idx] = [""] * 5
                log.info(f"fill_current_from_queues: Taking row {pq_idx+3} from Priority queue: {taken_priority}")
                filled_from = "Priority"
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
            log.info(f"fill_current_from_queues: Compacted Priority queue — {len(non_empty)} non-empty, {len(empty)} empty rows")
            # Insert taken row directly (5 columns, E–I)
            current_data[idx] = taken_priority
            # Pad to 5 columns in case
            if len(current_data[idx]) < 5:
                current_data[idx] += [""] * (5 - len(current_data[idx]))
            elif len(current_data[idx]) > 5:
                current_data[idx] = current_data[idx][:5]
            state.mark_dirty("current_queue")
            log.info(f"fill_current_from_queues: Filled Current queue row {row_num} (cols E–I) from Priority queue: {taken_priority}")
            changes_made = True
            continue
        # If not Priority, try NonPriority, but only for rows 6–9 (row_num 6,7,8,9)
        if row_num > 9:
            log.info(f"fill_current_from_queues: Skipping NonPriority for row {row_num} (bottom two slots must be Priority only).")
            log.debug(f"fill_current_from_queues: Only Priority queue may fill Current rows 10 and 11 (row {row_num}).")
            log.info(f"fill_current_from_queues: No data available to fill row {row_num}.")
            continue
        npq = state.sections["non_priority_queue"]["data"]
        npq_rows = [r + [""] * (5 - len(r)) if len(r) < 5 else r[:5] for r in npq]
        taken_nonpriority = None
        for npq_idx, npq_row in enumerate(npq_rows):
            if any(str(cell).strip() for cell in npq_row):
                taken_nonpriority = npq_row
                npq_rows[npq_idx] = [""] * 5
                log.info(f"fill_current_from_queues: Taking row {npq_idx+3} from NonPriority queue: {taken_nonpriority}")
                filled_from = "NonPriority"
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
            log.info(f"fill_current_from_queues: Compacted NonPriority queue — {len(non_empty)} non-empty, {len(empty)} empty rows")
            # Insert taken row directly (5 columns, E–I)
            current_data[idx] = taken_nonpriority
            if len(current_data[idx]) < 5:
                current_data[idx] += [""] * (5 - len(current_data[idx]))
            elif len(current_data[idx]) > 5:
                current_data[idx] = current_data[idx][:5]
            state.mark_dirty("current_queue")
            log.info(f"fill_current_from_queues: Filled Current queue row {row_num} (cols E–I) from NonPriority queue: {taken_nonpriority}")
            changes_made = True
            continue
        log.info(f"fill_current_from_queues: No data available to fill row {row_num}.")
    if not changes_made:
        log.info("fill_current_from_queues: No empty rows filled.")
    else:
        log.info("fill_current_from_queues: Current queue now starts at column F (E is first column of range); all rows are 5 columns: [E, F, G, H, I].")
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
        key = (leader.lower(), follower.lower(), division.lower())

        # Duplicate check (existing queues)
        all_existing = {
            tuple(str(c).strip().lower() for c in r[1:4])
            for r in priority_data + nonpriority_data
            if any(str(x).strip() for x in r)
        }
        if key in all_existing:
            log.info(f"Duplicate found for {leader} / {follower} / {division}")
            # Determine which queue the duplicate was found in
            found_in_priority = any(
                tuple(str(c).strip().lower() for c in r[1:4]) == key
                for r in priority_data
                if any(str(x).strip() for x in r)
            )
            found_in_nonpriority = any(
                tuple(str(c).strip().lower() for c in r[1:4]) == key
                for r in nonpriority_data
                if any(str(x).strip() for x in r)
            )
            if found_in_priority:
                rejected_data.append(row + ["Duplicate entry in Priority queue"])
            elif found_in_nonpriority:
                rejected_data.append(row + ["Duplicate entry in NonPriority queue"])
            else:
                rejected_data.append(row + ["Duplicate entry in queue"])
            continue

        # Determine destination
        is_priority = False
        for pname in priority_names:
            if pname and (division.startswith(pname) or pname.startswith(division)):
                is_priority = True
                break
        if is_priority:
            target_queue = priority_data
            dest = "Priority"
        else:
            target_queue = nonpriority_data
            dest = "NonPriority"
        # Add as 5 columns: ["", leader, follower, division, cue_desc]
        target_queue.append(["", leader, follower, division, cue_desc])
        # Append to report as [leader, follower, division, cue_desc, 0]
        report_data.append([leader, follower, division, cue_desc, 0])
        processed += 1
        log.info(f"Moved row to {dest}: {row}")

    # Sort report by leader name (column 0)
    # (Now first column is leader name, not timestamp)
    report_data.sort(key=lambda r: str(r[0]).lower() if r and len(r) > 0 else "")

    # Clear processed rows
    state.sections["raw_submissions"]["data"] = []
    state.sections["priority_queue"]["data"] = priority_data
    state.sections["non_priority_queue"]["data"] = nonpriority_data
    state.sections["report"]["data"] = report_data
    state.sections["rejected_submissions"]["data"] = rejected_data

    # Mark all affected sections dirty
    for sec in ["raw_submissions", "priority_queue", "non_priority_queue", "report", "rejected_submissions"]:
        state.mark_dirty(sec)

    log.info(f"process_raw_submissions_in_memory: Processed {processed} rows in-memory")


def update_floor_trial_status(service, spreadsheet_id):
    """
    Helper to update the Floor Trial status in Current!B19 (merged B19:D19) based on date/time fields.
    Reads Current!D15 (date), Current!B17 (start time), Current!D17 (end time).
    """
    try:
        # Read values
        ranges = [FLOOR_TRIAL_DATE_CELL, FLOOR_TRIAL_START_CELL, FLOOR_TRIAL_END_CELL]
        result = (
            service.spreadsheets()
            .values()
            .batchGet(spreadsheetId=spreadsheet_id, ranges=ranges)
            .execute()
        )
        d15_rows = result.get("valueRanges", [])[0].get("values", [])
        b17_rows = result.get("valueRanges", [])[1].get("values", [])
        d17_rows = result.get("valueRanges", [])[2].get("values", [])
        date_str = d15_rows[0][0].strip() if d15_rows and d15_rows[0] else ""
        start_time_str = b17_rows[0][0].strip() if b17_rows and b17_rows[0] else ""
        end_time_str = d17_rows[0][0].strip() if d17_rows and d17_rows[0] else ""
        log.info(
            f"update_floor_trial_status: Read date='{date_str}', start='{start_time_str}', end='{end_time_str}'"
        )
        # Parse date and times
        status = "Floor Trial Not Active"
        dt_start = None
        dt_end = None
        if date_str and start_time_str and end_time_str:
            try:
                # Accept date as YYYY-MM-DD or MM/DD/YYYY or DD/MM/YYYY
                # Try common formats
                date_formats = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]
                date_obj = None
                for fmt in date_formats:
                    try:
                        date_obj = datetime.strptime(date_str, fmt)
                        break
                    except Exception:
                        continue
                if not date_obj:
                    raise ValueError(f"Could not parse date '{date_str}'")

                # Accept time as HH:MM or HH:MM:SS or with AM/PM
                def parse_time(tstr):
                    for fmt in ("%H:%M", "%H:%M:%S", "%I:%M %p", "%I:%M:%S %p"):
                        try:
                            return datetime.strptime(tstr, fmt).time()
                        except Exception:
                            continue
                    raise ValueError(f"Could not parse time '{tstr}'")

                start_time_obj = parse_time(start_time_str)
                end_time_obj = parse_time(end_time_str)
                dt_start = datetime.combine(date_obj.date(), start_time_obj)
                dt_end = datetime.combine(date_obj.date(), end_time_obj)
                now_utc = datetime.utcnow()
                log.info(
                    f"update_floor_trial_status: Computed dt_start={dt_start.isoformat()}, dt_end={dt_end.isoformat()}, now_utc={now_utc.isoformat()}"
                )
                if dt_start <= now_utc <= dt_end:
                    status = "Floor Trial In Progress"
            except Exception as e:
                log.warning(f"update_floor_trial_status: Error parsing datetimes: {e}")
        else:
            log.info(
                "update_floor_trial_status: One or more date/time fields are empty; status will be Not Active."
            )
        # Update merged cell Current!B19 (B19:D19)
        update_range = FLOOR_TRIAL_STATUS_RANGE
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=update_range,
            valueInputOption="RAW",
            body={"values": [[status, "", ""]]},
        ).execute()
        log.info(
            f"update_floor_trial_status: Updated {update_range} with status='{status}'"
        )
    except Exception as e:
        log.error(f"update_floor_trial_status: Exception occurred: {e}", exc_info=True)


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

def run_watcher(
    spreadsheet_id: str,
    sheet_range: str,
    interval_seconds: int,
    duration_minutes: int,
    monitor_range: str,
):
    log.info("Watcher starting")
    service = sheets.get_sheets_service()

    if not isRunning(service, spreadsheet_id):
        return

    # Initialize SpreadsheetState and load from Sheets
    state = SpreadsheetState()
    state.load_from_sheets(service, spreadsheet_id)
    state.visualize()

    end_time = datetime.utcnow() + timedelta(minutes=duration_minutes)
    log.info(
        f"Watcher starting: spreadsheet_id={spreadsheet_id}, range={sheet_range}, "
        f"interval={interval_seconds}s, duration={duration_minutes}min, monitor_range={monitor_range}"
    )
    iteration = 0
    last_sync_time = time.time()
    ACTION_RANGE = "Current!C6:C11"
    while datetime.utcnow() < end_time:
        iteration += 1
        log.debug(f"Poll iteration {iteration} start")
        start = time.time()
        try:
            # Update Floor Trial status before processing submissions/actions
            update_floor_trial_status(service, spreadsheet_id)
            # Import external submissions before processing raw submissions
            import_external_submissions(service, state, EXTERNAL_SOURCE_SHEET_ID)
            # --- New: process actions in memory before raw submissions ---
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
                #process_actions(service, spreadsheet_id, monitor_range, current_values)
            else:
                log.debug("No non-empty values in monitored cells this poll iteration.")
            # Fill empty rows in Current!E6:I11 from queues after processing actions
            fill_current_from_queues(service, spreadsheet_id, state)
        except Exception as e:
            log.error(f"Error during polling: {e}", exc_info=True)
        elapsed = time.time() - start
        sleep_time = max(0, interval_seconds - elapsed)
        state.visualize()
        # Periodic in-memory sync to Sheets every SYNC_INTERVAL_SECONDS
        now = time.time()
        if now - last_sync_time >= SYNC_INTERVAL_SECONDS:
            log.info(f"Periodic sync: Writing in-memory state to Sheets (every {SYNC_INTERVAL_SECONDS}s)")
            try:
                state.sync_to_sheets(service, spreadsheet_id)
                last_sync_time = now
            except Exception as e:
                log.error(f"Periodic sync failed: {e}", exc_info=True)
        log.debug(
            f"Poll iteration {iteration} took {format_duration(elapsed)}; sleeping for {format_duration(sleep_time)} …"
        )
        time.sleep(sleep_time)
        log.debug(f"Poll iteration {iteration} end")

    # Sync in-memory state to Sheets before finishing
    state.sync_to_sheets(service, spreadsheet_id)
    log.info("Watcher finished — runtime limit reached.")


def main():
    log.info("Starting main function")
    log.info(
        f"Configuration loaded: SHEET_ID={SHEET_ID}, SHEET_RANGE={SHEET_RANGE}, INTERVAL_SECONDS={INTERVAL_SECONDS}, DURATION_MINUTES={DURATION_MINUTES}, MONITOR_RANGE={MONITOR_RANGE}"
    )
    run_watcher(
        SHEET_ID, SHEET_RANGE, INTERVAL_SECONDS, DURATION_MINUTES, MONITOR_RANGE
    )
    log.info("Main function complete")


if __name__ == "__main__":
    main()
