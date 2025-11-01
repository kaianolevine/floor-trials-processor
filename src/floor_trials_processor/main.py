#!/usr/bin/env python3
"""
main.py — Continuous Google Sheets watcher + processor utilising kaiano-common-utils.

This script will run for a configurable time, poll a configurable sheet range at a configurable interval,
and when values change in the watched sheet it will trigger processing (moving rows etc.).
"""

import time
from datetime import datetime, timedelta
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
INTERVAL_SECONDS = 60
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
COMPACTION_RANGE = "Current!E5:I12"
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
REJECTED_SUBMISSIONS_RANGE = "RejectedSubmissions!B:G"

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
def import_external_submissions(service, target_spreadsheet_id, source_spreadsheet_id):
    """
    Import new submissions from an external spreadsheet to RawSubmissions!B:G in the target sheet.
    - Fetch values from EXTERNAL_SOURCE_RANGE in the source spreadsheet.
    - Filter out rows where column H == "X" (case-insensitive).
    - For each unprocessed row, extract columns A, C, D, E, F, and G.
    - Append all mapped rows to RawSubmissions!B:G in the target spreadsheet.
    - Mark those rows as processed in column H in the source spreadsheet.
    - Log how many rows were imported and marked.
    """
    log.info("import_external_submissions: Starting import from external source.")
    try:
        # Fetch all rows from source
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=source_spreadsheet_id, range=EXTERNAL_SOURCE_RANGE)
            .execute()
        )
        values = result.get("values", [])
        log.debug(f"import_external_submissions: Fetched {len(values)} rows from external source.")
        mapped_rows = []
        rows_to_mark = []
        for idx, row in enumerate(values):
            # Pad row to at least 8 columns (A-H)
            row_padded = row + [""] * (8 - len(row)) if len(row) < 8 else row[:8]
            h_val = str(row_padded[7]).strip().lower()
            if h_val == "x":
                continue
            # Extract columns: A (0), C (2), D (3), E (4), F (5), G (6)
            mapped_row = [
                row_padded[0],  # A
                row_padded[2],  # C
                row_padded[3],  # D
                row_padded[4],  # E
                row_padded[5],  # F
                row_padded[6],  # G
            ]
            mapped_rows.append(mapped_row)
            rows_to_mark.append(idx)
        if mapped_rows:
            # Append to RawSubmissions!B:G in target spreadsheet
            append_range = "RawSubmissions!B:G"
            service.spreadsheets().values().append(
                spreadsheetId=target_spreadsheet_id,
                range=append_range,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": mapped_rows},
            ).execute()
            # Mark processed rows in column H with "X" in one update call
            # Compute the update range for H in source: Form Responses 1!H{row+2}:H{row+2} for each idx
            # Since EXTERNAL_SOURCE_RANGE starts at row 2, idx 0 -> row 2, idx 1 -> row 3, etc.
            update_rows = []
            for idx in rows_to_mark:
                update_rows.append(["X"])
            if rows_to_mark:
                first_row = rows_to_mark[0] + 2
                last_row = rows_to_mark[-1] + 2
                # Build update range as Sheet1!H{first_row}:H{last_row}
                update_range = f"Form Responses 1!H{first_row}:H{last_row}"
                # But if non-contiguous, need to update all rows at once - so just build a list of all target rows
                # To handle possible non-contiguous rows, update all at once using batchUpdate, but for simplicity, use update with all rows
                # Prepare values for each row in order
                update_rows_full = []
                for idx in range(len(values)):
                    if idx in rows_to_mark:
                        update_rows_full.append(["X"])
                    else:
                        # Keep existing value (or blank)
                        update_rows_full.append([values[idx][7]] if len(values[idx]) > 7 else [""])
                # Only update the rows that were fetched (all rows in the range)
                # So update Form Responses 1!H2:H{len(values)+1}
                full_update_range = f"Form Responses 1!H2:H{len(values)+1}"
                service.spreadsheets().values().update(
                    spreadsheetId=source_spreadsheet_id,
                    range=full_update_range,
                    valueInputOption="RAW",
                    body={"values": update_rows_full},
                ).execute()
            log.info(f"import_external_submissions: Imported {len(mapped_rows)} row(s) and marked as processed in external source.")
        else:
            log.info("import_external_submissions: No new unprocessed rows found in external source.")
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
def process_priority(service, spreadsheet_id):
    """
    Read from Priority!B3:F, find first non-empty row, log and clear, return as 5-cell list or None.
    """
    range_name = PRIORITY_QUEUE_RANGE
    values = fetch_sheet_values(service, spreadsheet_id, range_name)
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
        # Update the sheet in one call
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body={"values": compacted},
        ).execute()
        return taken_row
    log.info("process_priority: No non-empty rows found in Priority queue.")
    return None


def process_non_priority(service, spreadsheet_id):
    """
    Read from NonPriority!B3:F, find first non-empty row, log and clear, return as 5-cell list or None.
    """
    range_name = NON_PRIORITY_QUEUE_RANGE
    values = fetch_sheet_values(service, spreadsheet_id, range_name)
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
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body={"values": compacted},
        ).execute()
        return taken_row
    log.info("process_non_priority: No non-empty rows found in NonPriority queue.")
    return None


def fill_current_from_queues(service, spreadsheet_id):
    """
    Read Current!E6:I11, for each empty row, try to fill from Priority, then NonPriority (row ≤ 9).
    """
    range_name = CURRENT_QUEUE_RANGE
    values = fetch_sheet_values(service, spreadsheet_id, range_name)
    # Pad to always 6 rows
    while len(values) < 6:
        values.append([""] * 5)
    # Track if any changes made
    changes_made = False
    for idx, row in enumerate(values):
        row_num = 6 + idx
        # Pad row to 5 cells
        row_five = row + [""] * (5 - len(row)) if len(row) < 5 else row[:5]
        if not any(str(cell).strip() for cell in row_five):
            # Try Priority first
            data = process_priority(service, spreadsheet_id)
            if data is None and row_num <= 9:
                # Try NonPriority if Priority empty and row ≤ 9
                data = process_non_priority(service, spreadsheet_id)
            if data is not None:
                # Write data to Current!E{row_num}:I{row_num}
                update_range = f"Current!E{row_num}:I{row_num}"
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=update_range,
                    valueInputOption="RAW",
                    body={"values": [data]},
                ).execute()
                log.info(
                    f"fill_current_from_queues: Wrote data to {update_range}: {data}"
                )
                changes_made = True
            else:
                log.info(
                    f"fill_current_from_queues: No data available to fill row {row_num}."
                )
    if not changes_made:
        log.info("fill_current_from_queues: No empty rows filled.")


# ---------------------------------------------------------------------
# RawSubmissions processing function
# ---------------------------------------------------------------------
def process_raw_submissions(service, spreadsheet_id, max_rows_per_batch=20):
    """
    Efficiently process up to max_rows_per_batch rows from RawSubmissions!C4:F per batch,
    moving to Priority or NonPriority sheet based on division/priority, minimizing Google Sheets API read/write calls.
    Uses batchGet for reads, batched updates, and a single batchClear for source rows. Detailed logging for counts.
    """
    log.info(
        f"Starting process_raw_submissions ... (max_rows_per_batch={max_rows_per_batch})"
    )
    raw_range = RAW_SUBMISSIONS_RANGE
    divisions_range = PRIORITY_DIVISION_RANGE
    priority_range = PRIORITY_FLAG_RANGE
    batch_read_count = 0
    batch_write_count = 0
    # Fetch Report sheet data once at the beginning
    report_values = fetch_sheet_values(service, spreadsheet_id, REPORT_RANGE)
    # Step 1: Batch get all needed data
    ranges = [
        raw_range,
        divisions_range,
        priority_range,
        "Priority!C3:F",
        "NonPriority!C3:F",
    ]
    result = (
        service.spreadsheets()
        .values()
        .batchGet(spreadsheetId=spreadsheet_id, ranges=ranges)
        .execute()
    )
    batch_read_count += 1
    value_ranges = result.get("valueRanges", [])
    raw_values = value_ranges[0].get("values", [])
    divisions_values = value_ranges[1].get("values", [])
    priority_flags = value_ranges[2].get("values", [])
    priority_sheet_values = value_ranges[3].get("values", [])
    nonpriority_sheet_values = value_ranges[4].get("values", [])
    log.info(f"Fetched {len(raw_values)} rows from {raw_range}")
    # Flatten division/priority lists, pad to same length
    # Build division to priority mapping
    division_priority_map = {}
    for idx, divrow in enumerate(divisions_values):
        div_val = divrow[0].strip() if divrow and len(divrow) > 0 else ""
        if div_val == "":
            continue
        prio_flag = ""
        if idx < len(priority_flags):
            prio_flag = (
                priority_flags[idx][0].strip().upper()
                if priority_flags[idx] and len(priority_flags[idx]) > 0
                else ""
            )
        division_priority_map[div_val] = prio_flag
    priority_divisions = [
        div for div, flag in division_priority_map.items() if flag == "X"
    ]
    non_priority_divisions = [
        div for div, flag in division_priority_map.items() if flag != "X" and div != ""
    ]
    log.info(f"Priority divisions: {priority_divisions}")
    log.info(f"Non-priority divisions: {non_priority_divisions}")

    # Helper: find next open row given a sheet's C:F values (values from C3:F, first empty row = index + 3)
    def find_next_open_row_from_sheet(sheet_values):
        for i, row in enumerate(sheet_values):
            if not any(str(cell).strip() for cell in row):
                return i + 3
        return len(sheet_values) + 3

    processed_count = 0
    skipped_count = 0
    total_batches = 0
    start_time = time.time()
    # For batch appending to Report
    report_new_rows = []

    rejected_rows = []
    # --- Fetch Current!B17 value before the while loop ---
    current_time_rows = fetch_sheet_values(service, spreadsheet_id, FLOOR_TRIAL_START_CELL)
    current_time_str = current_time_rows[0][0].strip() if current_time_rows and current_time_rows[0] else ""
    def parse_time(tstr):
        for fmt in ("%H:%M", "%H:%M:%S", "%I:%M %p", "%I:%M:%S %p"):
            try:
                return datetime.strptime(tstr, fmt).time()
            except Exception:
                continue
        return None
    current_start_time = parse_time(current_time_str)

    while True:
        # Always use batchGet at the start of each batch
        result = (
            service.spreadsheets()
            .values()
            .batchGet(
                spreadsheetId=spreadsheet_id,
                ranges=[raw_range, "Priority!C3:F", "NonPriority!C3:F"],
            )
            .execute()
        )
        batch_read_count += 1
        raw_values = result.get("valueRanges", [])[0].get("values", [])
        priority_sheet_values = result.get("valueRanges", [])[1].get("values", [])
        nonpriority_sheet_values = result.get("valueRanges", [])[2].get("values", [])
        # Pad all rows to 4 cells
        raw_values = [
            row + [""] * (4 - len(row)) if len(row) < 4 else row[:4]
            for row in raw_values
        ]
        batch_processed = 0
        batch_skipped = 0
        rows_to_clear = []
        priority_moves = []
        nonpriority_moves = []
        priority_next_row = find_next_open_row_from_sheet(priority_sheet_values)
        nonpriority_next_row = find_next_open_row_from_sheet(nonpriority_sheet_values)
        skipped_divisions = set()
        skipped_rows = []
        # --- Collect existing (C, D, E) entries from Priority, NonPriority, and Current queues for duplicate check ---
        existing_entries = set()
        # Priority!C3:E, NonPriority!C3:E, Current!E6:G11
        for data_range in ["Priority!C3:E", "NonPriority!C3:E", "Current!E6:G11"]:
            vals = fetch_sheet_values(service, spreadsheet_id, data_range)
            for r in vals:
                if len(r) >= 3 and any(str(x).strip() for x in r[:3]):
                    existing_entries.add(tuple(str(x).strip().lower() for x in r[:3]))
        # Helper for normalization
        def norm_tuple(lst):
            return tuple(str(x).strip().lower() for x in lst)

        # Process up to max_rows_per_batch non-empty rows
        for idx, row in enumerate(raw_values):
            if batch_processed + batch_skipped >= max_rows_per_batch:
                break
            row_num = idx + 4
            if not any(str(cell).strip() for cell in row):
                continue
            # --- Validation logic ---
            is_valid = True
            rejection_reason = ""

            # --- Validation 1: Duplicate (C, D, E) in Priority, NonPriority, or Current ---
            cde = [row[0].strip(), row[1].strip(), row[2].strip()] if len(row) >= 3 else ["", "", ""]
            if norm_tuple(cde) in existing_entries:
                is_valid = False
                rejection_reason = "Duplicate entry in queue"

            # --- Validation 2: Time before Current!B17 ---
            if is_valid and current_start_time:
                raw_time_str = row[0].strip() if len(row) > 0 else ""
                raw_time_obj = parse_time(raw_time_str)
                if raw_time_obj and raw_time_obj < current_start_time:
                    is_valid = False
                    rejection_reason = f"Time {raw_time_str} before current start time {current_time_str}"

            if is_valid:
                div = row[2].strip() if len(row) > 2 else ""
                log.debug(f"Processing RawSubmissions row {row_num}: {row}")
                if div in priority_divisions:
                    dest_sheet = "Priority"
                    dest_row = priority_next_row
                    priority_moves.append((dest_row, row[:4]))
                    priority_next_row += 1
                elif div in non_priority_divisions:
                    dest_sheet = "NonPriority"
                    dest_row = nonpriority_next_row
                    nonpriority_moves.append((dest_row, row[:4]))
                    nonpriority_next_row += 1
                else:
                    log.warning(
                        f"Unknown division '{div}' in RawSubmissions row {row_num}; skipping."
                    )
                    skipped_count += 1
                    batch_skipped += 1
                    skipped_divisions.add(div)
                    skipped_rows.append(row_num)
                    continue

                # -- REPORT SHEET LOGIC --
                # row[0:4] = columns C, D, E, F from RawSubmissions, which map to A-D in Report
                # Check if a row already exists in report_values by matching first 3 columns (A, B, C)
                # Ignore case and whitespace
                def norm(x):
                    return str(x).strip().lower()
                match_found = False
                for report_row in report_values:
                    if (
                        len(report_row) >= 3
                        and norm(report_row[0]) == norm(row[0])
                        and norm(report_row[1]) == norm(row[1])
                        and norm(report_row[2]) == norm(row[2])
                    ):
                        match_found = True
                        break
                if not match_found:
                    # Compose new report row: [C, D, E, F, 0]
                    report_new_rows.append(row[0:4] + [0])

                log.info(
                    f"Will move RawSubmissions row {row_num} to {dest_sheet} at row {dest_row}: {row[:4]}"
                )
                rows_to_clear.append(row_num)
                processed_count += 1
                batch_processed += 1
            else:
                rejection_reason = rejection_reason or "Validation failed"
                log.info(f"Rejecting RawSubmissions row {row_num} for reason: {rejection_reason}")
                # Capture all columns B through G (we can reconstruct from context)
                # Since our raw_values represent columns C-F, we need to also fetch column B separately
                rejected_row_range = f"RawSubmissions!B{row_num}:G{row_num}"
                rejected_row_values = fetch_sheet_values(service, spreadsheet_id, rejected_row_range)
                if rejected_row_values and rejected_row_values[0]:
                    rejected_row = rejected_row_values[0]
                    # Append reason to column H
                    while len(rejected_row) < 6:
                        rejected_row.append("")
                    rejected_row.append(rejection_reason)
                    rejected_rows.append(rejected_row)
                else:
                    log.warning(f"Could not fetch values for rejected row {row_num}")
                rows_to_clear.append(row_num)
                processed_count += 1
                batch_processed += 1
                continue
        # Perform batch update for Priority moves
        if priority_moves:
            update_range = f"Priority!C{priority_moves[0][0]}:F{priority_moves[-1][0]}"
            update_values = []
            for dest_row, row_vals in priority_moves:
                update_values.append(row_vals)
            log.info(
                f"Batch updating {len(priority_moves)} rows to Priority!C:F at rows {[r for r, _ in priority_moves]}"
            )
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=update_range,
                valueInputOption="RAW",
                body={"values": update_values},
            ).execute()
            batch_write_count += 1
        # Perform batch update for NonPriority moves
        if nonpriority_moves:
            update_range = (
                f"NonPriority!C{nonpriority_moves[0][0]}:F{nonpriority_moves[-1][0]}"
            )
            update_values = []
            for dest_row, row_vals in nonpriority_moves:
                update_values.append(row_vals)
            log.info(
                f"Batch updating {len(nonpriority_moves)} rows to NonPriority!C:F at rows {[r for r, _ in nonpriority_moves]}"
            )
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=update_range,
                valueInputOption="RAW",
                body={"values": update_values},
            ).execute()
            batch_write_count += 1
        # Batch clear all moved rows in RawSubmissions
        if rows_to_clear:
            clear_ranges = [
                f"RawSubmissions!C{row_num}:F{row_num}" for row_num in rows_to_clear
            ]
            log.info(
                f"Batch clearing {len(clear_ranges)} RawSubmissions rows: {rows_to_clear}"
            )
            service.spreadsheets().values().batchClear(
                spreadsheetId=spreadsheet_id,
                body={"ranges": clear_ranges},
            ).execute()
            batch_write_count += 1
        # After batch, compact RawSubmissions!C4:F upward (remove blank rows)
        compact_vals = fetch_sheet_values(service, spreadsheet_id, raw_range)
        batch_read_count += 1
        compact_vals = [
            r + [""] * (4 - len(r)) if len(r) < 4 else r[:4] for r in compact_vals
        ]
        non_empty = [r for r in compact_vals if any(str(cell).strip() for cell in r)]
        total_rows = len(compact_vals)
        while len(non_empty) < total_rows:
            non_empty.append([""] * 4)
        if compact_vals != non_empty:
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=raw_range,
                valueInputOption="RAW",
                body={"values": non_empty},
            ).execute()
            batch_write_count += 1
            log.info("Compacted RawSubmissions!C4:F after batch processing.")
        else:
            log.debug("No compaction needed after batch.")
        total_batches += 1
        log.info(
            f"Batch {total_batches}: processed {batch_processed} rows, skipped {batch_skipped} rows."
        )
        log.info(
            f"API usage so far: {batch_read_count} read(s), {batch_write_count} write(s) this batch."
        )
        if skipped_rows:
            log.info(
                f"Skipped rows this batch: {skipped_rows} (unknown divisions: {list(skipped_divisions)})"
            )
        if batch_processed == 0 and batch_skipped == 0:
            break

    # After the loop, before clearing and compaction, append rejected rows in one batch if any:
    if rejected_rows:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=REJECTED_SUBMISSIONS_RANGE,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rejected_rows},
        ).execute()
        log.info(f"Appended {len(rejected_rows)} rejected rows to RejectedSubmissions sheet.")

    # After all batches, append new rows to Report if any
    if report_new_rows:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=REPORT_RANGE,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": report_new_rows},
        ).execute()
        log.info(f"Appended {len(report_new_rows)} new rows to Report sheet.")
        # Now, sort the Report sheet by column A
        try:
            # Fetch all data from Report sheet for sorting
            report_data_all = fetch_sheet_values(service, spreadsheet_id, REPORT_RANGE)
            # Sort rows based on column A (index 0), ignoring case
            sorted_report_data = sorted(report_data_all, key=lambda r: str(r[0]).lower() if r and len(r) > 0 else "")
            # Write back the sorted data to the Report sheet
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=REPORT_RANGE,
                valueInputOption="RAW",
                body={"values": sorted_report_data},
            ).execute()
            log.info(f"Sorted Report sheet by column A after appending {len(report_new_rows)} rows.")
        except Exception as e:
            log.error(f"Error while sorting Report sheet: {e}", exc_info=True)
    elapsed = time.time() - start_time
    log.info(
        f"process_raw_submissions complete: processed {processed_count} rows, skipped {skipped_count} rows in {total_batches} batches. "
        f"Total API usage: {batch_read_count} read(s), {batch_write_count} write(s), elapsed {format_duration(elapsed)}"
    )


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

    end_time = datetime.utcnow() + timedelta(minutes=duration_minutes)
    log.info(
        f"Watcher starting: spreadsheet_id={spreadsheet_id}, range={sheet_range}, "
        f"interval={interval_seconds}s, duration={duration_minutes}min, monitor_range={monitor_range}"
    )

    iteration = 0
    while datetime.utcnow() < end_time:
        iteration += 1
        log.debug(f"Poll iteration {iteration} start")
        start = time.time()
        try:
            # Update Floor Trial status before processing submissions/actions
            update_floor_trial_status(service, spreadsheet_id)
            # Import external submissions before processing raw submissions
            import_external_submissions(service, spreadsheet_id, EXTERNAL_SOURCE_SHEET_ID)
            #process_raw_submissions(service, spreadsheet_id)
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
            #fill_current_from_queues(service, spreadsheet_id)
        except Exception as e:
            log.error(f"Error during polling: {e}", exc_info=True)
        elapsed = time.time() - start
        sleep_time = max(0, interval_seconds - elapsed)
        log.debug(
            f"Poll iteration {iteration} took {format_duration(elapsed)}; sleeping for {format_duration(sleep_time)} …"
        )
        time.sleep(sleep_time)
        log.debug(f"Poll iteration {iteration} end")

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
