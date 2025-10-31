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

        # Ensure "History" sheet exists before writing
        HISTORY_SHEET_NAME = "History"
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
        compaction_range = f"{sheet_name}!E5:I12"
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
            log.info("Compaction complete: E5:I12 compacted upward.")
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
    range_name = "Priority!B3:F"
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
    range_name = "NonPriority!B3:F"
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
    range_name = "Current!E6:I11"
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
    Efficiently process up to max_rows_per_batch rows from RawSubmissions!B4:E per batch,
    moving to Priority or NonPriority sheet based on division/priority, minimizing Google Sheets API read/write calls.
    Uses batchGet for reads, batched updates, and a single batchClear for source rows. Detailed logging for counts.
    """
    log.info(
        f"Starting process_raw_submissions ... (max_rows_per_batch={max_rows_per_batch})"
    )
    raw_range = "RawSubmissions!B4:E"
    divisions_range = "Current!F16:F30"
    priority_range = "Current!G16:G30"
    batch_read_count = 0
    batch_write_count = 0
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
        # Process up to max_rows_per_batch non-empty rows
        for idx, row in enumerate(raw_values):
            if batch_processed + batch_skipped >= max_rows_per_batch:
                break
            row_num = idx + 4
            if not any(str(cell).strip() for cell in row):
                continue
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
            log.info(
                f"Will move RawSubmissions row {row_num} to {dest_sheet} at row {dest_row}: {row[:4]}"
            )
            rows_to_clear.append(row_num)
            processed_count += 1
            batch_processed += 1
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
                f"RawSubmissions!B{row_num}:E{row_num}" for row_num in rows_to_clear
            ]
            log.info(
                f"Batch clearing {len(clear_ranges)} RawSubmissions rows: {rows_to_clear}"
            )
            service.spreadsheets().values().batchClear(
                spreadsheetId=spreadsheet_id,
                body={"ranges": clear_ranges},
            ).execute()
            batch_write_count += 1
        # After batch, compact RawSubmissions!B4:E upward (remove blank rows)
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
            log.info("Compacted RawSubmissions!B4:E after batch processing.")
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
        ranges = ["Current!D15", "Current!B17", "Current!D17"]
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
        update_range = "Current!B19:D19"
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


def run_watcher(
    spreadsheet_id: str,
    sheet_range: str,
    interval_seconds: int,
    duration_minutes: int,
    monitor_range: str,
):
    log.info("Watcher starting")
    service = sheets.get_sheets_service()

    # Check Current!H2 value before running watcher loop
    try:
        h2_value_rows = fetch_sheet_values(service, spreadsheet_id, "Current!H2")
        h2_val = h2_value_rows[0][0] if h2_value_rows and h2_value_rows[0] else ""
        log.info(f"Fetched Current!H2 value: '{h2_val}'")
        if str(h2_val).strip().lower() != "runautomations":
            log.warning(
                f"Automation stopped: H2 is not RunAutomations (value was '{h2_val}')"
            )
            return
        else:
            log.info(
                "Current!H2 is 'RunAutomations' (case-insensitive) — proceeding with watcher."
            )
    except Exception as e:
        log.error(f"Error fetching Current!H2 value: {e}", exc_info=True)
        log.warning("Automation stopped: Could not fetch Current!H2 value.")
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
            time.sleep(3)
            # Update Floor Trial status before processing submissions/actions
            update_floor_trial_status(service, spreadsheet_id)
            time.sleep(3)
            process_raw_submissions(service, spreadsheet_id)
            time.sleep(3)
            current_values = fetch_sheet_values(service, spreadsheet_id, monitor_range)
            # Instead of change detection, process if any monitored cell is non-empty
            any_nonempty = any(
                (row and str(row[0]).strip() != "") for row in current_values
            )
            if any_nonempty:
                log.debug(
                    "Detected at least one non-empty monitored cell; processing changes."
                )
                time.sleep(3)
                process_actions(service, spreadsheet_id, monitor_range, current_values)
            else:
                log.debug("No non-empty values in monitored cells this poll iteration.")
            # Fill empty rows in Current!E6:I11 from queues after processing actions
            fill_current_from_queues(service, spreadsheet_id)
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
    SHEET_ID = "1TW21azr-P8PlvGyEQ7MCYXq3unG5JkdgO4pcV2j75Hw"
    SHEET_RANGE = "Current!A:Z"
    INTERVAL_SECONDS = int("20")
    DURATION_MINUTES = int("60")
    MONITOR_RANGE = "Current!C6:C11"

    log.info(
        f"Configuration loaded: SHEET_ID={SHEET_ID}, SHEET_RANGE={SHEET_RANGE}, INTERVAL_SECONDS={INTERVAL_SECONDS}, DURATION_MINUTES={DURATION_MINUTES}, MONITOR_RANGE={MONITOR_RANGE}"
    )

    run_watcher(
        SHEET_ID, SHEET_RANGE, INTERVAL_SECONDS, DURATION_MINUTES, MONITOR_RANGE
    )
    log.info("Main function complete")


if __name__ == "__main__":
    main()
