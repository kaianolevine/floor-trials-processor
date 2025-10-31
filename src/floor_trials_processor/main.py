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


def process_changes(
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

        actions_taken = False
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
                actions_taken = True
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
                actions_taken = True
            elif val_lc == "-":
                log.info(
                    f"Moving E:I data from row {row_num} to row 12 and clearing original"
                )
                ei_range = f"{sheet_name}!E{row_num}:I{row_num}"
                ei_values = fetch_sheet_values(service, spreadsheet_id, ei_range)
                ei_values = (
                    ei_values[0] if ei_values and len(ei_values) > 0 else [""] * 5
                )
                target_range = f"{sheet_name}!E12:I12"
                if can_modify_range(target_range):
                    log.debug(f"Writing values to {target_range}: {ei_values}")
                    service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=target_range,
                        valueInputOption="RAW",
                        body={"values": [ei_values]},
                    ).execute()
                else:
                    log.warning(
                        f"Attempted to update out-of-bounds range {target_range}, skipping."
                    )
                log.debug(f"Clearing E:I in original row {row_num}")
                if can_modify_range(ei_range):
                    service.spreadsheets().values().clear(
                        spreadsheetId=spreadsheet_id, range=ei_range, body={}
                    ).execute()
                else:
                    log.warning(
                        f"Attempted to clear out-of-bounds range {ei_range}, skipping."
                    )
                # Always clear the triggering monitored cell (column C)
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
                actions_taken = True
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
                actions_taken = True
            else:
                # Empty cell: nothing to do
                continue
        # After all actions, compact E5:I12 upward (preserving order)
        if actions_taken:
            compaction_range = f"{sheet_name}!E5:I12"
            compaction_data = fetch_sheet_values(
                service, spreadsheet_id, compaction_range
            )
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
                row
                for row in compaction_data
                if not any(str(cell).strip() for cell in row)
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
        else:
            log.info("No actions taken; skipping compaction.")
    except Exception as e:
        log.error(f"Error in process_changes: {e}", exc_info=True)
    log.info("Processing complete.")


def run_watcher(
    spreadsheet_id: str,
    sheet_range: str,
    interval_seconds: int,
    duration_minutes: int,
    monitor_range: str,
):
    log.info("Watcher starting")
    service = sheets.get_sheets_service()

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
            current_values = fetch_sheet_values(service, spreadsheet_id, monitor_range)
            # Instead of change detection, process if any monitored cell is non-empty
            any_nonempty = any(
                (row and str(row[0]).strip() != "") for row in current_values
            )
            if any_nonempty:
                log.debug(
                    "Detected at least one non-empty monitored cell; processing changes."
                )
                process_changes(service, spreadsheet_id, monitor_range, current_values)
            else:
                log.debug("No non-empty values in monitored cells this poll iteration.")
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
    INTERVAL_SECONDS = int("15")
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
