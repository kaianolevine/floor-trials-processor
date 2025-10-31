#!/usr/bin/env python3
"""
main.py — Continuous Google Sheets watcher + processor utilising kaiano-common-utils.

This script will run for a configurable time, poll a configurable sheet range at a configurable interval,
and when values change in the watched sheet it will trigger processing (moving rows etc.).
"""

import time
from datetime import datetime, timedelta
from typing import Any, List, Optional

from kaiano_common_utils import logger as log
from kaiano_common_utils.google_sheets import sheets


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
        # Fetch the full sheet data for rows of interest (E:I for rows start_row to end_row)
        # We'll need to manipulate rows in the main sheet, so fetch E:I for all rows in range
        # data_range = f"{sheet_name}!E{start_row}:I{end_row}"
        # sheet_data = fetch_sheet_values(service, spreadsheet_id, data_range)
        # For repacking, fetch all data from A:I (or A:Z, but we only process E:I) from row 6 downward
        # all_data_range = f"{sheet_name}!A6:I"
        # all_data = fetch_sheet_values(service, spreadsheet_id, all_data_range)
        # Map from monitored cell index to row number in sheet
        actions_taken = False
        for idx, valrow in enumerate(current_values):
            val = valrow[0] if valrow else ""
            val_lc = str(val).strip().lower()
            row_num = start_row + idx
            if val_lc in ("o", "x", "-"):
                log.info(f"Action '{val_lc.upper()}' detected at row {row_num}")
                # Calculate E:I range for this row
                ei_range = f"{sheet_name}!E{row_num}:I{row_num}"
                # Get E:I values for this row (may be missing)
                ei_values = fetch_sheet_values(service, spreadsheet_id, ei_range)
                ei_values = (
                    ei_values[0] if ei_values and len(ei_values) > 0 else [""] * 5
                )
                if val_lc == "o":
                    log.info(
                        f"Copying E:I data from row {row_num} to '{TARGET_SHEET_NAME}' sheet"
                    )
                    # Copy E:I data to TARGET_SHEET_NAME (append as new row)
                    # Fetch current rows in target sheet to find append row
                    target_range = f"{TARGET_SHEET_NAME}!A1:Z"
                    target_values = fetch_sheet_values(
                        service, spreadsheet_id, target_range
                    )
                    append_row = len(target_values) + 1 if target_values else 1
                    log.debug(f"Appending to {TARGET_SHEET_NAME} at row {append_row}")
                    # Write ei_values to target sheet at append_row
                    log.debug(
                        f"Writing values to {TARGET_SHEET_NAME}!A{append_row}:E{append_row}: {ei_values}"
                    )
                    service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=f"{TARGET_SHEET_NAME}!A{append_row}:E{append_row}",
                        valueInputOption="RAW",
                        body={"values": [ei_values]},
                    ).execute()
                    log.debug(f"Deleting row {row_num} from main sheet '{sheet_name}'")
                    # Delete the row in main sheet (row_num)
                    service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={
                            "requests": [
                                {
                                    "deleteDimension": {
                                        "range": {
                                            "sheetId": sheets.get_sheet_id(
                                                service, spreadsheet_id, sheet_name
                                            ),
                                            "dimension": "ROWS",
                                            "startIndex": row_num - 1,
                                            "endIndex": row_num,
                                        }
                                    }
                                }
                            ]
                        },
                    ).execute()
                    actions_taken = True
                elif val_lc == "x":
                    log.info(f"Deleting row {row_num} from main sheet '{sheet_name}'")
                    # Just delete the row in main sheet (row_num)
                    service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={
                            "requests": [
                                {
                                    "deleteDimension": {
                                        "range": {
                                            "sheetId": sheets.get_sheet_id(
                                                service, spreadsheet_id, sheet_name
                                            ),
                                            "dimension": "ROWS",
                                            "startIndex": row_num - 1,
                                            "endIndex": row_num,
                                        }
                                    }
                                }
                            ]
                        },
                    ).execute()
                    actions_taken = True
                elif val_lc == "-":
                    log.info(
                        f"Moving E:I data from row {row_num} to row 6 and clearing original"
                    )
                    # Move E:I data to row 6 (top active row), clear original
                    # Write ei_values to E6:I6
                    log.debug(f"Writing values to {sheet_name}!E6:I6: {ei_values}")
                    service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=f"{sheet_name}!E6:I6",
                        valueInputOption="RAW",
                        body={"values": [ei_values]},
                    ).execute()
                    log.debug(f"Clearing E:I in original row {row_num}")
                    # Clear E:I in original row
                    service.spreadsheets().values().clear(
                        spreadsheetId=spreadsheet_id, range=ei_range, body={}
                    ).execute()
                    actions_taken = True
        # After all actions, repack non-empty rows upward (preserving order)
        if actions_taken:
            log.info("Repacking rows after processing actions")
            # Fetch all data again (A6:I)
            repack_range = f"{sheet_name}!A6:I"
            repack_data = fetch_sheet_values(service, spreadsheet_id, repack_range)
            # Only keep rows with any non-empty value in E:I
            non_empty_rows = [
                row for row in repack_data if any(cell for cell in row[4:9] if cell)
            ]
            log.debug(f"Non-empty rows before repacking: {len(non_empty_rows)}")
            # Pad to original number of rows (optional)
            max_rows = len(repack_data)
            while len(non_empty_rows) < max_rows:
                non_empty_rows.append([""] * 9)
            log.debug(
                f"Writing {len(non_empty_rows)} rows back to {sheet_name}!A6:I after repacking"
            )
            # Write back packed rows to A6:I
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=repack_range,
                valueInputOption="RAW",
                body={"values": non_empty_rows},
            ).execute()
            log.info("Repacking complete.")
        else:
            log.info("No actions taken; skipping repacking.")
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
    previous_values: Optional[List[List[Any]]] = None

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
            if detect_changes(previous_values, current_values):
                process_changes(service, spreadsheet_id, monitor_range, current_values)
            else:
                log.debug("No changes detected in current poll iteration")
            previous_values = current_values
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
    SHEET_ID = "1SvAEJ_fIk3BWJ6MGvPEqfSmq2a3WS7u16jFpxd5pAZ4"
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
