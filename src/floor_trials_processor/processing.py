import copy
import re
from datetime import datetime, timezone
from typing import Any, List, Optional

from kaiano_common_utils import google_sheets as sheets
from kaiano_common_utils import logger as log

import floor_trials_processor.config as config
import floor_trials_processor.helpers as helpers
import floor_trials_processor.state as state
from floor_trials_processor.helpers import names_match
from floor_trials_processor.state import SpreadsheetState


def normalize_row_length(row: List[str], length: int = 5) -> List[str]:
    """
    Ensure a row is exactly `length` cells, filling with "" or truncating as needed.
    """
    row = list(row)
    if len(row) < length:
        row = row + [""] * (length - len(row))
    elif len(row) > length:
        row = row[:length]
    return row


def compact_queue(data: List[List[str]], columns: int = 5) -> List[List[str]]:
    """
    Remove empty rows, compact non-empty upward, pad to maintain total length.
    Each row is normalized to `columns` cells.
    """
    total = len(data)
    normalized = [normalize_row_length(row, columns) for row in data]
    non_empty = [row for row in normalized if any(str(cell).strip() for cell in row)]
    while len(non_empty) < total:
        non_empty.append([""] * columns)
    return non_empty


def import_external_submissions(
    service, state: state.SpreadsheetState, source_spreadsheet_id: str
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
            .get(
                spreadsheetId=source_spreadsheet_id, range=config.EXTERNAL_SOURCE_RANGE
            )
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
                timestamp_val = datetime.now(timezone.utc).strftime(
                    "%m/%d/%Y %H:%M:%S UTC"
                )

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
    service,
    spreadsheet_id: str,
    monitor_range: str,
    current_values: List[List[Any]],
    state: "SpreadsheetState",
):
    """
    Process action commands from monitored range in the Google Sheet and update in-memory state.
    This function reads the monitored cells (usually "Current!C6:C11"), interprets the actions ("O", "X", "-"),
    and:
      - Updates the in-memory queue/history/report sections of the SpreadsheetState accordingly.
      - Uses the in-memory state as the source of truth for the queue.
      - After all logic is applied in memory, pushes the updated queue and history changes back to the sheet.
      - Uses helpers.write_sheet_value with value_input_option="RAW" for all writes.
      - Marks updated state sections dirty after in-memory mutations.
      - After O-actions, reconciles the report run counts using update_report_from_history_in_memory(state).

    Args:
        service: Google Sheets API service instance.
        spreadsheet_id: The ID of the spreadsheet.
        monitor_range: The range string (e.g. "Current!C6:C11") being monitored for actions.
        current_values: The current values read from the monitor_range (list of lists, 1 cell per row).
        state: The SpreadsheetState object for in-memory helpers.
    """
    log.info("Detected change — starting processing ...")

    # --- Range Parsing ---
    def parse_range(range_str: str):
        """Parse a range string like 'Sheet!C6:C11' or 'Sheet!AA10:AB15'.
        Returns (sheet, start_col, start_row, end_col, end_row), all as strings except rows as ints.
        Supports multi-letter columns.
        """
        m = re.match(r"([^!]+)!([A-Z]+)(\d+):([A-Z]+)(\d+)", range_str, re.I)
        if not m:
            # Try single-cell "Sheet!C6"
            m2 = re.match(r"([^!]+)!([A-Z]+)(\d+)", range_str, re.I)
            if m2:
                return (
                    m2.group(1),
                    m2.group(2).upper(),
                    int(m2.group(3)),
                    m2.group(2).upper(),
                    int(m2.group(3)),
                )
            return None
        return (
            m.group(1),
            m.group(2).upper(),
            int(m.group(3)),
            m.group(4).upper(),
            int(m.group(5)),
        )

    # --- Range Constants from config ---
    (
        monitor_sheet,
        monitor_start_col,
        monitor_start_row,
        monitor_end_col,
        monitor_end_row,
    ) = (None, None, None, None, None)
    parsed = parse_range(monitor_range)
    if not parsed:
        log.error(f"Unable to parse monitor_range: {monitor_range}")
        return
    (
        monitor_sheet,
        monitor_start_col,
        monitor_start_row,
        monitor_end_col,
        monitor_end_row,
    ) = parsed
    log.debug(
        f"Processing changes on sheet '{monitor_sheet}', rows {monitor_start_row} to {monitor_end_row}, columns {monitor_start_col}-{monitor_end_col}"
    )

    # Use config for allowed ranges
    allowed_monitor_col = getattr(config, "MONITOR_COL", "C")
    allowed_monitor_rows = (
        range(int(config.MONITOR_ROW_START), int(config.MONITOR_ROW_END) + 1)
        if hasattr(config, "MONITOR_ROW_START") and hasattr(config, "MONITOR_ROW_END")
        else range(6, 12)
    )
    allowed_data_cols = getattr(config, "DATA_COLS", ["E", "F", "G", "H", "I"])
    allowed_data_rows = range(
        int(getattr(config, "DATA_ROW_START", 5)),
        int(getattr(config, "DATA_ROW_END", 12)) + 1,
    )
    compaction_range = getattr(config, "COMPACTION_RANGE", "Current!E5:I12")
    history_sheet = getattr(config, "HISTORY_SHEET_NAME", "History")

    # --- Range Validation Helper ---
    def col_to_index(col: str) -> int:
        """Convert column letters (e.g. 'A', 'AA') to 0-based index."""
        col = col.upper()
        idx = 0
        for c in col:
            idx = idx * 26 + (ord(c) - ord("A") + 1)
        return idx - 1

    def can_modify_range(range_str: str) -> bool:
        """Check if a range is allowed for modification."""
        parsed_inner = parse_range(range_str)
        if not parsed_inner:
            return False
        sheet, start_col, start_row, end_col, end_row = parsed_inner
        if sheet != "Current":
            return True
        for col_idx in range(col_to_index(start_col), col_to_index(end_col) + 1):
            col_letter = ""
            n = col_idx + 1
            # Convert index to letters
            while n > 0:
                n, rem = divmod(n - 1, 26)
                col_letter = chr(rem + ord("A")) + col_letter
            for row in range(start_row, end_row + 1):
                if not (
                    (col_letter == allowed_monitor_col and row in allowed_monitor_rows)
                    or (col_letter in allowed_data_cols and row in allowed_data_rows)
                ):
                    return False
        return True

    # --- Sheet Existence ---
    sheets.ensure_sheet_exists(service, spreadsheet_id, history_sheet)

    # --- Unified In-Memory Action Processing ---
    # In-memory: operate on state.sections["current_queue"] and state.sections["history"]
    current_queue = state.sections["current_queue"]["data"]
    history = state.sections["history"]["data"]
    report_data = state.sections["reports"]["data"]

    # Normalize current_queue/history/report_data shapes
    while len(current_queue) < 6:
        current_queue.append([""] * 5)
    for idx in range(len(current_queue)):
        current_queue[idx] = normalize_row_length(current_queue[idx])
    for i in range(len(history)):
        history[i] = normalize_row_length(history[i])
    for i in range(len(report_data)):
        report_data[i] = normalize_row_length(report_data[i])

    # Prepare for deferred "-" rows and newly created history rows
    deferred_minus_rows: List[List[str]] = []
    history_new_rows: List[List[str]] = []

    # Prepare to clear processed actions in sheet (but only write at the end)
    cleared_action_values = copy.deepcopy(current_values)

    # Main action loop: process each action (C6:C11 maps to current_queue rows 0-5)
    for idx, row in enumerate(current_values):
        action = row[0].strip() if row and len(row) > 0 else ""
        action_lc = action.lower()
        row_num = monitor_start_row + idx

        if action_lc == "o":
            # Move row to history (in memory) and clear from current_queue
            queue_row = current_queue[idx]
            if any(str(cell).strip() for cell in queue_row):
                timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                new_history_row = normalize_row_length(
                    [timestamp, queue_row[1], queue_row[2], queue_row[3]]
                )
                history.append(new_history_row)
                history_new_rows.append(new_history_row)

                # ⬇️ This is the call you asked for
                increment_report_from_history_rows(history_new_rows, state)

                log.debug(
                    f"O-action debug: Updated reports after new history row. "
                    f"history_new_rows_count={len(history_new_rows)}, "
                    f"total_report_rows={len(state.sections['reports']['data'])}"
                )
                log.info(
                    f"process_actions: 'O' at row {row_num} – moved to history in-memory: {new_history_row}"
                )
            current_queue[idx] = [""] * 5
            cleared_action_values[idx][0] = ""

        elif action_lc == "x":
            # Clear the row in current_queue (in memory only)
            current_queue[idx] = [""] * 5
            cleared_action_values[idx][0] = ""
            log.info(
                f"process_actions: 'X' at row {row_num} – cleared current queue row in memory."
            )

        elif action_lc == "-":
            queue_row = copy.deepcopy(current_queue[idx])
            if any(str(cell).strip() for cell in queue_row):
                deferred_minus_rows.append(normalize_row_length(queue_row))
            current_queue[idx] = [""] * 5
            cleared_action_values[idx][0] = ""

        elif action_lc != "":
            # Unrecognized: clear monitored cell later when we write cleared_action_values
            cleared_action_values[idx][0] = ""
            log.warning(
                f"process_actions: Unrecognized action '{action}' at row {row_num}; clearing command only."
            )
        # else: empty cell, nothing to do

    # --- In-Memory: Rebuild current queue and reinsert deferred "-" rows ---
    # Take all non-empty rows in their existing order (after O/X/- cleared some),
    # then append any deferred "-" rows to the bottom. Finally, trim/pad to 6 rows.
    non_empty_rows: List[List[str]] = [
        normalize_row_length(row, 5)
        for row in current_queue
        if any(str(cell).strip() for cell in row)
    ]
    compacted = non_empty_rows + deferred_minus_rows
    compacted = [normalize_row_length(row, 5) for row in compacted]
    if len(compacted) > 6:
        compacted = compacted[:6]
    while len(compacted) < 6:
        compacted.append([""] * 5)

    state.sections["current_queue"]["data"] = compacted
    state.sections["history"]["data"] = history
    state.mark_dirty("current_queue")
    state.mark_dirty("history")

    # --- Sheet: Write current queue snapshot from memory into compaction_range ---
    compaction_data = [normalize_row_length(row, 5) for row in compacted]
    total_rows = len(compaction_data)
    if can_modify_range(compaction_range):
        comp_sheet, comp_start_col, comp_start_row, comp_end_col, comp_end_row = (
            parse_range(compaction_range)
        )
        target_range = f"{comp_sheet}!{comp_start_col}{comp_start_row}:{comp_end_col}{comp_start_row + total_rows - 1}"
        helpers.write_sheet_value(
            service,
            spreadsheet_id,
            target_range,
            compaction_data,
            value_input_option="RAW",
        )
        log.info(
            f"process_actions: Wrote {total_rows} rows of current queue to {target_range} from in-memory state."
        )
    else:
        log.warning(
            f"process_actions: Compaction range {compaction_range} is not allowed to modify; skipping sheet queue update."
        )

    # --- Sheet: Append new history rows based on in-memory history_new_rows ---
    if history_new_rows:
        history_range = f"{history_sheet}!A6:E"
        existing_history = helpers.fetch_sheet_values(
            service, spreadsheet_id, history_range
        )
        append_row = 6 + len(existing_history) if existing_history else 6
        end_append_row = append_row + len(history_new_rows) - 1
        target_history_range = f"{history_sheet}!A{append_row}:E{end_append_row}"
        helpers.write_sheet_value(
            service,
            spreadsheet_id,
            target_history_range,
            history_new_rows,
            value_input_option="RAW",
        )
        log.info(
            f"process_actions: Appended {len(history_new_rows)} new history row(s) to {target_history_range}."
        )
    else:
        log.info("process_actions: No 'O' actions – no new history rows to append.")

    # --- Write cleared actions back to Google Sheets after processing (always RAW) ---
    helpers.write_sheet_value(
        service,
        spreadsheet_id,
        monitor_range,
        cleared_action_values,
        value_input_option="RAW",
    )
    log.info("Cleared processed commands from monitored action range after handling.")

    log.info("Processing complete.")


# ---------------------------------------------------------------------
# New queue processing functions
# ---------------------------------------------------------------------
def process_priority(service, spreadsheet_id, state):
    """
    Read from in-memory Priority queue, find first non-empty row, log and clear, return as 5-cell list or None.
    """
    section_name = "priority_queue"
    values: List[List[str]] = state.sections[section_name]["data"]
    taken_row = None
    values = [normalize_row_length(row) for row in values]
    for idx, row in enumerate(values):
        if any(str(cell).strip() for cell in row):
            log.info(f"process_priority: Taking row {idx+3} from Priority queue: {row}")
            taken_row = row
            values[idx] = [""] * 5
            break
    if taken_row is not None:
        compacted = compact_queue(values)
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
    values: List[List[str]] = state.sections[section_name]["data"]
    taken_row = None
    values = [normalize_row_length(row) for row in values]
    for idx, row in enumerate(values):
        if any(str(cell).strip() for cell in row):
            log.info(
                f"process_non_priority: Taking row {idx+3} from NonPriority queue: {row}"
            )
            taken_row = row
            values[idx] = [""] * 5
            break
    if taken_row is not None:
        compacted = compact_queue(values)
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
    current_data: List[List[str]] = current_section["data"]
    while len(current_data) < 6:
        current_data.append([""] * 5)
    for idx in range(len(current_data)):
        current_data[idx] = normalize_row_length(current_data[idx])

    # Helper: filled row is all 5 columns non-empty (or any non-empty cell in all 5 columns)
    def is_filled(row: List[str]) -> bool:
        return any(str(cell).strip() for cell in row[:5])

    changes_made = False
    for idx, row in enumerate(current_data):
        row_num = 6 + idx
        if is_filled(row):
            continue
        # Normalize priority_queue rows to 5 columns
        pq: List[List[str]] = state.sections["priority_queue"]["data"]
        pq_rows = [normalize_row_length(r) for r in pq]
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
            compacted = compact_queue(pq_rows)
            state.sections["priority_queue"]["data"] = compacted
            state.mark_dirty("priority_queue")
            log.info(
                f"fill_current_from_queues: Compacted Priority queue — {len([r for r in compacted if any(str(cell).strip() for cell in r)])} non-empty, {len([r for r in compacted if not any(str(cell).strip() for cell in r)])} empty rows"
            )
            current_data[idx] = normalize_row_length(taken_priority)
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
        npq: List[List[str]] = state.sections["non_priority_queue"]["data"]
        npq_rows = [normalize_row_length(r) for r in npq]
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
            compacted = compact_queue(npq_rows)
            state.sections["non_priority_queue"]["data"] = compacted
            state.mark_dirty("non_priority_queue")
            log.info(
                f"fill_current_from_queues: Compacted NonPriority queue — {len([r for r in compacted if any(str(cell).strip() for cell in r)])} non-empty, {len([r for r in compacted if not any(str(cell).strip() for cell in r)])} empty rows"
            )
            current_data[idx] = normalize_row_length(taken_nonpriority)
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
def process_raw_submissions_in_memory(state: state.SpreadsheetState):
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
            rows = helpers.fetch_sheet_values(service, spreadsheet_id, range_)
            return rows[0][0] if rows and rows[0] else ""
        except Exception as e:
            log.warning(f"Error getting value from {range_}: {e}")
            return ""

    open_time = get_value(service, config.SHEET_ID, "Current!B15")
    # start_time = get_value(service, config.SHEET_ID, "Current!B16")
    end_time = get_value(service, config.SHEET_ID, "Current!B17")

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
    # trial_start = parse_utc_string(start_time)
    trial_end = parse_utc_string(end_time)

    raw_data: List[List[str]] = state.sections["raw_submissions"]["data"]
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

    priority_data: List[List[str]] = state.sections["priority_queue"]["data"]
    nonpriority_data: List[List[str]] = state.sections["non_priority_queue"]["data"]
    report_data: List[List[str]] = state.sections["reports"]["data"]
    rejected_data: List[List[str]] = state.sections["rejected_submissions"]["data"]

    # Normalize all rows to 6 columns
    raw_data = [normalize_row_length(r, 6) for r in raw_data]

    processed = 0
    for row in list(raw_data):  # iterate over a copy
        if not any(str(c).strip() for c in row):
            continue

        timestamp, leader, follower, division, has_cue, cue_desc = row
        division = str(division).strip()

        # --- Parse submission timestamp and check if within window ---
        try:
            submission_time = helpers.parse_utc_datetime(timestamp)
        except Exception as e:
            log.warning(f"Could not parse submission time '{timestamp}': {e}")
            continue
        if (
            trial_open
            and trial_end
            and (submission_time < trial_open or submission_time > trial_end)
        ):
            log.warning(
                f"Outside accepted submission window, submission_time:{submission_time}, trial_open:{trial_open}, trial_end:{trial_end}"
            )
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
        target_queue[:] = compact_queue(target_queue)
        target_queue.append(
            normalize_row_length(["", leader, follower, division, cue_desc])
        )
        while len(target_queue) < 20:
            target_queue.append([""] * 5)
        target_queue[:] = compact_queue(target_queue)

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
    state.sections["priority_queue"]["data"] = compact_queue(priority_data)
    state.sections["non_priority_queue"]["data"] = compact_queue(nonpriority_data)
    state.sections["reports"]["data"] = report_data
    state.sections["rejected_submissions"]["data"] = rejected_data

    # Mark all affected sections dirty
    for sec in [
        "raw_submissions",
        "priority_queue",
        "non_priority_queue",
        "reports",
        "rejected_submissions",
    ]:
        state.mark_dirty(sec)

    log.info(f"process_raw_submissions_in_memory: Processed {processed} rows in-memory")


def increment_report_from_history_rows(
    history_new_rows: List[List[str]], state: SpreadsheetState
):
    """
    Increment run counts for history rows that have just been added.
    Assumes corresponding report entries already exist.
    Logs an error if no matching report row is found.
    """
    report = state.sections["reports"]["data"]
    updated_count = 0
    missing_count = 0

    for hist in history_new_rows:
        hist = normalize_row_length(hist, 4)
        leader = str(hist[1]).strip()
        follower = str(hist[2]).strip()
        division = str(hist[3]).strip()

        if not (leader or follower or division):
            continue

        match_found = False
        for rep in report:
            r_leader = str(rep[0]).strip()
            r_follower = str(rep[1]).strip()
            r_division = str(rep[2]).strip()

            if names_match(
                r_leader, r_follower, r_division, leader, follower, division
            ):
                try:
                    run_count = int(rep[4]) if str(rep[4]).strip() else 0
                except Exception:
                    run_count = 0

                rep[4] = str(run_count + 1)
                updated_count += 1
                match_found = True
                state.mark_dirty("reports")
                log.debug(
                    f"increment_report_from_history_rows: Incremented {leader}/{follower}/{division} -> {rep[4]}"
                )
                break

        if not match_found:
            missing_count += 1
            log.error(
                f"increment_report_from_history_rows: Report row NOT FOUND for "
                f"{leader}/{follower}/{division}! History and report OUT OF SYNC!"
            )

    log.info(
        f"increment_report_from_history_rows: Updated {updated_count} existing report entries. "
        f"Missing {missing_count} report entries."
    )
