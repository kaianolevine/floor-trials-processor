import copy
from typing import Any

from kaiano_common_utils import logger as log

import floor_trials_processor.config as config
import floor_trials_processor.helpers as helpers


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
                "range": config.CURRENT_QUEUE_RANGE,
                "data": [],
                "cols": 5,
            },
            "priority_queue": {
                "range": config.PRIORITY_QUEUE_RANGE,
                "data": [],
                "cols": 5,
            },
            "non_priority_queue": {
                "range": config.NON_PRIORITY_QUEUE_RANGE,
                "data": [],
                "cols": 5,
            },
            "raw_submissions": {
                "range": config.RAW_SUBMISSIONS_RANGE,
                "data": [],
                "cols": 5,
            },
            "rejected_submissions": {
                "range": config.REJECTED_SUBMISSIONS_RANGE,
                "data": [],
                "cols": 7,
            },
            "report": {
                "range": config.REPORT_RANGE,
                "data": [],
                "cols": 5,
            },
            "history": {
                "range": f"{config.HISTORY_SHEET_NAME}!A6:E",
                "data": [],
                "cols": 5,
            },
            "priority_division_map": {
                "range": config.PRIORITY_DIVISION_RANGE,
                "data": [],
                "cols": 5,
            },
            "priority_flag_map": {
                "range": config.PRIORITY_FLAG_RANGE,
                "data": [],
                "cols": 5,
            },
        }
        # Set of section names with unsynced changes
        self.dirty_sections = set()

    def load_from_sheets(self, service: Any, spreadsheet_id: str) -> None:
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

    def sync_to_sheets(self, service: Any, spreadsheet_id: str) -> None:
        """
        Pushes updates for all dirty sections to Google Sheets.
        """
        for name in list(self.dirty_sections):
            section = self.sections.get(name)
            if not section:
                log.warning(
                    f"SpreadsheetState: Unknown section '{name}' - cannot sync."
                )
                self.dirty_sections.discard(name)
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
            except Exception as e:
                log.error(
                    f"SpreadsheetState: Error syncing '{name}': {e}", exc_info=True
                )
            finally:
                self.dirty_sections.discard(name)
            # After each section sync, audit queues for ghost gaps
            helpers.audit_queues(self)

    def mark_dirty(self, section_name: str) -> None:
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

    def visualize(self) -> None:
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

    def diff(self, other: "SpreadsheetState") -> dict[str, bool]:
        """
        Compares data between self and another SpreadsheetState instance.
        Returns a dictionary mapping section names to True if they differ, False otherwise.
        """
        differences = {}
        for name in self.sections:
            self_data = self.sections[name]["data"]
            other_data = other.sections.get(name, {}).get("data", [])
            differences[name] = self_data != other_data
        return differences

    def clone(self) -> "SpreadsheetState":
        """
        Returns a deep copy of this SpreadsheetState instance.
        """
        new_state = SpreadsheetState()
        new_state.sections = copy.deepcopy(self.sections)
        new_state.dirty_sections = self.dirty_sections.copy()
        return new_state


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

    current = get_values(config.CURRENT_QUEUE_RANGE, 5)
    priority = get_values(config.PRIORITY_QUEUE_RANGE, 5)
    nonpriority = get_values(config.NON_PRIORITY_QUEUE_RANGE, 5)
    report = get_values(config.REPORT_RANGE, 5)
    rejected = get_values(config.REJECTED_SUBMISSIONS_RANGE, 7)

    log.info(
        f"Loaded state: {len(current)} current, {len(priority)} priority, "
        f"{len(nonpriority)} nonpriority, {len(report)} reports, {len(rejected)} rejected"
    )

    state = SpreadsheetState()
    state.sections["current_queue"]["data"] = helpers.clean_and_compact_queue(
        current, "current_queue"
    )
    state.sections["priority_queue"]["data"] = helpers.clean_and_compact_queue(
        priority, "priority_queue"
    )
    state.sections["non_priority_queue"]["data"] = helpers.clean_and_compact_queue(
        nonpriority, "non_priority_queue"
    )
    state.sections["report"]["data"] = report
    state.sections["rejected_submissions"]["data"] = rejected

    return state
