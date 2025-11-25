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
    Manage in-memory data storage for all relevant sheet sections.
    Provides methods to load data from Google Sheets, mark dirty sections,
    sync updates back, and visualize the current state.
    """

    def __init__(self):
        # Initialize storage for all sections with their ranges and column counts
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
            "reports": {
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
        # Track sections with unsynced changes
        self.dirty_sections = set()

    def load_from_sheets(self, service: Any, spreadsheet_id: str) -> None:
        """
        Enhanced loader — batch loads all sections, pads, and cleans queues consistently.

        Args:
            service: Google Sheets API service instance.
            spreadsheet_id: ID of the spreadsheet to load from.
        """
        log.info("✅ INFO: Loading spreadsheet state from Google Sheets (enhanced)...")

        section_order = list(self.sections.keys())
        ranges = [self.sections[name]["range"] for name in section_order]

        try:
            result = (
                service.spreadsheets()
                .values()
                .batchGet(spreadsheetId=spreadsheet_id, ranges=ranges)
                .execute()
            )
            value_ranges = result.get("valueRanges", [])
        except Exception as e:
            log.error(
                f"❌ ERROR: Failed batchGet in load_from_sheets: {e}", exc_info=True
            )
            return

        for idx, name in enumerate(section_order):
            section = self.sections[name]
            expected_cols = section["cols"]

            values = (
                value_ranges[idx].get("values", []) if idx < len(value_ranges) else []
            )

            # Pad rows to ensure fixed width
            padded = [row + [""] * (expected_cols - len(row)) for row in values]

            # Apply queue cleanup for queue sections
            if name in ["current_queue", "priority_queue", "non_priority_queue"]:
                cleaned = helpers.clean_and_compact_queue(padded, name)
                section["data"] = cleaned
            else:
                section["data"] = padded

        log.info("✅ INFO: Enhanced state loaded and normalized successfully.")

    def sync_to_sheets(self, service: Any, spreadsheet_id: str) -> None:
        """
        Sync all dirty sections' data back to Google Sheets.

        Args:
            service: Google Sheets API service instance.
            spreadsheet_id: ID of the spreadsheet to update.
        """
        for name in list(self.dirty_sections):
            section = self.sections.get(name)
            if not section:
                log.warning(f"⚠️ WARNING: Unknown section '{name}' cannot be synced.")
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
                log.debug(
                    f"✅ INFO: Synced section '{name}' to range '{rng}' with {len(data)} rows."
                )
            except Exception as e:
                log.error(
                    f"❌ ERROR: Failed syncing section '{name}': {e}", exc_info=True
                )
            finally:
                self.dirty_sections.discard(name)
            # Audit queues after each sync to detect gaps or issues
            helpers.audit_queues(self)

    def mark_dirty(self, section_name: str) -> None:
        """
        Mark a section as dirty indicating it needs to be synced.

        Args:
            section_name: Name of the section to mark dirty.
        """
        if section_name in self.sections:
            self.dirty_sections.add(section_name)
            log.debug(
                f"🧩 DEBUG: Marked section '{section_name}' as dirty for syncing."
            )
        else:
            log.warning(
                f"⚠️ WARNING: Attempted to mark unknown section '{section_name}' as dirty."
            )

    def visualize(self) -> None:
        """
        Print a formatted representation of all in-memory data to the console.
        """
        for name, section in self.sections.items():
            rng = section["range"]
            data = section["data"]
            title = f"=== {name.replace('_', ' ').upper()} ==="
            log.debug(f"🧩 DEBUG: {title}")
            log.debug(f"🧩 DEBUG: Range: {rng}")
            if not data:
                log.debug("🧩 DEBUG: No data available.")
                continue
            # Determine max width per column for alignment
            num_cols = max(len(row) for row in data)
            col_widths = [0] * num_cols
            for row in data:
                for i in range(num_cols):
                    cell = str(row[i]) if i < len(row) else ""
                    col_widths[i] = max(col_widths[i], len(cell))
            # Print header row
            header_row = "Row | " + " | ".join(f"C{i+1}" for i in range(num_cols))
            sep_row = "----|" + "|".join(
                "-" * (col_widths[i] + 2) for i in range(num_cols)
            )
            log.debug(header_row)
            log.debug(sep_row)
            for idx, row in enumerate(data):
                rownum = idx + 1
                row_cells = []
                for i in range(num_cols):
                    cell = str(row[i]) if i < len(row) else ""
                    row_cells.append(cell.ljust(col_widths[i]))
                log.debug(f"{rownum:<4}| " + " | ".join(row_cells))
            log.debug("")  # Blank line between sections

    def diff(self, other: "SpreadsheetState") -> dict[str, bool]:
        """
        Compare this state with another SpreadsheetState instance.

        Args:
            other: Another SpreadsheetState instance to compare against.

        Returns:
            Dict mapping section names to True if data differs, False otherwise.
        """
        differences = {}
        for name in self.sections:
            self_data = self.sections[name]["data"]
            other_data = other.sections.get(name, {}).get("data", [])
            differences[name] = self_data != other_data
        return differences

    def clone(self) -> "SpreadsheetState":
        """
        Create a deep copy of this SpreadsheetState instance.

        Returns:
            A new SpreadsheetState instance with copied data.
        """
        new_state = SpreadsheetState()
        new_state.sections = copy.deepcopy(self.sections)
        new_state.dirty_sections = self.dirty_sections.copy()
        return new_state
