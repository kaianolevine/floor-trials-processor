#!/usr/bin/env python3
"""
main.py — Continuous Google Sheets watcher and processor using kaiano-common-utils.

Runs for a configurable duration, polling a specified sheet range at set intervals.
Triggers processing when watched sheet values change (e.g., moving rows).

Main flow:
- Verify automation control cell to decide if watcher should run.
- Poll sheet data, process submissions and actions.
- Periodically sync in-memory state back to Sheets.
- Log progress with clear, standardized prefixes.
"""

import time
from datetime import datetime, timezone

from kaiano_common_utils import google_sheets as sheets
from kaiano_common_utils import logger as log

import floor_trials_processor.config as config
import floor_trials_processor.helpers as helpers
import floor_trials_processor.processing as processing
import floor_trials_processor.timing as timing
from floor_trials_processor.state import SpreadsheetState

STEP_INTERVALS = {
    "floor_trial_heartbeat": 55,
    "process_submissions": 60,
    "process_floor_trials": 20,
}


def run_watcher(
    spreadsheet_id: str,
    submission_sheet_id: str,
    max_priority_runs: int,
    duration_minutes: int,
    monitor_range: str,
    current_utc_cell: str,
):
    """Run watcher loop, polling sheet and processing changes until duration or stop signal."""
    log.info("✅ Watcher starting (UTC-based).")
    service = sheets.get_sheets_service()

    st = SpreadsheetState()
    st.load_from_sheets(service, spreadsheet_id)
    st.visualize()

    # Retrieve floor trial timing (open, start, end)
    times = helpers.get_floor_trial_times(service, spreadsheet_id)
    dt_open = times.get("open")
    dt_start = times.get("start")
    dt_end = times.get("end")

    start_time = datetime.now(timezone.utc)
    utc_now = start_time
    floor_trials_in_progress = False

    last_step_run = {step: 0 for step in STEP_INTERVALS}

    while timing.should_run(
        utc_now,
        dt_open,
        dt_end,
        start_time,
        duration_minutes,
        config.EARLY_WINDOW_RUNNING_BUFFER,
        config.FLOOR_END_BUFFER,
    ):

        now = time.time()
        floor_trials_in_progress = timing.floor_trial_active(utc_now, dt_open, dt_end)
        processing.process_raw_submissions_in_memory(
            st, dt_open, dt_end, max_priority_runs
        )

        if floor_trials_in_progress:
            processing.fill_current_from_queues(st)

        # Step: Floor Trial Heartbeat
        if (
            now - last_step_run["floor_trial_heartbeat"]
            >= STEP_INTERVALS["floor_trial_heartbeat"]
        ):
            helpers.update_utc_heartbeat(service, spreadsheet_id, current_utc_cell)
            helpers.update_floor_trial_status(
                service,
                spreadsheet_id,
                dt_open,
                dt_start,
                dt_end,
                utc_now,
            )
            timing.check_sheet_should_run(service, spreadsheet_id)
            log.info("✅ Updated floor trials heartbeat")
            last_step_run["floor_trial_heartbeat"] = now

        # Step: Process Submissions
        elif (
            now - last_step_run["process_submissions"]
            >= STEP_INTERVALS["process_submissions"]
        ):
            processing.import_external_submissions(service, submission_sheet_id, st)
            st.visualize()
            log.info("✅ Processed external submissions")
            last_step_run["process_submissions"] = now

        # Step: Floor Trials Processing
        elif (
            now - last_step_run["process_floor_trials"]
            >= STEP_INTERVALS["process_floor_trials"]
        ):
            if floor_trials_in_progress:
                processing.process_actions(
                    service=service,
                    spreadsheet_id=spreadsheet_id,
                    monitor_range=monitor_range,
                    state=st,
                )

            st.sync_to_sheets(service, spreadsheet_id)
            st.visualize()
            log.info("✅ Processed actions, synced sheets")
            last_step_run["process_floor_trials"] = now

        time.sleep(1)  # Sleep briefly to avoid tight loop
        utc_now = datetime.now(timezone.utc)

    st.sync_to_sheets(service, spreadsheet_id)
    helpers.update_floor_trial_status(
        service,
        spreadsheet_id,
        dt_open,
        dt_start,
        dt_end,
        datetime.now(timezone.utc),
    )
    helpers.update_utc_heartbeat(service, spreadsheet_id, current_utc_cell)

    log.info("✅ Watcher finished")


def main():
    """Main function to start the floor trials processor watcher."""

    sheet_id = config.SHEET_ID
    external_id = config.EXTERNAL_SHEET_ID
    max_priority_runs = config.MAX_RUN_COUNT_FOR_PRIORITY
    duration_minutes = config.DURATION_MINUTES
    monitor_range = config.MONITOR_RANGE
    current_utc_cell = config.CURRENT_UTC_CELL

    log.info("✅ Starting Floor Trials Processor")
    log.info(f"✅ Configuration — SHEET_ID={sheet_id}")
    log.info(f"✅ Configuration — EXTERNAL_SHEET_ID={external_id}")
    log.info(f"✅ Configuration — MAX_RUN_COUNT_FOR_PRIORITY={max_priority_runs}")
    log.info(f"✅ Configuration — DURATION_MINUTES={duration_minutes}")
    log.info(f"✅ Configuration — MONITOR_RANGE={monitor_range}")
    log.info(f"✅ Configuration — CURRENT_UTC_CELL={current_utc_cell}")

    run_watcher(
        sheet_id,
        external_id,
        max_priority_runs,
        duration_minutes,
        monitor_range,
        current_utc_cell,
    )

    log.info("✅ Main function complete.")


if __name__ == "__main__":
    main()
