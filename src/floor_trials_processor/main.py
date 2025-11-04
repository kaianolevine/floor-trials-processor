#!/usr/bin/env python3
"""
main.py — Continuous Google Sheets watcher + processor utilising kaiano-common-utils.

This script will run for a configurable time, poll a configurable sheet range at a configurable interval,
and when values change in the watched sheet it will trigger processing (moving rows etc.).
"""


import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from kaiano_common_utils import google_sheets as sheets
from kaiano_common_utils import logger as log

import floor_trials_processor.config as config
import floor_trials_processor.helpers as helpers
import floor_trials_processor.processing as processing
import floor_trials_processor.state as state
import floor_trials_processor.timing as timing
from floor_trials_processor.state import SpreadsheetState


def update_floor_trial_status(service, spreadsheet_id):
    """
    Update Floor Trial status using UTC datetimes in config-defined cells.
    """
    try:
        ranges = [
            config.FLOOR_OPEN_CELL,
            config.FLOOR_START_CELL,
            config.FLOOR_END_CELL,
        ]
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

        dt_open = helpers.parse_utc_datetime(open_str)
        dt_start = helpers.parse_utc_datetime(start_str)
        dt_end = helpers.parse_utc_datetime(end_str)
        now_utc = datetime.now(timezone.utc)

        log.info(
            f"update_floor_trial_status: Open={dt_open}, Start={dt_start}, End={dt_end}, Now={now_utc}"
        )

        status = config.STATUS_NOT_ACTIVE
        if dt_start and dt_end:
            if dt_start <= now_utc <= dt_end:
                status = config.STATUS_IN_PROGRESS
            elif now_utc < dt_start:
                status = config.STATUS_OPEN
            elif now_utc > dt_end:
                status = config.STATUS_CLOSED

        # helpers.write_sheet_value must support writing a row; if not, keep original update with config.FLOOR_STATUS_RANGE
        try:
            helpers.write_sheet_value(
                service, spreadsheet_id, config.FLOOR_STATUS_RANGE, [status, "", ""]
            )
        except Exception:
            # fallback to direct update if helpers.write_sheet_value fails
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=config.FLOOR_STATUS_RANGE,
                valueInputOption="RAW",
                body={"values": [[status, "", ""]]},
            ).execute()
        log.info(f"update_floor_trial_status: Updated status to '{status}'")
        return config.STATUS_IN_PROGRESS in status
    except Exception as e:
        log.error(f"update_floor_trial_status: Exception occurred: {e}", exc_info=True)
        return False


def isRunning(service, spreadsheet_id: str) -> bool:
    # Check automation control cell value before running watcher loop
    try:
        h2_value_rows = helpers.fetch_sheet_values(
            service, spreadsheet_id, config.AUTOMATION_CONTROL_CELL
        )
        h2_val = h2_value_rows[0][0] if h2_value_rows and h2_value_rows[0] else ""
        log.info(f"Fetched {config.AUTOMATION_CONTROL_CELL} value: '{h2_val}'")
        if str(h2_val).strip().lower() != "runautomations":
            log.warning(
                f"Automation stopped: {config.AUTOMATION_CONTROL_CELL} is not RunAutomations (value was '{h2_val}')"
            )
            return False
        else:
            log.info(
                f"{config.AUTOMATION_CONTROL_CELL} is 'RunAutomations' (case-insensitive) — proceeding with watcher."
            )
        return True
    except Exception as e:
        log.error(
            f"Error fetching {config.AUTOMATION_CONTROL_CELL} value: {e}", exc_info=True
        )
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
    start_time: Optional[datetime] = None,
):
    log.info("Watcher starting (UTC-based)")
    service = sheets.get_sheets_service()

    if not isRunning(service, spreadsheet_id):
        return

    st = SpreadsheetState()
    st.load_from_sheets(service, spreadsheet_id)
    st.visualize()

    utc_end_time = datetime.now(timezone.utc) + timedelta(minutes=duration_minutes)
    if start_time is None:
        start_time = datetime.now(timezone.utc)

    log.info(
        f"Watcher starting: spreadsheet_id={spreadsheet_id}, range={sheet_range}, "
        f"interval={interval_seconds}s, duration={duration_minutes}min (UTC), monitor_range={monitor_range}"
    )
    iteration = 0
    last_sync_time = time.time()
    ACTION_RANGE = config.MONITOR_RANGE

    while datetime.now(timezone.utc) < utc_end_time:
        iteration += 1
        log.debug(f"Poll iteration {iteration} start (UTC)")
        poll_start = time.time()
        try:
            in_progress = update_floor_trial_status(service, spreadsheet_id)

            processing.import_external_submissions(
                service, st, config.EXTERNAL_SOURCE_SHEET_ID
            )

            action_values = helpers.fetch_sheet_values(
                service, spreadsheet_id, ACTION_RANGE
            )
            processing.process_actions_in_memory(st, action_values)

            processing.process_raw_submissions_in_memory(st)

            current_values = helpers.fetch_sheet_values(
                service, spreadsheet_id, monitor_range
            )
            any_nonempty = any(
                (row and str(row[0]).strip() != "") for row in current_values
            )
            if any_nonempty:
                log.debug(
                    "Detected at least one non-empty monitored cell; processing changes."
                )
            else:
                log.debug("No non-empty values in monitored cells this poll iteration.")

            if in_progress:
                processing.fill_current_from_queues(service, spreadsheet_id, st)
            else:
                log.info(
                    "⏸️ Floor Trial not in progress — skipping current queue population"
                )
        except Exception as e:
            log.error(f"Error during polling: {e}", exc_info=True)

        elapsed = time.time() - poll_start
        sleep_time = max(0, interval_seconds - elapsed)
        st.visualize()

        try:
            utc_now_str_iter = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
            helpers.write_sheet_value(
                service, spreadsheet_id, config.CURRENT_UTC_CELL, utc_now_str_iter
            )
            log.debug(
                f"Heartbeat -> {config.CURRENT_UTC_CELL} set to {utc_now_str_iter}"
            )
        except Exception as e:
            log.error(f"Failed to update Current!D2 heartbeat: {e}", exc_info=True)

        now = time.time()
        if now - last_sync_time >= config.SYNC_INTERVAL_SECONDS:
            log.info(
                f"Periodic sync: Writing in-memory state to Sheets (every {config.SYNC_INTERVAL_SECONDS}s, UTC-based)"
            )
            try:
                st.sync_to_sheets(service, spreadsheet_id)
                try:
                    utc_now_str = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S UTC"
                    )
                    helpers.write_sheet_value(
                        service, spreadsheet_id, config.CURRENT_UTC_CELL, utc_now_str
                    )
                    log.info(
                        f"Updated {config.CURRENT_UTC_CELL} with UTC timestamp {utc_now_str}"
                    )
                except Exception as e:
                    log.error(
                        f"Failed to update {config.CURRENT_UTC_CELL} timestamp: {e}",
                        exc_info=True,
                    )
                last_sync_time = now
            except Exception as e:
                log.error(f"Periodic sync failed: {e}", exc_info=True)

        # Runtime limit check
        elapsed_hours = (datetime.now(timezone.utc) - start_time).total_seconds() / 3600
        if elapsed_hours >= config.MAX_RUNTIME_HOURS:
            log.info(
                f"Reached max runtime of {config.MAX_RUNTIME_HOURS} hours — exiting gracefully."
            )
            break

        log.debug(
            f"Poll iteration {iteration} took {helpers.format_duration(elapsed)}; sleeping for {helpers.format_duration(sleep_time)} (UTC) …"
        )
        time.sleep(sleep_time)
        log.debug(f"Poll iteration {iteration} end (UTC)")

    st.sync_to_sheets(service, spreadsheet_id)
    try:
        utc_now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        helpers.write_sheet_value(
            service, spreadsheet_id, config.CURRENT_UTC_CELL, utc_now_str
        )
        log.info(f"Updated {config.CURRENT_UTC_CELL} with UTC timestamp {utc_now_str}")
    except Exception as e:
        log.error(
            f"Failed to update {config.CURRENT_UTC_CELL} timestamp: {e}", exc_info=True
        )
    log.info("Watcher finished — runtime limit reached (UTC).")


def main():
    log.info("Starting main function")
    log.info(
        f"Configuration loaded: SHEET_ID={config.SHEET_ID}, SHEET_RANGE={config.SHEET_RANGE}, INTERVAL_SECONDS={config.INTERVAL_SECONDS}, DURATION_MINUTES={config.DURATION_MINUTES}, MONITOR_RANGE={config.MONITOR_RANGE}"
    )
    service = sheets.get_sheets_service()
    # UTC verification diagnostics (if enabled)
    if config.DEBUG_UTC_MODE:
        timing.verify_utc_timing(service, config.SHEET_ID)
    # Check if we should start this run (based on next floor trial start time)
    if not timing.should_start_run(service, config.SHEET_ID):
        log.info("No active or near-future floor trial — stopping run.")
        return
    # Load state from sheets at startup
    state.load_state_from_sheets(service, config.SHEET_ID)
    # Start time for max runtime check
    start_time = datetime.now(timezone.utc)
    # Main watcher loop with runtime check inside run_watcher
    run_watcher(
        config.SHEET_ID,
        config.SHEET_RANGE,
        config.INTERVAL_SECONDS,
        config.DURATION_MINUTES,
        config.MONITOR_RANGE,
        start_time,
    )
    log.info("Main function complete")


if __name__ == "__main__":
    main()
