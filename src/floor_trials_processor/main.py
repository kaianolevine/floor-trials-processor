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
from datetime import datetime, timedelta, timezone

from kaiano_common_utils import google_sheets as sheets
from kaiano_common_utils import logger as log

import floor_trials_processor.config as config
import floor_trials_processor.helpers as helpers
import floor_trials_processor.processing as processing
import floor_trials_processor.timing as timing
from floor_trials_processor.state import SpreadsheetState


def run_watcher(
    spreadsheet_id: str,
    interval_seconds: int,
    duration_minutes: int,
    monitor_range: str,
    current_utc_cell: str,
):
    """Run watcher loop, polling sheet and processing changes until duration or stop signal."""
    log.info("✅ Watcher starting (UTC-based).")
    service = sheets.get_sheets_service()

    floor_trial_end_buffer_mins = config.FLOOR_END_BUFFER_MIN

    # Retrieve floor trial timing (open, start, end)
    times = helpers.get_floor_trial_times(service, spreadsheet_id)
    dt_open = times.get("open")
    dt_start = times.get("start")
    dt_end = times.get("end")

    st = SpreadsheetState()
    st.load_from_sheets(service, spreadsheet_id)
    st.visualize()

    start_time = datetime.now(timezone.utc)
    max_end_time = start_time + timedelta(minutes=duration_minutes)
    iteration = 0
    last_sync_time = time.time()

    service = sheets.get_sheets_service()

    timing.verify_utc_timing(service, spreadsheet_id)
    utc_now_str_iter = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    helpers.write_sheet_value(
        service, spreadsheet_id, current_utc_cell, utc_now_str_iter
    )
    log.debug(
        f"🧩 DEBUG: Heartbeat updated at {current_utc_cell} -> {utc_now_str_iter}"
    )

    if not timing.should_start_run(service, spreadsheet_id):
        log.info("⚠️ WARNING: No active or upcoming floor trial; stopping run.")
        return

    while datetime.now(timezone.utc) < max_end_time:

        # Time-window gating
        now_utc = datetime.now(timezone.utc)
        # Stop if past end time
        if dt_end and now_utc > (
            dt_end + timedelta(minutes=floor_trial_end_buffer_mins)
        ):
            log.info(
                f"⛔ Current time {now_utc} is past floor trial END time + buffer ({dt_end} + {floor_trial_end_buffer_mins}min); stopping watcher."
            )
            break

        # Only begin running if within 1 hour before open time, or anytime after start time
        should_run = True
        one_hour_before_open = None
        if dt_open:
            one_hour_before_open = dt_open - timedelta(hours=1)  # TODO config

        if one_hour_before_open and now_utc < one_hour_before_open:
            should_run = False
        if (
            dt_start
            and now_utc < dt_start
            and (not one_hour_before_open or now_utc < one_hour_before_open)
        ):
            should_run = False

        if not should_run:
            log.info(
                f"⏸️ INFO: Outside floor trial window — "
                f"now={now_utc}, open={dt_open}, start={dt_start}, end={dt_end}; skipping this iteration."
            )
            break

        if not timing.isRunning(service, spreadsheet_id):
            log.info(
                "⚠️ WARNING: Automation control indicates stop; exiting watcher loop."
            )
            break

        iteration += 1
        log.debug(f"🧩 DEBUG: Poll iteration {iteration} start.")
        poll_start = time.time()

        try:
            in_progress = helpers.update_floor_trial_status(service, spreadsheet_id)

            processing.process_raw_submissions_in_memory(st)
            processing.import_external_submissions(
                service, st, config.EXTERNAL_SHEET_ID
            )

            if in_progress:
                processing.fill_current_from_queues(service, spreadsheet_id, st)
            else:
                log.info(
                    "⚠️ WARNING: Floor Trial not in progress; skipping current queue population."
                )

        except Exception as e:
            log.error(f"❌ ERROR: Exception during polling: {e}", exc_info=True)

        elapsed = time.time() - poll_start
        sleep_time = max(0, interval_seconds - elapsed)

        st.visualize()

        now = time.time()
        if now - last_sync_time >= config.SYNC_INTERVAL_SECONDS:
            log.info(
                f"✅ INFO: Periodic sync — writing state to Sheets (every {config.SYNC_INTERVAL_SECONDS}s)."
            )

            try:
                action_values = helpers.fetch_sheet_values(
                    service, spreadsheet_id, monitor_range
                )
                any_nonempty = any(
                    row and str(row[0]).strip() != "" for row in action_values
                )

                if any_nonempty:
                    log.debug(
                        "🧩 DEBUG: Detected non-empty monitored cells; processing changes."
                    )
                else:
                    log.debug(
                        "🧩 DEBUG: No changes detected in monitored cells this iteration."
                    )

                processing.process_actions(
                    service=service,
                    spreadsheet_id=spreadsheet_id,
                    monitor_range=monitor_range,
                    current_values=action_values,
                    state=st,
                )

                st.sync_to_sheets(service, spreadsheet_id)

                try:
                    utc_now_str = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S UTC"
                    )
                    helpers.write_sheet_value(
                        service, spreadsheet_id, current_utc_cell, utc_now_str
                    )
                    log.info(
                        f"✅ INFO: Updated {current_utc_cell} with UTC timestamp {utc_now_str}."
                    )
                except Exception as e:
                    log.error(
                        f"❌ ERROR: Failed to update timestamp cell: {e}", exc_info=True
                    )

                last_sync_time = now

            except Exception as e:
                log.error(f"❌ ERROR: Periodic sync failed: {e}", exc_info=True)

        elapsed_hours = (datetime.now(timezone.utc) - start_time).total_seconds() / 3600
        if elapsed_hours >= config.MAX_RUNTIME_HOURS:
            log.info(
                f"✅ INFO: Max runtime of {config.MAX_RUNTIME_HOURS} hours reached; exiting."
            )
            break

        log.debug(
            f"🧩 DEBUG: Poll iteration {iteration} took {helpers.format_duration(elapsed)}; "
            f"sleeping {helpers.format_duration(sleep_time)}."
        )

        time.sleep(sleep_time)
        log.debug(f"🧩 DEBUG: Poll iteration {iteration} end.")

    st.sync_to_sheets(service, spreadsheet_id)

    try:
        utc_now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        helpers.write_sheet_value(
            service, spreadsheet_id, current_utc_cell, utc_now_str
        )
        log.info(
            f"✅ INFO: Final update of {current_utc_cell} with UTC timestamp {utc_now_str}."
        )
    except Exception as e:
        log.error(
            f"❌ ERROR: Failed final update of timestamp cell: {e}", exc_info=True
        )

    log.info("✅ INFO: Watcher finished — runtime limit reached or stopped.")


def main():
    """Main function to start the floor trials processor watcher."""

    sheet_id = config.SHEET_ID
    interval_seconds = config.INTERVAL_SECONDS
    duration_minutes = config.DURATION_MINUTES
    monitor_range = config.MONITOR_RANGE
    current_utc_cell = config.CURRENT_UTC_CELL

    log.info("✅ Starting main function.")
    log.info(f"✅ Configuration — SHEET_ID={sheet_id}")
    log.info(f"✅ Configuration — INTERVAL_SECONDS={interval_seconds}")
    log.info(f"✅ Configuration — DURATION_MINUTES={duration_minutes}")
    log.info(f"✅ Configuration — MONITOR_RANGE={monitor_range}")
    log.info(f"✅ Configuration — CURRENT_UTC_CELL={current_utc_cell}")

    run_watcher(
        sheet_id, interval_seconds, duration_minutes, monitor_range, current_utc_cell
    )

    log.info("✅ Main function complete.")


if __name__ == "__main__":
    main()
