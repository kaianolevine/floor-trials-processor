from datetime import datetime, timedelta, timezone
from typing import Optional

from kaiano_common_utils import logger as log

import floor_trials_processor.config as config
import floor_trials_processor.helpers as helpers


# ---------------------------------------------------------------------
# Helper: Check if next floor trial is within MAX_START_DELAY_HOURS
# ---------------------------------------------------------------------
def should_start_run(service, spreadsheet_id) -> bool:
    """
    Determine if the next floor trial start time is within the allowed delay window.

    Returns False if the start time is more than MAX_START_DELAY_HOURS away.
    """
    start_str = helpers.get_single_cell(
        service, spreadsheet_id, config.FLOOR_OPEN_RANGE
    )
    dt_start = None
    if start_str:
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                dt_start = datetime.strptime(start_str.strip(), fmt).replace(
                    tzinfo=timezone.utc
                )
                break
            except Exception:
                continue

    end_str = helpers.get_single_cell(service, spreadsheet_id, config.FLOOR_END_RANGE)
    dt_end = None
    if end_str:
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                dt_end = datetime.strptime(end_str.strip(), fmt).replace(
                    tzinfo=timezone.utc
                )
                break
            except Exception:
                continue

    if not dt_start:
        log.warning(
            "⚠️ WARNING: No valid floor trial start time found — exiting gracefully."
        )
        return False

    now_utc = datetime.now(timezone.utc)

    start_within_delay = dt_start <= now_utc + timedelta(
        hours=config.MAX_START_DELAY_HOURS
    )
    end_within_runtime = dt_end is not None and dt_end <= now_utc + timedelta(
        hours=config.MAX_RUNTIME_HOURS
    )

    if start_within_delay or end_within_runtime:
        return True
    else:
        if not start_within_delay:
            log.info(
                f"✅ INFO: Floor trial starts at {dt_start} (more than {config.MAX_START_DELAY_HOURS} hours away) — exiting early."
            )
        if dt_end and not end_within_runtime:
            log.info(
                f"✅ INFO: Floor trial ends at {dt_end} (more than {config.MAX_RUNTIME_HOURS} hours away) — exiting early."
            )
        return False


def verify_utc_timing(service, sheet_id) -> Optional[dict]:
    """
    Log UTC-based diagnostics for the Floor Trial schedule and update status.

    Returns a dictionary with timing details and status, or None on error.
    """
    try:
        ranges = [
            config.FLOOR_TRIAL_DATE_CELL,
            config.FLOOR_TRIAL_START_CELL,
            config.FLOOR_TRIAL_END_CELL,
        ]
        result = (
            service.spreadsheets()
            .values()
            .batchGet(spreadsheetId=sheet_id, ranges=ranges)
            .execute()
        )
        date_val = result["valueRanges"][0].get("values", [[""]])[0][0]
        start_val = result["valueRanges"][1].get("values", [[""]])[0][0]
        end_val = result["valueRanges"][2].get("values", [[""]])[0][0]

        dt_start = parse_trial_datetime(date_val, start_val)
        dt_end = parse_trial_datetime(date_val, end_val)
        now_utc = datetime.now(timezone.utc)

        log.info("✅ INFO: === UTC Verification — Floor Trial Timing ===")
        log.info(f"✅ INFO: Trial Date (D15): {date_val}")
        log.info(f"✅ INFO: Start Time (C17): {start_val}")
        log.info(f"✅ INFO: End Time (D17):   {end_val}")

        if dt_start is None:
            log.warning(f"⚠️ WARNING: Invalid start time: '{start_val}'")
        else:
            log.info(f"✅ INFO: Parsed UTC Start: {dt_start}")

        if dt_end is None:
            log.warning(f"⚠️ WARNING: Invalid end time: '{end_val}'")
        else:
            log.info(f"✅ INFO: Parsed UTC End:   {dt_end}")

        log.info(f"✅ INFO: Current UTC Now:  {now_utc}")

        status = "unknown"
        if dt_start and dt_end:
            if dt_start <= now_utc <= dt_end:
                log.info("✅ INFO: Floor Trial is IN PROGRESS (UTC)")
                status = "in_progress"
            elif now_utc < dt_start:
                log.info("⏳ INFO: Floor Trial has NOT STARTED yet (UTC)")
                status = "not_started"
            else:
                log.info("🏁 INFO: Floor Trial is FINISHED (UTC)")
                status = "finished"
        else:
            log.warning(
                "⚠️ WARNING: Could not parse trial date/time — check sheet values"
            )

        helpers.update_floor_trial_status(service, sheet_id)
        log.info("✅ INFO: UTC Verification complete — proceeding to queue processing")

        return {
            "date": date_val,
            "start": start_val,
            "end": end_val,
            "now": now_utc.isoformat(),
            "status": status,
        }
    except Exception as e:
        log.error(f"❌ ERROR: Error verifying UTC timing: {e}")
        return None


# ---------------------------------------------------------------------
# Helper: Parse trial date and time into datetime
# ---------------------------------------------------------------------
def parse_trial_datetime(date_str: str, time_str: str) -> Optional[datetime]:
    """
    Parse date and time strings into a timezone-aware UTC datetime.

    Supports various formats including combined and separate legacy formats.
    Returns None if parsing fails.
    """
    try:
        date_str = (date_str or "").strip()
        time_str = (time_str or "").strip()

        # Attempt to parse full datetime strings directly from either input
        for val in (date_str, time_str):
            for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(val, fmt).replace(tzinfo=timezone.utc)
                except Exception:
                    continue

        # Attempt combined datetime string parsing
        if date_str and time_str:
            full_str = f"{date_str} {time_str}".strip()
            for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(full_str, fmt).replace(tzinfo=timezone.utc)
                except Exception:
                    continue

        # Parse separate legacy date and time formats
        if date_str and time_str:
            for date_fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                try:
                    date_obj = datetime.strptime(date_str, date_fmt).date()
                    break
                except Exception:
                    continue
            else:
                raise ValueError(f"Could not parse date '{date_str}'")

            for time_fmt in ("%I:%M %p", "%H:%M", "%H:%M:%S"):
                try:
                    time_obj = datetime.strptime(time_str, time_fmt).time()
                    break
                except Exception:
                    continue
            else:
                raise ValueError(f"Could not parse time '{time_str}'")

            return datetime.combine(date_obj, time_obj).replace(tzinfo=timezone.utc)

        raise ValueError("No recognizable datetime format")

    except Exception as e:
        log.warning(
            f"⚠️ WARNING: Failed to parse trial datetime: '{date_str}' '{time_str}' — {e}"
        )
        return None
