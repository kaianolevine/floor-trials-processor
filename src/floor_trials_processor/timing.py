from datetime import datetime, timedelta, timezone
from typing import Optional

from kaiano_common_utils import logger as log

import floor_trials_processor.config as config
import floor_trials_processor.helpers as helpers


# ---------------------------------------------------------------------
# Helper: Check if next floor trial is within MAX_START_DELAY_HOURS
# ---------------------------------------------------------------------
def should_start_run(service, spreadsheet_id) -> bool:
    """Return False if the next floor trial starts more than 1.5 hours away."""
    start_str = helpers.get_single_cell(
        service, spreadsheet_id, config.FLOOR_START_RANGE
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
    if not dt_start:
        log.warning("No valid floor trial start time — exiting gracefully.")
        return False

    now_utc = datetime.now(timezone.utc)
    if dt_start > now_utc + timedelta(hours=config.MAX_START_DELAY_HOURS):
        log.info(
            f"Floor trial starts at {dt_start} (more than {config.MAX_START_DELAY_HOURS} hours away) — exiting early."
        )
        return False
    return True


def verify_utc_timing(service, sheet_id) -> None:
    """Log UTC-based timing diagnostics for the Floor Trial schedule."""
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

        log.info("=== UTC Verification — Floor Trial Timing ===")
        log.info(f"Trial Date (D15): {date_val}")
        log.info(f"Start Time (C17): {start_val}")
        log.info(f"End Time (D17):   {end_val}")

        if dt_start is None:
            log.warning(f"⚠️ Invalid start time: '{start_val}'")
        else:
            log.info(f"Parsed UTC Start: {dt_start}")

        if dt_end is None:
            log.warning(f"⚠️ Invalid end time: '{end_val}'")
        else:
            log.info(f"Parsed UTC End:   {dt_end}")

        log.info(f"Current UTC Now:  {now_utc}")

        status = "unknown"
        if dt_start and dt_end:
            if dt_start <= now_utc <= dt_end:
                log.info("✅ Floor Trial is IN PROGRESS (UTC)")
                status = "in_progress"
            elif now_utc < dt_start:
                log.info("⏳ Floor Trial has NOT STARTED yet (UTC)")
                status = "not_started"
            else:
                log.info("🏁 Floor Trial is FINISHED (UTC)")
                status = "finished"
        else:
            log.warning("⚠️ Could not parse trial date/time — check sheet values")

        helpers.update_floor_trial_status(service, sheet_id)
        log.info("✅ UTC Verification complete — proceeding to queue processing")

        return {
            "date": date_val,
            "start": start_val,
            "end": end_val,
            "now": now_utc.isoformat(),
            "status": status,
        }
    except Exception as e:
        log.error(f"Error verifying UTC timing: {e}")
        return None


# ---------------------------------------------------------------------
# Helper: Parse trial date and time into datetime
# ---------------------------------------------------------------------
def parse_trial_datetime(date_str: str, time_str: str) -> Optional[datetime]:
    """Parse combined or standalone UTC datetime strings into a timezone-aware datetime."""
    try:
        date_str = (date_str or "").strip()
        time_str = (time_str or "").strip()

        # Case 0: If one field already looks like a full UTC datetime, parse it directly
        for val in (date_str, time_str):
            for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(val, fmt).replace(tzinfo=timezone.utc)
                except Exception:
                    continue

        # Case 1: Combined string (legacy fallback)
        if date_str and time_str:
            full_str = f"{date_str} {time_str}".strip()
            for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(full_str, fmt).replace(tzinfo=timezone.utc)
                except Exception:
                    continue

        # Case 2: Separate legacy formats (M/D/YYYY and H:MM AM/PM)
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
        log.warning(f"Failed to parse trial datetime: '{date_str}' '{time_str}' — {e}")
        return None
