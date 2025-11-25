from datetime import datetime, timedelta, timezone
from typing import Optional

from kaiano_common_utils import logger as log

import floor_trials_processor.config as config
import floor_trials_processor.helpers as helpers


def check_sheet_should_run(service, spreadsheet_id):
    """
    Determines if automation should run based on control cell value in the sheet.
    """

    control_cell = config.AUTOMATION_CONTROL_CELL
    try:
        value = helpers.get_single_cell(service, spreadsheet_id, control_cell)
        if str(value).strip().lower() != "runautomations":
            log.warning(f"⚠️ Automation disabled (cell={control_cell}, val='{value}')")
            return False
    except Exception as e:
        log.error(f"❌ Control cell read failure: {e}", exc_info=True)
        return False

    # Control cell explicitly allows automation
    return True


def should_run(
    now_utc: datetime,
    dt_open: datetime,
    dt_end: datetime,
    start_time: datetime,
    max_duration_minutes: int,
    max_start_delay_hours: float = config.EARLY_WINDOW_RUNNING_BUFFER,
    end_buffer_minutes: int = config.FLOOR_END_BUFFER,
) -> bool:
    """
    Return True if the watcher should currently be running based on the allowed window relative to open and end times.

    Logic: True if time is within [open - MAX_START_DELAY_HOURS, end + FLOOR_END_BUFFER_MIN]
    """

    earliest_allowed = dt_open - timedelta(hours=max_start_delay_hours)
    latest_allowed = dt_end + timedelta(minutes=end_buffer_minutes)

    # Too early — before allowed window
    if now_utc < earliest_allowed:
        log.debug(f"⏸️ Too early: now={now_utc}, earliest allowed={earliest_allowed}")
        return False

    # Too late — outside allowed window
    if now_utc > latest_allowed:
        log.debug(
            f"🏁 Past allowed run window: now={now_utc}, latest allowed={latest_allowed}"
        )
        return False

    # Exceeded maximum allowed duration — stop running
    if start_time and max_duration_minutes:
        elapsed = now_utc - start_time
        if elapsed > timedelta(minutes=max_duration_minutes):
            log.debug(
                f"🕒 Max duration exceeded: elapsed={elapsed}, allowed={max_duration_minutes} minutes"
            )
            return False

    return True


def floor_trial_active(
    now_utc: datetime,
    dt_open: datetime,
    dt_end: datetime,
) -> bool:
    """
    Return True if we are between dt_open and dt_end (strict active window).

    This version does NOT allow early or extended automation — only true active window.
    """

    if now_utc < dt_open:
        log.debug(f"⏸️ Too early: now={now_utc}, earliest allowed={dt_open}")
        return False

    if now_utc > dt_end:
        log.debug(f"🏁 Past allowed run window: now={now_utc}, latest allowed={dt_end}")
        return False

    return True


# ---------------------------------------------------------------------
# Helper: Parse trial date and time into datetime
# ---------------------------------------------------------------------
def parse_trial_datetime(date_str: str, time_str: str) -> Optional[datetime]:
    """
    Parse date and time strings into a timezone-aware UTC datetime.

    Supports various formats including combined and separate legacy formats.
    Returns None if no recognizable format is found.
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
