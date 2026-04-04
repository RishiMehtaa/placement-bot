"""
integrations/calendar.py
Stage 16 — Google Calendar Integration

Creates or updates a Google Calendar event for a placement family.
Called by worker/processor.py after sheets sync (Stage 15).

Rules:
- If deadline is null: skip, log warning, return False
- If calendar_event_id exists on family: update event in place
- If calendar_event_id is null: create new event
- Always update families.calendar_event_id after successful sync
- Never raises — all exceptions caught and logged
- Idempotent — running twice produces identical calendar state
"""

import json
from datetime import timezone
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config.settings import settings
from db.database import get_db_context
from db.queries import get_family_by_id, update_family
from utils.logger import get_logger

logger = get_logger(__name__)

SCOPES = ["https://www.googleapis.com/auth/calendar"]


def _get_calendar_service():
    """Build and return an authenticated Google Calendar service client."""
    credentials = service_account.Credentials.from_service_account_file(
        settings.GOOGLE_SERVICE_ACCOUNT_JSON,
        scopes=SCOPES,
    )
    service = build("calendar", "v3", credentials=credentials, cache_discovery=False)
    return service


def _build_event_body(family) -> dict:
    """
    Build the Google Calendar event body from a family record.
    deadline is guaranteed non-null by the caller.
    """
    company = family.company or "Unknown Company"
    role = family.role or "Unknown Role"
    title = f"{company} — {role} Deadline"

    # Deadline may be timezone-aware or naive — normalise to UTC date string
    deadline = family.deadline
    if deadline.tzinfo is None:
        deadline = deadline.replace(tzinfo=timezone.utc)
    date_str = deadline.strftime("%Y-%m-%d")

    description_parts = []
    if family.package:
        description_parts.append(f"Package: {family.package}")
    if family.jd_link:
        description_parts.append(f"JD Link: {family.jd_link}")
    description_parts.append(f"Family ID: {family.id}")
    description = "\n".join(description_parts)

    event_body = {
        "summary": title,
        "description": description,
        "start": {
            "date": date_str,
            "timeZone": "UTC",
        },
        "end": {
            "date": date_str,
            "timeZone": "UTC",
        },
        "reminders": {
            "useDefault": False,
            "overrides": [
                {"method": "popup", "minutes": 1440},   # 24 hours before
                {"method": "popup", "minutes": 60},     # 1 hour before
            ],
        },
    }
    return event_body


async def _update_calendar_event_id(family_id: str, event_id: str) -> None:
    """Persist the calendar_event_id back to the families table."""
    async with get_db_context() as db:
        await update_family(db, family_id, {"calendar_event_id": event_id})


async def sync_to_calendar(family_id: str) -> bool:
    """
    Main entry point. Creates or updates a Google Calendar event for the family.

    Returns True on success, False on any failure.
    Never raises.
    """
    try:
        async with get_db_context() as db:
            family = await get_family_by_id(db, family_id)

        if family is None:
            logger.warning(f"[calendar] Family not found: {family_id}")
            return False

        if family.deadline is None:
            logger.warning(
                f"[calendar] Skipping family {family_id} — deadline is null, "
                "cannot create calendar event without a date"
            )
            return False

        service = _get_calendar_service()
        event_body = _build_event_body(family)
        calendar_id = settings.GOOGLE_CALENDAR_ID

        existing_event_id: Optional[str] = family.calendar_event_id

        if existing_event_id:
            # Update existing event in place
            try:
                service.events().update(
                    calendarId=calendar_id,
                    eventId=existing_event_id,
                    body=event_body,
                ).execute()
                logger.info(
                    f"[calendar] Updated event {existing_event_id} "
                    f"for family {family_id} ({family.company} — {family.role})"
                )
                return True

            except HttpError as e:
                if e.resp.status == 404:
                    # Event was deleted externally — fall through to create a new one
                    logger.warning(
                        f"[calendar] Event {existing_event_id} not found (404) "
                        f"for family {family_id} — creating a new event"
                    )
                    existing_event_id = None
                else:
                    raise  # re-raise non-404 HttpErrors to outer handler

        # Create new event
        created_event = service.events().insert(
            calendarId=calendar_id,
            body=event_body,
        ).execute()

        new_event_id = created_event.get("id")
        if not new_event_id:
            logger.error(
                f"[calendar] Google Calendar returned no event ID for family {family_id}"
            )
            return False

        await _update_calendar_event_id(family_id, new_event_id)

        logger.info(
            f"[calendar] Created event {new_event_id} "
            f"for family {family_id} ({family.company} — {family.role}) "
            f"on {family.deadline.strftime('%Y-%m-%d')}"
        )
        return True

    except HttpError as e:
        logger.error(
            f"[calendar] Google API HttpError for family {family_id}: "
            f"status={e.resp.status} reason={e.reason}"
        )
        return False

    except Exception as e:
        logger.error(
            f"[calendar] Unexpected error for family {family_id}: {type(e).__name__}: {e}"
        )
        return False