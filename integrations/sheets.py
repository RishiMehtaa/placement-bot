"""
integrations/sheets.py
Phase 15 — Google Sheets Integration

Responsibilities:
- Authenticate using service account JSON
- Write or update a row in the configured Google Sheet for a given family
- Track sync state in the sheets_sync table
- Idempotent — if family already has a sheets_row_id, update in place
- Never raise on failure — log and mark sync_status=failed, pipeline continues
"""

import os
from datetime import datetime, timezone
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config.settings import settings
from db.database import get_db_context
from db.queries import (
    get_family_by_id,
    get_sheets_sync_record,
    upsert_sheets_sync,
    update_family_sheets_row,
)
from utils.logger import get_logger

logger = get_logger(__name__)

# Google Sheets API scope — read/write
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Sheet column order — must match HEADER_ROW below
# HEADER_ROW = [
#     "Family ID",
#     "Company",
#     "Role",
#     "Deadline",
#     "Package",
#     "JD Link",
#     "Confidence",
#     "Last Updated",
# ]

HEADER_ROW = [
    "Family ID",
    "Company",
    "Role/s",
    "Duration",
    "JD Link/s",
    "Internal Form Link",
    "Start Date",
    "Location",
    "Stipend/Package",
    "Application Deadline",
    "Eligible",
    "Eligible Reason",
    "Confidence",
    "Last Updated",
]

# Which row number the header lives on (1-indexed)
HEADER_ROW_NUMBER = 1

# Data starts from this row (1-indexed)
DATA_START_ROW = 2


def _build_sheets_client():
    """
    Build and return an authenticated Google Sheets API client.
    Uses service account JSON from the path in settings.
    """
    creds = service_account.Credentials.from_service_account_file(
        settings.GOOGLE_SERVICE_ACCOUNT_JSON,
        scopes=SCOPES,
    )
    client = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return client



def _family_to_rows(family) -> list[list]:
    """Convert a Family ORM object to one or more Sheets rows."""

    roles = family.roles or [family.role] or [""]

    rows = []

    for i, role in enumerate(roles):
        rows.append([
            str(family.id),
            family.company or "",
            role or "",
            family.duration or "",
            family.jd_links[i]
                if family.jd_links and i < len(family.jd_links)
                else (family.jd_link or ""),
            family.internal_form_link or "",
            family.start_date or "",
            family.location or "",
            family.package or "",
            family.deadline.strftime("%d %b %Y %H:%M")
            if family.deadline
            else "",
            family.eligible or "",
            family.eligible_reason or "",
            str(round(family.confidence, 2)) if family.confidence else "0.0",
            datetime.utcnow().strftime("%d %b %Y %H:%M"),
        ])

    return rows


async def _ensure_header_row(client, spreadsheet_id: str) -> None:
    """
    Check if the header row exists. If not, write it.
    This is a no-op if the header already exists.
    """
    try:
        range_name = f"Sheet1!A{HEADER_ROW_NUMBER}:{_col_letter(len(HEADER_ROW))}{HEADER_ROW_NUMBER}"
        result = (
            client.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_name)
            .execute()
        )
        existing = result.get("values", [])
        if existing and existing[0] == HEADER_ROW:
            return  # Header already correct
        # Write header
        client.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body={"values": [HEADER_ROW]},
        ).execute()
        logger.info("Sheets | Header row written")
    except HttpError as e:
        logger.error(f"Sheets | Failed to ensure header row | error={e}")
        raise


async def _find_row_by_family_id(
    client, spreadsheet_id: str, family_id: str
) -> Optional[int]:
    """
    Scan column A (Family ID) to find the row number of an existing family.
    Returns the 1-indexed row number if found, None otherwise.
    """
    try:
        result = (
            client.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range="Sheet1!A:A")
            .execute()
        )
        values = result.get("values", [])
        for i, row in enumerate(values):
            if row and row[0] == family_id:
                return i + 1  # Convert 0-indexed to 1-indexed
        return None
    except HttpError as e:
        logger.error(f"Sheets | Failed to scan column A | family_id={family_id} | error={e}")
        return None


def _col_letter(n: int) -> str:
    """
    Convert a 1-indexed column number to a letter (1=A, 2=B, ..., 8=H).
    Handles up to 26 columns.
    """
    return chr(ord("A") + n - 1)


def _row_range(row_number: int) -> str:
    """
    Build a Sheets range string for a full row, e.g. 'Sheet1!A3:H3'
    """
    last_col = _col_letter(len(HEADER_ROW))
    return f"Sheet1!A{row_number}:{last_col}{row_number}"


async def sync_to_sheets(family_id: str) -> bool:
    """
    Main entry point. Called by processor.py after merge engine completes.

    Steps:
    1. Load family from DB
    2. Build authenticated Sheets client
    3. Ensure header row exists
    4. Check sheets_sync table for existing sheets_row_id
    5. If row exists: update it in place
    6. If row does not exist: append new row, record row number
    7. Update sheets_sync table with sync result
    8. Return True on success, False on failure

    Never raises — all exceptions are caught, logged, and returned as False.
    """
    try:
        async with get_db_context() as db:
            family = await get_family_by_id(db, family_id)
            if not family:
                logger.warning(f"Sheets | Family not found | family_id={family_id}")
                return False

            sync_record = await get_sheets_sync_record(db, family_id)
            existing_row_id = sync_record.sheets_row_id if sync_record else None

            row_data = _family_to_rows(family)

        client = _build_sheets_client()
        spreadsheet_id = settings.GOOGLE_SHEET_ID

        await _ensure_header_row(client, spreadsheet_id)

        target_row: Optional[int] = None

        # Try to find the row by family_id in column A (source of truth)
        if existing_row_id:
            try:
                # target_row = int(existing_row_id)
                parts = str(existing_row_id).split(":")
                target_row = int(parts[0])
                row_count = int(parts[1]) if len(parts) > 1 else 1
            except (ValueError, TypeError):
                target_row = None

        if target_row is None:
            # Scan sheet to find if family already has a row (defensive check)
            target_row = await _find_row_by_family_id(client, spreadsheet_id, family_id)

        # if target_row is not None:
        #     # UPDATE existing row in place
        #     range_name = _row_range(target_row)
        #     client.spreadsheets().values().update(
        #         spreadsheetId=spreadsheet_id,
        #         range=range_name,
        #         valueInputOption="RAW",
        #         body={"values": row_data},
        #     ).execute()
        #     sheets_row_id = str(target_row)
        #     action = "updated"
        if target_row is not None:
            # UPDATE existing row(s) in place
            last_row = target_row + len(row_data) - 1
            range_name = f"Sheet1!A{target_row}:{_col_letter(len(HEADER_ROW))}{last_row}"

            client.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                body={"values": row_data},
            ).execute()

            sheets_row_id = f"{target_row}:{len(row_data)}"
            action = "updated"
        else:
            # APPEND new row
            result = (
                client.spreadsheets()
                .values()
                .append(
                    spreadsheetId=spreadsheet_id,
                    range=f"Sheet1!A{DATA_START_ROW}",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": row_data},
                )
                .execute()
            )
            # Parse the updated range to get the actual row number
            updated_range = result.get("updates", {}).get("updatedRange", "")
            start_row = int(_parse_row_from_range(updated_range))
            row_count = len(row_data)
            sheets_row_id = f"{start_row}:{row_count}"
            # sheets_row_id = _parse_row_from_range(updated_range)
            action = "appended"

        # Persist sync state back to DB
        async with get_db_context() as db:
            await upsert_sheets_sync(
                db,
                family_id=family_id,
                sheets_row_id=sheets_row_id,
                sync_status="success",
            )
            await update_family_sheets_row(db, family_id, sheets_row_id)

        logger.info(
            f"Sheets | Sync complete | family_id={family_id} | action={action} | row={sheets_row_id}"
        )
        return True

    except HttpError as e:
        logger.error(f"Sheets | Google API error | family_id={family_id} | error={e}")
        await _mark_sync_failed(family_id, str(e))
        return False
    except Exception as e:
        logger.error(f"Sheets | Unexpected error | family_id={family_id} | error={e}")
        await _mark_sync_failed(family_id, str(e))
        return False


def _parse_row_from_range(updated_range: str) -> str:
    """
    Parse the row number from a Sheets range string like 'Sheet1!A5:H5'.
    Returns the row number as a string, or '0' if parsing fails.
    """
    try:
        # updated_range looks like "Sheet1!A5:H5"
        # Strip sheet name
        cell_part = updated_range.split("!")[1]
        # Take first cell reference e.g. "A5"
        first_cell = cell_part.split(":")[0]
        # Strip the column letter(s)
        row_num = "".join(filter(str.isdigit, first_cell))
        return row_num if row_num else "0"
    except Exception:
        return "0"


async def _mark_sync_failed(family_id: str, reason: str) -> None:
    """
    Write sync_status=failed to sheets_sync table.
    Called on any exception in sync_to_sheets.
    Never raises.
    """
    try:
        async with get_db_context() as db:
            await upsert_sheets_sync(
                db,
                family_id=family_id,
                sheets_row_id=None,
                sync_status="failed",
            )
    except Exception as e:
        logger.error(f"Sheets | Failed to mark sync failed | family_id={family_id} | error={e}")