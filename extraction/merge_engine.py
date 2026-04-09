"""
Stage 7 — Merge Engine
Applies merge rules to an existing family record using fields from a NormalizedRecord.
Called only when FamilyResolutionResult.is_new_family is False.
"""

from dataclasses import dataclass, field
from typing import Optional
from datetime import timezone

from sqlalchemy.ext.asyncio import AsyncSession

from extraction.normalizer import NormalizedRecord
from extraction.family_resolver import FamilyResolutionResult
from db.queries import get_family_by_id, update_family
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class MergeResult:
    family_id: str
    was_merged: bool
    updated_fields: list[str] = field(default_factory=list)
    skipped_fields: list[str] = field(default_factory=list)


def _is_valid_url(url: Optional[str]) -> bool:
    """Basic URL validation — must start with http:// or https://"""
    if not url:
        return False
    return url.startswith("http://") or url.startswith("https://")


def _ensure_utc(dt):
    """Ensure a datetime is timezone-aware UTC. If naive, attach UTC."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


async def merge_into_family(
    record: NormalizedRecord,
    resolution: FamilyResolutionResult,
    db: AsyncSession,
) -> MergeResult:
    """
    Apply merge rules to an existing family using fields from a NormalizedRecord.

    Merge rules:
    - Never overwrite a valid non-null field with null
    - Deadline: update only if new value is later than existing
    - Package: update if new value is non-null
    - jd_link: update only if new URL is valid AND current family jd_link is null
    - Notes: always append, never replace
    - Confidence: always take max(existing, new)

    If is_new_family is True, skips merge entirely (family already has the data).
    """
    family_id = str(resolution.family_id)

    if resolution.is_new_family:
        logger.info(f"[merge] family_id={family_id} is new — no merge needed")
        return MergeResult(
            family_id=family_id,
            was_merged=False,
            updated_fields=[],
            skipped_fields=[],
        )

    family = await get_family_by_id(db, family_id)
    if family is None:
        logger.warning(f"[merge] family_id={family_id} not found in DB — skipping merge")
        return MergeResult(
            family_id=family_id,
            was_merged=False,
            updated_fields=[],
            skipped_fields=["all — family not found"],
        )

    updates: dict = {}
    updated_fields: list[str] = []
    skipped_fields: list[str] = []

    # --- company ---
    # --- company ---
    if record.company is not None:
        if family.company != record.company:
            updates["company"] = record.company
            updated_fields.append("company")
        else:
            skipped_fields.append("company")
    
    # --- role ---
    if record.role is not None:
        if family.role != record.role:
            updates["role"] = record.role
            updated_fields.append("role")
        else:
            skipped_fields.append("role")
    # --- roles ---
    if record.roles:
        if family.roles != record.roles:
            updates["roles"] = record.roles
            updated_fields.append("roles")
        else:
            skipped_fields.append("roles")

    # # --- deadline ---
    # if record.deadline is not None:
    #     new_deadline = _ensure_utc(record.deadline)
    #     if family.deadline is None:
    #         updates["deadline"] = new_deadline
    #         updated_fields.append("deadline")
    #     else:
    #         existing_deadline = _ensure_utc(family.deadline)
    #         if new_deadline > existing_deadline:
    #             updates["deadline"] = new_deadline
    #             updated_fields.append("deadline")
    #         else:
    #             skipped_fields.append("deadline")
    # --- deadline ---
    if record.deadline is not None:
        new_deadline = _ensure_utc(record.deadline)
        existing_deadline = _ensure_utc(family.deadline)

        if existing_deadline != new_deadline:
            updates["deadline"] = new_deadline
            updated_fields.append("deadline")
        else:
            skipped_fields.append("deadline")

    # # --- package ---
    # if record.package is not None:
    #     updates["package"] = record.package
    #     updated_fields.append("package")
    # else:
    #     if family.package is not None:
    #         skipped_fields.append("package")

    # --- package ---
# --- package ---
    if record.package is not None:
        if family.package != record.package:
            updates["package"] = record.package
            updated_fields.append("package")
        else:
            skipped_fields.append("package")
    # --- jd_link ---
    if _is_valid_url(record.jd_link):
        if family.jd_link != record.jd_link:
            updates["jd_link"] = record.jd_link
            updated_fields.append("jd_link")
        else:
            skipped_fields.append("jd_link")

    # --- jd_links ---
    if record.jd_links:
        existing_links = family.jd_links or []
        merged_links = existing_links.copy()

        for link in record.jd_links:
            if link not in merged_links:
                merged_links.append(link)

        if merged_links != existing_links:
            updates["jd_links"] = merged_links
            updated_fields.append("jd_links")
        else:
            skipped_fields.append("jd_links")
    # --- internal_form_link ---
    if record.internal_form_link is not None:
        if family.internal_form_link != record.internal_form_link:
            updates["internal_form_link"] = record.internal_form_link
            updated_fields.append("internal_form_link")
        else:
            skipped_fields.append("internal_form_link")
    # --- duration ---
    if record.duration is not None:
        if family.duration != record.duration:
            updates["duration"] = record.duration
            updated_fields.append("duration")
        else:
            skipped_fields.append("duration")
    # --- start_date ---
    if record.start_date is not None:
        if family.start_date != record.start_date:
            updates["start_date"] = record.start_date
            updated_fields.append("start_date")
        else:
            skipped_fields.append("start_date")
    # --- location ---
    if record.location is not None:
        if family.location != record.location:
            updates["location"] = record.location
            updated_fields.append("location")
        else:
            skipped_fields.append("location")
    # --- eligible ---
    if record.eligible is not None:
        if family.eligible != record.eligible:
            updates["eligible"] = record.eligible
            updated_fields.append("eligible")
        else:
            skipped_fields.append("eligible")
    # --- eligible_reason ---
    if record.eligible_reason is not None:
        if family.eligible_reason != record.eligible_reason:
            updates["eligible_reason"] = record.eligible_reason
            updated_fields.append("eligible_reason")
        else:
            skipped_fields.append("eligible_reason")
                
# --- notes ---
    # if record.notes:
    #     existing_notes = family.notes or []
    #     new_notes = [n for n in record.notes if n not in existing_notes]
    #     if new_notes:
    #         updates["notes"] = existing_notes + new_notes
    #         updated_fields.append("notes")
    #     else:
    #         skipped_fields.append("notes")

    # --- confidence ---
    if record.confidence is not None:
        existing_confidence = family.confidence or 0.0
        if record.confidence > existing_confidence:
            updates["confidence"] = record.confidence
            updated_fields.append("confidence")
        else:
            skipped_fields.append("confidence")

    if updates:
        await update_family(db, family_id, updates)
        logger.info(
            f"[merge] family_id={family_id} updated fields={updated_fields}"
        )
    else:
        logger.info(
            f"[merge] family_id={family_id} no fields to update — skipped={skipped_fields}"
        )

    return MergeResult(
        family_id=family_id,
        was_merged=bool(updates),
        updated_fields=updated_fields,
        skipped_fields=skipped_fields,
    )