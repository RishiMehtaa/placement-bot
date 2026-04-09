"""
Stage 6 — Family Resolver
Accepts a NormalizedRecord, finds or creates a matching family,
and maps the current message to that family.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from db import queries
from extraction.normalizer import NormalizedRecord
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class FamilyResolutionResult:
    family_id: UUID
    company: Optional[str]
    role: Optional[str]
    is_new_family: bool
    contribution_role: str          # "anchor" | "context" | "unmapped"
    matched_on: str                 # "company_and_role" | "company_only" | "none"


async def resolve_family(
    record: NormalizedRecord,
    db: AsyncSession,
    current_message,
) -> FamilyResolutionResult:
    """
    Main entry point for Stage 6.

    Matching priority:
      1. company + role both present → match on both (strongest signal)
      2. company present, role absent → match on company only (weaker signal)
      3. company absent → cannot anchor; attempt to attach to most recent family

    Contribution roles:
      - "anchor"  → message created the family or is the primary record
      - "context" → message added to an existing family
      - "unmapped" → company is None and no recent family exists; message stored
                     but not attached to any family
    """

    message_id = record.message_id
    company = record.company
    role = record.role

    # ------------------------------------------------------------------ #
    # Case 0: reply-based grouping (HIGHEST PRIORITY)
    # ------------------------------------------------------------------ #
    if current_message.reply_to_id:
        parent_family = await queries.get_family_by_message_id(
            db, current_message.reply_to_id
        )

        if parent_family:
            logger.info(
                "Stage 6 | message=%s | attached via reply to family=%s",
                message_id,
                parent_family.id,
            )
            await queries.map_message_to_family(
                db, message_id, parent_family.id, contribution_role="context"
            )
            return FamilyResolutionResult(
                family_id=parent_family.id,
                company=parent_family.company,
                role=parent_family.role,
                is_new_family=False,
                contribution_role="context",
                matched_on="reply",
            )

    # ------------------------------------------------------------------ #
    # Case 1: company + role both known — strongest match signal
    # ------------------------------------------------------------------ #
    if company and role:
        existing = await queries.find_family_by_company_and_role(db, company, role)

        if existing:
            logger.info(
                "Stage 6 | message=%s | matched existing family=%s on company+role",
                message_id,
                existing.id,
            )
            await queries.map_message_to_family(
                db, message_id, existing.id, contribution_role="context"
            )
            return FamilyResolutionResult(
                family_id=existing.id,
                company=existing.company,
                role=existing.role,
                is_new_family=False,
                contribution_role="context",
                matched_on="company_and_role",
            )

        # No existing family — create one
        new_family = await queries.create_family(
                db,
                {
                    "company": company,
        "role": role,
        "roles": record.roles,
        "deadline": record.deadline,
        "package": record.package,
        "jd_link": record.jd_link,
        "jd_links": record.jd_links,
        "internal_form_link": record.internal_form_link,
        "duration": record.duration,
        "start_date": record.start_date,
        "location": record.location,
        "eligible": record.eligible,
        "eligible_reason": record.eligible_reason,
        "confidence": record.confidence,
                },
            )
        await queries.map_message_to_family(
            db, message_id, new_family.id, contribution_role="anchor"
        )
        logger.info(
            "Stage 6 | message=%s | created new family=%s (company+role)",
            message_id,
            new_family.id,
        )
        return FamilyResolutionResult(
            family_id=new_family.id,
            company=new_family.company,
            role=new_family.role,
            is_new_family=True,
            contribution_role="anchor",
            matched_on="company_and_role",
        )

    # ------------------------------------------------------------------ #
    # Case 2: company known, role unknown — match on company only
    # ------------------------------------------------------------------ #
    if company and not role:
        existing = await queries.find_family_by_company_only(db, company)

        if existing:
            logger.info(
                "Stage 6 | message=%s | matched existing family=%s on company only",
                message_id,
                existing.id,
            )
            await queries.map_message_to_family(
                db, message_id, existing.id, contribution_role="context"
            )
            return FamilyResolutionResult(
                family_id=existing.id,
                company=existing.company,
                role=existing.role,
                is_new_family=False,
                contribution_role="context",
                matched_on="company_only",
            )

        # No match even on company — create a partial family
        new_family = await queries.create_family(
            db,
            {
                "company": company,
        "role": role,
        "roles": record.roles,
        "deadline": record.deadline,
        "package": record.package,
        "jd_link": record.jd_link,
        "jd_links": record.jd_links,
        "internal_form_link": record.internal_form_link,
        "duration": record.duration,
        "start_date": record.start_date,
        "location": record.location,
        "eligible": record.eligible,
        "eligible_reason": record.eligible_reason,
        "confidence": record.confidence,
            },
        )
        await queries.map_message_to_family(
            db, message_id, new_family.id, contribution_role="anchor"
        )
        logger.info(
            "Stage 6 | message=%s | created new partial family=%s (company only)",
            message_id,
            new_family.id,
        )
        return FamilyResolutionResult(
            family_id=new_family.id,
            company=new_family.company,
            role=new_family.role,
            is_new_family=True,
            contribution_role="anchor",
            matched_on="company_only",
        )

    # # ------------------------------------------------------------------ #
    # # Case 3: company unknown — cannot anchor a family
    # # Attach to the most recent family as context if one exists,
    # # otherwise mark as unmapped.
    # # ------------------------------------------------------------------ #
    # recent = await queries.get_most_recent_family(db)

    # if recent:
    #     logger.info(
    #         "Stage 6 | message=%s | no company — attaching to most recent family=%s as context",
    #         message_id,
    #         recent.id,
    #     )
    #     await queries.map_message_to_family(
    #         db, message_id, recent.id, contribution_role="context"
    #     )
    #     return FamilyResolutionResult(
    #         family_id=recent.id,
    #         company=recent.company,
    #         role=recent.role,
    #         is_new_family=False,
    #         contribution_role="context",
    #         matched_on="none",
    #     )

    # logger.warning(
    #     "Stage 6 | message=%s | no company and no existing families — unmapped",
    #     message_id,
    # )
    # return FamilyResolutionResult(
    #     family_id=None,
    #     company=None,
    #     role=None,
    #     is_new_family=False,
    #     contribution_role="unmapped",
    #     matched_on="none",
    # )
    
    # ------------------------------------------------------------------ #
    # Case 3: weak message (no company, no role)
    # ------------------------------------------------------------------ #

    # Only attach if message has useful data
    has_signal = any([
        record.deadline,
        record.package,
        record.jd_link,
        # record.notes
    ])

    if has_signal:
        recent = await queries.get_most_recent_family(db)

        if recent:
            logger.info(
                "Stage 6 | message=%s | weak signal — attaching to recent family=%s",
                message_id,
                recent.id,
            )
            await queries.map_message_to_family(
                db, message_id, recent.id, contribution_role="context"
            )
            return FamilyResolutionResult(
                family_id=recent.id,
                company=recent.company,
                role=recent.role,
                is_new_family=False,
                contribution_role="context",
                matched_on="recent_family",
            )

    # fallback
    logger.warning(
        "Stage 6 | message=%s | weak message but no suitable family — unmapped",
        message_id,
    )

    return FamilyResolutionResult(
        family_id=None,
        company=None,
        role=None,
        is_new_family=False,
        contribution_role="unmapped",
        matched_on="none",
    )