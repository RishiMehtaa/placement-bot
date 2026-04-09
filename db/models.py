# db/models.py
import hashlib
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Integer, String, Text, UUID, ARRAY
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import relationship

from db.database import Base


def compute_content_hash(text: str) -> str:
    """SHA-256 of normalized text — used for Layer 2 deduplication."""
    normalized = " ".join(text.lower().strip().split())
    return hashlib.sha256(normalized.encode()).hexdigest()


# ── Table 1: messages ─────────────────────────────────────────────────────────

class Message(Base):
    __tablename__ = "messages"

    message_id = Column(String, primary_key=True, nullable=False)
    text = Column(Text, nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    sender = Column(String, nullable=True)
    reply_to_id = Column(
        String, ForeignKey("messages.message_id", ondelete="SET NULL"),
        nullable=True
    )
    reply_to_preview = Column(Text, nullable=True)
    content_hash = Column(String, nullable=True, index=True)
    processed = Column(Boolean, default=False, nullable=False, index=True)
    process_attempts = Column(Integer, default=0, nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # # Relationships
    # replies = relationship(
    #     "Message",
    #     foreign_keys=[reply_to_id],
    #     backref="parent",
    #     lazy="select",
    # )
    # Self-referential relationship for reply chains
    replies = relationship(
        "Message",
        foreign_keys=[reply_to_id],
        primaryjoin="Message.reply_to_id == Message.message_id",
        remote_side="Message.message_id",
        lazy="select",
    )
    family_maps = relationship(
        "MessageFamilyMap",
        back_populates="message",
        cascade="all, delete-orphan",
    )


# ── Table 2: families ─────────────────────────────────────────────────────────

class Family(Base):
    __tablename__ = "families"

    id = Column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        nullable=False,
    )
    company = Column(String, nullable=True)
    role = Column(String, nullable=True)
    deadline = Column(DateTime(timezone=True), nullable=True)
    package = Column(String, nullable=True)
    jd_link = Column(Text, nullable=True)
    roles = Column(ARRAY(Text), default=list)
    duration = Column(Text, nullable=True)
    jd_links = Column(ARRAY(Text), default=list)
    internal_form_link = Column(Text, nullable=True)
    start_date = Column(Text, nullable=True)
    location = Column(Text, nullable=True)
    eligible = Column(Text, nullable=True)
    eligible_reason = Column(Text, nullable=True)
    notes = Column(ARRAY(Text), default=list, nullable=True)
    confidence = Column(Float, nullable=True)
    sheets_row_id = Column(String, nullable=True)
    calendar_event_id = Column(String, nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=True,
    )

    # Relationships
    message_maps = relationship(
        "MessageFamilyMap",
        back_populates="family",
        cascade="all, delete-orphan",
    )
    sheets_syncs = relationship(
        "SheetsSync",
        back_populates="family",
        cascade="all, delete-orphan",
    )


# ── Table 3: message_family_map ───────────────────────────────────────────────

class MessageFamilyMap(Base):
    __tablename__ = "message_family_map"

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(
        String,
        ForeignKey("messages.message_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    family_id = Column(
        PG_UUID(as_uuid=True),
        ForeignKey("families.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    contribution_role = Column(
        String,
        nullable=False,
        default="anchor",
        # Values: anchor | context | reply
    )

    # Relationships
    message = relationship("Message", back_populates="family_maps")
    family = relationship("Family", back_populates="message_maps")


# ── Table 4: sheets_sync ──────────────────────────────────────────────────────

class SheetsSync(Base):
    __tablename__ = "sheets_sync"

    id = Column(Integer, primary_key=True, autoincrement=True)
    family_id = Column(
        PG_UUID(as_uuid=True),
        ForeignKey("families.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    last_synced_at = Column(DateTime(timezone=True), nullable=True)
    sheets_row_id = Column(String, nullable=True)
    sync_status = Column(
        String,
        nullable=False,
        default="pending",
        # Values: pending | success | failed
    )

    # Relationship
    family = relationship("Family", back_populates="sheets_syncs")


# ── Table 5: dead_letter_queue ────────────────────────────────────────────────

class DeadLetterQueue(Base):
    __tablename__ = "dead_letter_queue"

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(String, nullable=True, index=True)
    failure_reason = Column(Text, nullable=True)
    failed_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    raw_payload = Column(JSONB, nullable=True)

# ── Table 6: queue_items ──────────────────────────────────────────────────────

class QueueItem(Base):
    __tablename__ = "queue_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(
        String,
        ForeignKey("messages.message_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    status = Column(
        String,
        nullable=False,
        default="pending",
        index=True,
        # Values: pending | processing | done | failed
    )
    enqueued_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    attempts = Column(Integer, default=0, nullable=False)
    last_error = Column(Text, nullable=True)