# db/models.py
import hashlib
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Text, Boolean, Integer,
    DateTime, ForeignKey
)
from sqlalchemy.orm import relationship
from db.database import Base


def compute_content_hash(text: str) -> str:
    normalized = " ".join(text.lower().strip().split())
    return hashlib.sha256(normalized.encode()).hexdigest()


class Message(Base):
    __tablename__ = "messages"

    message_id = Column(String, primary_key=True, nullable=False)
    text = Column(Text, nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    sender = Column(String, nullable=True)
    reply_to_id = Column(String, ForeignKey("messages.message_id"), nullable=True)
    reply_to_preview = Column(Text, nullable=True)
    content_hash = Column(String, nullable=True)
    processed = Column(Boolean, default=False, nullable=False)
    process_attempts = Column(Integer, default=0, nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # Self-referential relationship for reply chains
    replies = relationship("Message", foreign_keys=[reply_to_id])