# scraper/receiver.py
from pydantic import BaseModel, field_validator
from typing import Optional
from datetime import datetime, timezone


class MessagePayload(BaseModel):
    message_id: str
    text: str
    timestamp: datetime
    sender: Optional[str] = None
    reply_to_id: Optional[str] = None
    reply_to_preview: Optional[str] = None

    @field_validator("message_id")
    @classmethod
    def message_id_must_not_be_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("message_id must not be empty")
        return v.strip()

    @field_validator("text")
    @classmethod
    def text_must_not_be_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("text must not be empty")
        return v.strip()

    @field_validator("reply_to_preview")
    @classmethod
    def truncate_preview(cls, v):
        if v and len(v) > 100:
            return v[:100]
        return v


# ── Test message fixtures ─────────────────────────────────────────────────────
# Used by /ingest/test endpoint — simulates real WhatsApp messages
# Replace or extend these with messages typical of your placement group

TEST_MESSAGES = [
    {
        "message_id": "test_msg_001",
        "text": "TCS is hiring! Role: Software Engineer. Package: 7 LPA. Apply by 30 March 2025. Link: https://tcs.com/careers/apply",
        "timestamp": "2025-03-20T10:00:00+00:00",
        "sender": "919876543210@s.whatsapp.net",
        "reply_to_id": None,
        "reply_to_preview": None,
    },
    {
        "message_id": "test_msg_002",
        "text": "Infosys placement drive on 25th March. Stipend: 25k/month. Register here: https://infosys.com/placement/register2025",
        "timestamp": "2025-03-20T10:05:00+00:00",
        "sender": "919876543211@s.whatsapp.net",
        "reply_to_id": None,
        "reply_to_preview": None,
    },
    {
        "message_id": "test_msg_003",
        "text": "Last date to apply is tomorrow by 5pm",
        "timestamp": "2025-03-20T10:10:00+00:00",
        "sender": "919876543211@s.whatsapp.net",
        "reply_to_id": "test_msg_002",
        "reply_to_preview": "Infosys placement drive on 25th March. Stipend: 25k/month.",
    },
    {
        "message_id": "test_msg_004",
        "text": "Wipro is hiring for Data Engineer role. CTC: 12 lakhs per annum. Deadline: 31/03/2025. Apply: https://wipro.com/careers",
        "timestamp": "2025-03-20T10:15:00+00:00",
        "sender": "919876543212@s.whatsapp.net",
        "reply_to_id": None,
        "reply_to_preview": None,
    },
    {
        "message_id": "test_msg_005",
        "text": "Amazon SDE internship opening. Stipend 60k/month. Apply by this Friday. Link: https://amazon.jobs/internship2025",
        "timestamp": "2025-03-20T10:20:00+00:00",
        "sender": "919876543213@s.whatsapp.net",
        "reply_to_id": None,
        "reply_to_preview": None,
    },
]