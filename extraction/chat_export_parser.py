from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import re
from typing import Optional


@dataclass
class ChatExportParseResult:
    messages: list[dict]
    total_lines: int
    parsed_messages: int
    skipped_lines: int


_LINE_PATTERNS = [
    re.compile(
        r"^\[(?P<date>[^\],]+),\s*(?P<time>[^\]]+)\]\s*(?P<sender>[^:]+):\s?(?P<text>.*)$"
    ),
    re.compile(
        r"^(?P<date>\d{1,2}[/-]\d{1,2}[/-]\d{2,4}),\s*"
        r"(?P<time>\d{1,2}:\d{2}(?::\d{2})?\s?(?:[APap][Mm])?)\s*-\s*"
        r"(?P<sender>[^:]+):\s?(?P<text>.*)$"
    ),
]


_DATE_FORMATS = [
    "%d/%m/%y",
    "%d/%m/%Y",
    "%m/%d/%y",
    "%m/%d/%Y",
    "%d-%m-%y",
    "%d-%m-%Y",
    "%m-%d-%y",
    "%m-%d-%Y",
]

_TIME_FORMATS = [
    "%H:%M",
    "%H:%M:%S",
    "%I:%M %p",
    "%I:%M:%S %p",
]


def _parse_timestamp(date_text: str, time_text: str) -> Optional[datetime]:
    date_text = date_text.strip()
    time_text = re.sub(r"\s+", " ", time_text.strip().upper())

    for date_fmt in _DATE_FORMATS:
        for time_fmt in _TIME_FORMATS:
            try:
                parsed = datetime.strptime(f"{date_text} {time_text}", f"{date_fmt} {time_fmt}")
                return parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def _to_message_dict(import_hash: str, index: int, msg: dict, fallback_base_time: datetime) -> dict:
    timestamp = msg.get("timestamp") or (fallback_base_time + timedelta(seconds=index))
    text = (msg.get("text") or "").strip()
    sender = (msg.get("sender") or "").strip() or None

    return {
        "message_id": f"import_{import_hash}_{index:06d}",
        "text": text,
        "timestamp": timestamp,
        "sender": sender,
        "reply_to_id": None,
        "reply_to_preview": None,
    }


def parse_chat_export_text(raw_text: str) -> ChatExportParseResult:
    normalized = raw_text.replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized.split("\n")
    import_hash = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]

    parsed_messages: list[dict] = []
    current_message: Optional[dict] = None
    skipped_lines = 0

    for line in lines:
        clean_line = line.lstrip("\ufeff")
        if not clean_line.strip():
            if current_message and current_message.get("text"):
                current_message["text"] += "\n"
            continue

        matched = None
        for pattern in _LINE_PATTERNS:
            matched = pattern.match(clean_line)
            if matched:
                break

        if matched:
            if current_message and (current_message.get("text") or "").strip():
                parsed_messages.append(current_message)

            timestamp = _parse_timestamp(matched.group("date"), matched.group("time"))
            current_message = {
                "timestamp": timestamp,
                "sender": matched.group("sender").strip(),
                "text": matched.group("text").strip(),
            }
            continue

        if current_message:
            # Continuation line for multi-line exported messages.
            current_message["text"] += f"\n{clean_line}" if current_message["text"] else clean_line
        else:
            skipped_lines += 1

    if current_message and (current_message.get("text") or "").strip():
        parsed_messages.append(current_message)

    fallback_base_time = datetime.now(timezone.utc)
    final_messages = [
        _to_message_dict(import_hash=import_hash, index=i, msg=msg, fallback_base_time=fallback_base_time)
        for i, msg in enumerate(parsed_messages, start=1)
        if (msg.get("text") or "").strip()
    ]

    return ChatExportParseResult(
        messages=final_messages,
        total_lines=len(lines),
        parsed_messages=len(final_messages),
        skipped_lines=skipped_lines,
    )
