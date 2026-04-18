from extraction.chat_export_parser import parse_chat_export_text


def test_parses_common_whatsapp_lines():
    raw = (
        "18/04/26, 10:31 PM - Alex: TCS hiring for SDE role\n"
        "18/04/26, 10:32 PM - Priya: Deadline is 25 April\n"
    )

    result = parse_chat_export_text(raw)

    assert result.parsed_messages == 2
    assert result.skipped_lines == 0
    assert result.messages[0]["sender"] == "Alex"
    assert "TCS hiring" in result.messages[0]["text"]
    assert result.messages[0]["message_id"].startswith("import_")


def test_parses_bracket_timestamp_format():
    raw = "[18/04/2026, 22:31:00] Team Bot: Amazon internship opening"

    result = parse_chat_export_text(raw)

    assert result.parsed_messages == 1
    assert result.messages[0]["sender"] == "Team Bot"
    assert result.messages[0]["text"] == "Amazon internship opening"


def test_keeps_multiline_message_body():
    raw = (
        "18/04/26, 10:31 PM - Alex: TCS hiring\n"
        "Role: SDE\n"
        "Link: https://example.com/apply\n"
        "18/04/26, 10:35 PM - Priya: Thanks\n"
    )

    result = parse_chat_export_text(raw)

    assert result.parsed_messages == 2
    assert "Role: SDE" in result.messages[0]["text"]
    assert "Link: https://example.com/apply" in result.messages[0]["text"]


def test_skips_non_message_header_lines():
    raw = (
        "Messages and calls are end-to-end encrypted.\n"
        "18/04/26, 10:31 PM - Alex: Infosys drive announced\n"
    )

    result = parse_chat_export_text(raw)

    assert result.parsed_messages == 1
    assert result.skipped_lines >= 1


def test_generates_deterministic_message_ids_for_same_input():
    raw = "18/04/26, 10:31 PM - Alex: Hiring now"

    first = parse_chat_export_text(raw)
    second = parse_chat_export_text(raw)

    assert first.messages[0]["message_id"] == second.messages[0]["message_id"]
