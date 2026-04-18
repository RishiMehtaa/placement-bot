from db.queries import build_opportunities_timeline_query


def test_timeline_query_groups_by_message_date():
    sql = str(build_opportunities_timeline_query().compile(compile_kwargs={"literal_binds": True}))

    normalized_sql = " ".join(sql.lower().split())

    assert "message_family_map" in normalized_sql
    assert "messages.timestamp" in normalized_sql
    assert "date(messages.timestamp)" in normalized_sql
    assert "count(messages.message_id)" in normalized_sql
    assert "group by" in normalized_sql


def test_timeline_query_counts_same_day_messages():
    sql = str(build_opportunities_timeline_query().compile(compile_kwargs={"literal_binds": True}))

    normalized_sql = " ".join(sql.lower().split())

    assert "join message_family_map" in normalized_sql
    assert "count(messages.message_id)" in normalized_sql
    assert "group by date(messages.timestamp)" in normalized_sql


def test_timeline_query_keeps_limit():
    sql = str(build_opportunities_timeline_query(limit=12).compile(compile_kwargs={"literal_binds": True}))

    assert "limit 12" in sql.lower()