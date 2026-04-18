from db.queries import build_opportunities_timeline_query


def test_timeline_query_uses_message_timestamp_not_family_created_at():
    sql = str(build_opportunities_timeline_query().compile(compile_kwargs={"literal_binds": True}))

    normalized_sql = " ".join(sql.lower().split())

    assert "message_family_map" in normalized_sql
    assert "messages.timestamp" in normalized_sql
    assert "families.created_at" in normalized_sql
    assert "coalesce" in normalized_sql
    assert "min(messages.timestamp)" in normalized_sql
    assert "group by" in normalized_sql


def test_timeline_query_keeps_limit():
    sql = str(build_opportunities_timeline_query(limit=12).compile(compile_kwargs={"literal_binds": True}))

    assert "limit 12" in sql.lower()