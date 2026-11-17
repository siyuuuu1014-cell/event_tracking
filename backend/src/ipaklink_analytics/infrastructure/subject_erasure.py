import json

# Serializes a subject purge with receipt creation and warehouse writes.
SUBJECT_DATA_LOCK = 345972154
UNFINISHED = "SELECT EXISTS (SELECT 1 FROM analytics_subject_erasure WHERE status<>'succeeded')"
SUPPRESSED = "SELECT DISTINCT actor_key FROM analytics_subject_erasure WHERE actor_key=ANY(%s)"


def row_subjects(row):
    subjects = {row[8]} if row[8] else set()
    if row[1] == "search_result_open":
        properties = json.loads(row[24])
        if properties.get("targetType") == "user" and properties.get("targetId"):
            subjects.add(properties["targetId"])
    return subjects


def filter_rows(pg, rows):
    candidates = [row_subjects(row) for row in rows]
    subjects = set().union(*candidates)
    if not subjects:
        return rows
    blocked = {row[0] for row in pg.execute(SUPPRESSED,(list(subjects),)).fetchall()}
    return [row for row, keys in zip(rows,candidates,strict=True) if keys.isdisjoint(blocked)]


def persist_events(pg, warehouse, rows, columns):
    if not rows:
        return 0
    with pg.transaction():
        pg.execute("SELECT pg_advisory_xact_lock_shared(%s)",(SUBJECT_DATA_LOCK,))
        kept = filter_rows(pg,rows)
        if kept:
            warehouse.insert("events_raw",kept,column_names=columns)
    return len(kept)
