from datetime import datetime, timedelta
from typing import List

from cassandra.cqltypes import UUID
from cassandra.query import BatchStatement

import db
import json

RFC3339_NO_FRACTION_NO_ZULU = '%Y-%m-%dT%H:%M:%S'

NOW = datetime.strptime('2016-12-02T15:00:00', RFC3339_NO_FRACTION_NO_ZULU)

session = db.session

insert_start = session.prepare("""
    INSERT INTO sessions 
    (session_id, player_id, country, start, has_start) values 
    (?, ?, ?, ?, True)
""")

insert_end = session.prepare("""
    INSERT INTO sessions  
    (session_id, player_id, finish, has_end) values 
    (?, ?, ?, True)  
""")

select_session_starts_for_last_hours = session.prepare("""
    SELECT country, start FROM sessions
    WHERE start > ? ALLOW FILTERING
""")


def save_batch(payload: (str or List[str])) -> int:
    """
        Saving batch of events.

    :param payload: new line separated JSONs for events or list of it
    :return: Number of batched statements
    """
    assert payload is not None

    if isinstance(payload, str):
        payload = payload.split(sep='\n')

    batch_sessions = BatchStatement()

    for js_str in payload:
        js = json.loads(js_str)
        session_id = UUID(js['session_id'])
        player_id = UUID(js['player_id'])
        ts = datetime.strptime(js['ts'], RFC3339_NO_FRACTION_NO_ZULU)

        if js['event'] == 'start':
            batch_sessions.add(insert_start, (session_id, player_id, js['country'], ts))
        else:
            batch_sessions.add(insert_end, (session_id, player_id, ts))

    session.execute(batch_sessions)
    return len(batch_sessions)


def session_starts_for_last_hours(hours: int) -> dict:
    from_date = NOW - timedelta(hours=hours)
    result = {}

    rows = session.execute(select_session_starts_for_last_hours.bind((from_date,)))
    for row in rows:
        if row.country not in result.keys():
            result[row.country] = []
        result[row.country].append(row.start.strftime(RFC3339_NO_FRACTION_NO_ZULU))
    return result
