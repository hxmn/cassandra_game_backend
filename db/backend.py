from datetime import datetime
from typing import List

from cassandra.cqltypes import UUID
from cassandra.query import BatchStatement

import db
import json

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


def save_batch(payload: (str or List[str])):
    """
        Saving batch of events.

    :param payload: new line separated JSONs for events or list of it
    :return:
    """
    assert payload is not None

    if isinstance(payload, str):
        payload = payload.split(sep='\n')

    batch_sessions = BatchStatement()

    for js_str in payload:
        js = json.loads(js_str)
        session_id = UUID(js['session_id'])
        player_id = UUID(js['player_id'])
        ts = datetime.strptime(js['ts'], '%Y-%m-%dT%H:%M:%S')

        if js['event'] == 'start':
            batch_sessions.add(insert_start, (session_id, player_id, js['country'], ts))
        else:
            batch_sessions.add(insert_end, (session_id, player_id, ts))

        session.execute(batch_sessions)
