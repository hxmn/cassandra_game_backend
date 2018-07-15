from datetime import datetime, timedelta
from typing import List

from cassandra.cluster import Session
from cassandra.cqltypes import UUID
from cassandra.query import BatchStatement

import json

RFC3339_NO_FRACTION_NO_ZULU = '%Y-%m-%dT%H:%M:%S'

NOW = datetime.strptime('2016-12-02T15:00:00', RFC3339_NO_FRACTION_NO_ZULU)


class Backend:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.insert_start = session.prepare("""
            INSERT INTO sessions 
            (session_id, player_id, country, start, has_start) values 
            (?, ?, ?, ?, True)
        """)

        self.insert_end = session.prepare("""
            INSERT INTO sessions  
            (session_id, player_id, finish, has_end) VALUES 
            (?, ?, ?, True)  
        """)

        self.select_session_starts_for_last_hours = session.prepare("""
            SELECT country, start FROM sessions
            WHERE start > ? ALLOW FILTERING
        """)

        self.insert_complete_start = session.prepare("""
            INSERT INTO complete_sessions 
            (player_id, session_id, ts, has_start) VALUES 
            (?, ?, '', True)
        """)

        self.insert_complete_end = session.prepare("""
            INSERT INTO complete_sessions
            (player_id, session_id, ts) VALUES 
            (?, ?, ?)
        """)

        self.select_last_complete_sessions = session.prepare("""
            SELECT session_id FROM complete_sessions 
            where ts > '' and player_id = ? limit ? ALLOW FILTERING 
        """)

    def save_batch(self, payload: (str or List[str])) -> int:
        """
            Saving batch of events.

        :param payload: new line separated JSONs for events or list of it
        :return: Number of batched statements
        """
        assert payload is not None

        if isinstance(payload, str):
            payload = payload.split(sep='\n')

        batch_sessions, batch_completes = BatchStatement(), BatchStatement()

        for js_str in payload:
            js = json.loads(js_str)
            session_id = UUID(js['session_id'])
            player_id = UUID(js['player_id'])
            ts = datetime.strptime(js['ts'], RFC3339_NO_FRACTION_NO_ZULU)

            if js['event'] == 'start':
                batch_sessions.add(self.insert_start, (session_id, player_id, js['country'], ts))
                batch_completes.add(self.insert_complete_start, (player_id, session_id))
            else:
                batch_sessions.add(self.insert_end, (session_id, player_id, ts))
                batch_completes.add(self.insert_complete_end, (player_id, session_id, ts))

        self.session.execute(batch_sessions)
        self.session.execute(batch_completes)
        num_statements = len(batch_sessions) + len(batch_completes)
        return num_statements

    def session_starts_for_last_hours(self, hours: int) -> dict:
        from_date = NOW - timedelta(hours=hours)
        result = {}

        rows = self.session.execute(self.select_session_starts_for_last_hours, (from_date,))
        for row in rows:
            if row.country not in result.keys():
                result[row.country] = []
            result[row.country].append(row.start.strftime(RFC3339_NO_FRACTION_NO_ZULU))
        return result

    def last_complete_sessions(self, player_id: str, num_sessions=20) -> List[str]:
        rows = self.session.execute(self.select_last_complete_sessions, (player_id, num_sessions))
        sessions = [row.session_id for row in rows]
        return sessions
