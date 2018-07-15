from typing import Optional

from cassandra.cluster import Cluster, Session
from cassandra.metadata import InvalidRequest

KEYSPACE = 'game'

_cluster = Cluster()
_session = _cluster.connect()


def get_session(init=False) -> Optional[Session]:
    try:
        _session.set_keyspace(KEYSPACE)
    except InvalidRequest:
        if init:
            init_db()
        else:
            print('Key space %s is not initialized' % KEYSPACE)
            print('Run init_db() command')
            return None
    return _session


def init_db():
    # Create keyspace
    _session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = 
         {'class' : 'SimpleStrategy', 'replication_factor' : 1}
    """ % KEYSPACE)

    _session.set_keyspace(KEYSPACE)

    # Denormalized storage of all sessions
    _session.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            player_id UUID, 
            session_id UUID, 
            country TEXT, 
            start TIMESTAMP, 
            finish TIMESTAMP,
            has_start BOOLEAN,
            has_end BOOLEAN,
            PRIMARY KEY (player_id, session_id)
        ) 
    """)

    # Storage for complete sessions (We can receive complete sessions with sessions table, but
    #       we want to have it ordered by timestamp, so it can be done with another table)
    _session.execute("""
        CREATE TABLE IF NOT EXISTS complete_sessions (
            player_id UUID,
            session_id UUID,
            ts TIMESTAMP,
            has_start BOOLEAN,
            PRIMARY KEY ((player_id), ts)
        ) WITH CLUSTERING ORDER BY (ts DESC)
    """)


def clean_db():
    _session.execute("""
        DROP KEYSPACE %s    
    """ % KEYSPACE)


if __name__ == '__main__':
    init_db()
    print('Keyspace %s is initialized.' % KEYSPACE)
