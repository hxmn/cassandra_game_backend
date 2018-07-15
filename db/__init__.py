from cassandra.cluster import Cluster
from cassandra.metadata import InvalidRequest

KEYSPACE = 'game'

cluster = Cluster()
session = cluster.connect()

try:
    session.set_keyspace(KEYSPACE)
except InvalidRequest:
    print('Key space %s is not initialized' % KEYSPACE)
    print('Run python -m db.__init__ command')


def init_db():
    # Create keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = 
         {'class' : 'SimpleStrategy', 'replication_factor' : 1}
    """ % KEYSPACE)

    session.set_keyspace(KEYSPACE)

    # Denormalized storage of all sessions
    session.execute("""
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

    session.execute("""
        CREATE TABLE IF NOT EXISTS complete_sessions (
            player_id UUID,
            session_id UUID,
            ts TIMESTAMP,
            has_start BOOLEAN,
            PRIMARY KEY ((player_id), ts)
        ) WITH CLUSTERING ORDER BY (ts DESC)
    """)

def clean_db():
    session.execute("""
        DROP KEYSPACE %s    
    """ % KEYSPACE)


if __name__ == '__main__':
    init_db()
    print('Keyspace %s is initialized.' % KEYSPACE)
