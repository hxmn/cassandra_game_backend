from db import init_db, KEYSPACE

init_db()
print('Keyspace %s is initialized.' % KEYSPACE)
