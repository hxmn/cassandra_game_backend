import pytest
import falcon
from falcon import testing

from db import clean_db, init_db

def setup():
    init_db()


def teardown():
    clean_db()

from rest.app import app

@pytest.fixture()
def client():
    return testing.TestClient(app)


@pytest.mark.run(order = -1)
def test_load_events(client):
    print('load events')

@pytest.mark.run(order = 1)
def test_get_last_sessions(client):
    print('get last sessions')
    resp = client.simulate_get('/last_hours_session_starts', params={'hours': 3})
    assert resp.status == falcon.HTTP_200
