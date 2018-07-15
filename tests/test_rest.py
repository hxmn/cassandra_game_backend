import pytest
import falcon
from falcon import testing

from rest.app import app


@pytest.fixture()
def client():
    return testing.TestClient(app)


def test_get_last_sessions(client):
    resp = client.simulate_get('/last_hours_session_starts', params={'hours': 3})
    assert resp.status == falcon.HTTP_200
