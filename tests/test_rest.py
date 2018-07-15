import json
import random
import uuid
from datetime import datetime, timedelta

import pytest
import falcon
from falcon import testing

from db import clean_db, init_db, backend
from db.backend import date2str

N_PLAYERS = 10
N_SESSIONS = 25
COUNTRIES = ["AA", "BB"]
LAST_HOURS = 5


def setup_module():
    init_db()
    backend.NOW = datetime.now()


def teardown_module():
    clean_db()


@pytest.fixture(scope='module')
def players_and_sessions(num_players=N_PLAYERS, num_sessions=N_SESSIONS):
    players = {}
    for i in range(num_players):
        player_id = uuid.uuid4().hex
        players[player_id] = []
        for j in range(num_sessions):
            players[player_id].append(str(uuid.uuid4()))
    return players


@pytest.fixture(scope='module')
def client():
    from rest.app import app
    return testing.TestClient(app)


@pytest.mark.run(order=0)
def test_load_events(client, players_and_sessions):
    now = datetime.now()

    for player_id, session_ids in players_and_sessions.items():
        country = random.choice(COUNTRIES)
        batch = ""
        counter = 0
        for session_id in session_ids:
            if counter != 0:  # remove first start event
                start_event = {
                    "event": "start",
                    "country": country,
                    "player_id": player_id,
                    "session_id": session_id,
                    "ts": date2str(now - timedelta(hours=counter))
                }
                batch += json.dumps(start_event) + '\n'
            if counter != N_SESSIONS - 1:  # remove last end event
                end_event = {
                    "event": "end",
                    "player_id": player_id,
                    "session_id": session_id,
                    "ts": date2str(now)
                }
                batch += json.dumps(end_event) + '\n'
            counter += 1
        resp = client.simulate_post('/load_events', body=batch)
        assert resp.status == falcon.HTTP_200


@pytest.mark.run(order=1)
def test_get_last_sessions(client):
    resp = client.simulate_get('/last_hours_session_starts', params={'hours': LAST_HOURS})
    js = resp.json

    assert resp.status == falcon.HTTP_200
    assert len(js['AA']) + len(js['BB']) == (LAST_HOURS - 1) * N_PLAYERS

@pytest.mark.run(order=1)
def test_get_complete_sessions(client, players_and_sessions):
    complete_sessions = 0
    for player_id in players_and_sessions.keys():
        resp = client.simulate_get('/last_complete_sessions',
                                   params={'player_id': player_id})
        js = resp.json
        complete_sessions += len(js)

    sessions = 22 if N_SESSIONS > 22 else N_SESSIONS # we are getting only 20 complete sessions
    assert N_PLAYERS * (sessions - 2) == complete_sessions