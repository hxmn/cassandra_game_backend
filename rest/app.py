from wsgiref import simple_server

import falcon

from db.backend import save_batch, session_starts_for_last_hours, last_complete_sessions


class LoadEvents(object):
    def on_post(self, req, res):
        body = req.stream.read()
        if not body:
            raise falcon.HTTPBadRequest('Empty request body',
                                        'A valid payload is required.')
        payload = body.decode('utf-8')
        num_of_statements = save_batch(payload)

        res.body = f'{num_of_statements} statements are executed'
        res.status = falcon.HTTP_200


class GetSessionStartsForLastHours(object):
    def on_get(self, req, res):
        hours = int(req.params['hours'])
        starts_by_country = session_starts_for_last_hours(hours)
        res.body = falcon.json.dumps(starts_by_country)
        res.status = falcon.HTTP_200


class GetLastCompleteSessionsByPlayer(object):
    def on_get(self, req, res):
        player_id = req.params['player_id']
        sessions = last_complete_sessions(player_id)
        res.body = falcon.json.dumps(sessions)
        res.status = falcon.HTTP_200


app = falcon.API()
app.add_route('/load_events', LoadEvents())
app.add_route('/last_hours_session_starts', GetSessionStartsForLastHours())
app.add_route('/last_complete_sessions', GetLastCompleteSessionsByPlayer())

if __name__ == '__main__':
    httpd = simple_server.make_server('127.0.0.1', 18080, app)
    httpd.serve_forever()
