import falcon

from db.backend import save_batch


class LoadEvents(object):
    def on_post(self, req, res):
        body = req.stream.read()
        if not body:
            raise falcon.HTTPBadRequest('Empty request body',
                                        'A valid payload is required.')
        payload = body.decode('utf-8')
        save_batch(payload)

class GetSessionsForLastHours(object):
    def on_get(self, req, res):
        res.status = falcon.HTTP_200
        res.body = 'last sessions'

class GetLastCompleteSessionsByPlayer(object):
    def on_get(self, req, res):
        res.status = falcon.HTTP_200
        res.body = 'last complete sessions'

app = falcon.API()
app.add_route('/load_events', LoadEvents())
app.add_route('/last_hours_sessions', GetSessionsForLastHours())
app.add_route('/last_complete_sessions', GetLastCompleteSessionsByPlayer())