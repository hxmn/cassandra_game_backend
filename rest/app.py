import falcon

class LoadEvents(object):
    pass

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