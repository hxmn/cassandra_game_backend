import requests as req

# Integration tests

URL = "http://localhost:18080"

def test_batch_loading():
    payload = """{"player_id": "d6313e1fb7d247a6a034e2aadc30ab3f", "country": "PK", "event": "start", "session_id": "674606b1-2270-4285-928f-eef4a6b90a60", "ts": "2016-11-22T20:40:50"}
{"player_id": "20ac16ebb30a477087c3c7501b1fce73", "event": "end", "session_id": "16ca9d01-d240-4527-9f8f-00ef6cddb1d4", "ts": "2016-11-18T06:24:50"}
{"player_id": "318e22b061b54042b880c365c28982d0", "event": "end", "session_id": "5f933591-8cd5-4147-8736-d6237bef5891", "ts": "2016-11-16T18:01:37"}
{"player_id": "29bb390d9b1b4b4b9ec0d6243da34ec4", "event": "end", "session_id": "ef939180-692a-4845-aef7-afb03524c2da", "ts": "2016-11-13T10:38:09"}
{"player_id": "a477ecabc3cc455cb1c6d1dab77d8e5c", "country": "GH", "event": "start", "session_id": "4c55263e-66b2-4814-b431-8ca4c1a9dcc8", "ts": "2016-11-29T19:31:43"}
{"player_id": "1ec36a67785046b3bce1dc432fad9129", "country": "SK", "event": "start", "session_id": "3346a60a-0989-4041-aacc-cf6ff44bd151", "ts": "2016-11-16T05:36:16"}
{"player_id": "9595af0063e94cb8a76cb6628c6b80eb", "country": "DE", "event": "start", "session_id": "06830030-d091-428b-87d6-53914d3d2a18", "ts": "2016-11-07T01:18:09"}
{"player_id": "8d0e3cd4a25d4a0895a6c2e13b5bb26a", "event": "end", "session_id": "a78a4889-4bcf-45a7-a4bd-967cc7adf581", "ts": "2016-11-24T02:12:33"}
{"player_id": "e59f1fa31e144fd8b3634f397492126a", "event": "end", "session_id": "dd223ea6-0e6b-4dd2-bc1d-b2decd43aabf", "ts": "2016-11-13T00:35:30"}
{"player_id": "fd8a1e9fff25471dad3e8ab951c90d60", "event": "end", "session_id": "3015bf71-4b28-4c91-a253-b48607170a1e", "ts": "2016-11-21T01:18:57"}"""


    resp = req.post("%s/load_events" % URL, data=payload)
    print(resp.text)
    assert resp.status_code == 200

def test_last_sessions_starts():
    resp = req.get("%s/last_hours_session_starts?hours=200000" % URL)
    js = resp.json()
    print(js)
