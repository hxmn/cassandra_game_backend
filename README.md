# Python REST service for saving game information into Cassndra

## Installation and Running
Docker compose is used to configure environment and run application. 
To build and run application use:

```bash
docker-compose build
docker-compose up
```

## Using
Load data:
```text
$ sed -n '1,100 p' data.jsonl | http localhost:18080/load_events     
HTTP/1.1 200 OK
Connection: close
Date: Sun, 15 Jul 2018 17:36:06 GMT
Server: gunicorn/19.9.0
content-length: 27
content-type: application/json; charset=UTF-8

101 statements are executed
```

Fetching sessions starts for the last X hours for each country:
```text
$ http localhost:18080/last_hours_session_starts\?hours=60
HTTP/1.1 200 OK
Connection: close
Date: Sun, 15 Jul 2018 17:38:37 GMT
Server: gunicorn/19.9.0
content-length: 93
content-type: application/json; charset=UTF-8

{
    "AG": [
        "2016-11-30T10:35:35"
    ],
    "CD": [
        "2016-12-01T15:58:38"
    ],
    "HK": [
        "2016-11-30T12:27:13"
    ]
}
```

Fetching last 20 complete sessions for given player:
```text
$ http localhost:18080/last_complete_sessions\?player_id=42694017-efdd-4e5a-b7ce-34476a3e14ab
HTTP/1.1 200 OK
Connection: close
Date: Sun, 15 Jul 2018 17:49:28 GMT
Server: gunicorn/19.9.0
content-length: 40
content-type: application/json; charset=UTF-8

[
    "c6b04421-b053-40f2-8599-ffb51a2e3f5a"
]

```