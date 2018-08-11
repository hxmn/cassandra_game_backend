import asyncio
from typing import List

import requests as req

# Our REST API endpoint for loading data
URL = 'http://localhost:18080/load_events'
N = 50  # batch size


def load(batch: List[str]) -> None:
    payload = '\n'.join(batch)
    resp = req.post(URL, data=payload)
    print(resp.text)


async def main() -> None:
    loop = asyncio.get_event_loop()
    batch = list()
    with open('data.jsonl') as f:
        for line in f:
            batch.append(line)
            if len(batch) == N:
                loop.run_in_executor(None, load, batch)
                batch = []


# Asynchronous loading of the data. It utilizes 8 cores of my laptop.
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
