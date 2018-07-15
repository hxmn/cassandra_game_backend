import asyncio
from typing import List
import requests as req

URL = 'http://localhost:18080/load_events'
N = 100


def load(batch: List[str]):
    payload = '\n'.join(batch)
    resp = req.post(URL, data=payload)
    print(resp.text)


async def main():
    loop = asyncio.get_event_loop()
    batch = list()
    with open('data.jsonl') as f:
        for line in f:
            batch.append(line)
            if len(batch) == N:
                loop.run_in_executor(None, load, batch)
                batch = []


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
