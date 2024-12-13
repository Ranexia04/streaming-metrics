#!/usr/bin/env python3

import random
from typing import Any, Generator
import json
import datetime
import sys

import pulsar

# MAX_MESSAGE : float = float("inf")
MAX_MESSAGES_DEFAULT : float = float("inf")
MAX_MESSAGES : float = float(sys.argv[1]) if len(sys.argv) > 1 else MAX_MESSAGES_DEFAULT


def callback(res, mes_id):
    pass


def create_msg(code: str, domain: str, start_time: datetime.datetime, hostname: str) -> bytes:
    msg = {
        "code": code,
        "domain": domain,
        "strtTm": start_time.isoformat(),
        "hstnm": hostname
    }
    msg = json.dumps(msg)
    return msg.encode('utf-8')


def produce() -> Generator[bytes, Any, Any]:
    n_messages = 0
    while n_messages < MAX_MESSAGES:
        yield create_msg(random.choice(statuses), random.choice(domains), datetime.datetime.now(datetime.timezone.utc), random.choice(hostnames))
        n_messages += 1


def logic(producer: pulsar.Producer) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    c = 0
    for i, msg in enumerate(produce()):
        producer.send_async(content=msg, callback=callback)
        c+=1

        later = datetime.datetime.now(datetime.timezone.utc)
        diff = (later - now).total_seconds()
        if diff > 5:
            now = later
            print(f"{now}: Sent rate {c/float(diff):0.2f}, total {i}")
            c = 0


def main() -> None:
    client = pulsar.Client('pulsar://localhost:6650')
    producer = client.create_producer("persistent://public/default/in",
                block_if_queue_full=True,
                batching_enabled=True,
                batching_max_publish_delay_ms=1000,
                batching_max_messages=1000)

    logic(producer)

    producer.flush()
    client.close()


if __name__=="__main__":
    n_domains: int = 1
    domains: list[str] = [f"GROUP{i}" for i in range(n_domains)]

    n_statuses: int = 10
    statuses: list[str] = [f"STATUS{i}" for i in range(n_statuses)]

    n_hostnames: int = 4
    hostnames: list[str] = [f"HOST{i}" for i in range(n_hostnames)]

    main()
