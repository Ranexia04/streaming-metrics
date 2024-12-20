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


def create_msg() -> bytes:
    msg = {
        "strtTm": datetime.datetime.now().isoformat(),
        "drtn": random.randint(5, 25),
        "tp": random.choice(tps),
        "systm": random.choice(systems),
        "dmn": random.choice(domains),
        "cmpnnt": random.choice(components),
        "hstnm": random.choice(hostnames),
        "nm": random.choice(nms),
        "oprtn": random.choice(oprtns),
        "isErr": random.choice([True] + [False]*4),
        "sCd": random.choice(scds),
        "nCd": random.choice(ncds),
    }
    msg = json.dumps(msg)
    return msg.encode('utf-8')


def produce() -> Generator[bytes, Any, Any]:
    n_messages = 0
    while n_messages < MAX_MESSAGES:
        yield create_msg()
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
    n_tps: int = 3
    tps: list[str] = [f"TP{i}" for i in range(n_tps)]

    n_systems: int = 3
    systems: list[str] = [f"SYSTEM{i}" for i in range(n_systems)]

    n_domains: int = 10
    domains: list[str] = [f"DOMAIN{i}" for i in range(n_domains)]

    n_components: int = 50
    components: list[str] = [f"COMPONENT{i}" for i in range(n_components)]

    n_hostnames: int = 4
    hostnames: list[str] = [f"HOST{i}" for i in range(n_hostnames)]

    n_nms: int = 5
    nms: list[str] = [f"NM{i}" for i in range(n_nms)]

    n_oprtns: int = 5
    oprtns: list[str] = [f"OPRTN{i}" for i in range(n_oprtns)]

    n_scds: int = 4
    scds: list[str] = [f"SCD{i}" for i in range(n_scds)]

    n_ncds: int = 4
    ncds: list[str] = [f"NCD{i}" for i in range(n_ncds)]

    main()
