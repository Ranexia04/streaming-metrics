#!/usr/bin/env python3

import random
import time
from typing import Any, Generator
import pulsar
import datetime
import sys

# MAX_MESSAGE : float = float("inf")
MAX_MESSAGES_DEFAULT : float = float("inf")
MAX_MESSAGES : float = float(sys.argv[1]) if len(sys.argv) > 1 else MAX_MESSAGES_DEFAULT

def callback(res, mes_id):
    pass

def create_msg(phase: int, code: str, domain: str, start_time: datetime.datetime, cluster: str, host: str) -> bytes:
    msg = '{"phase": %d, "code": "%s", "domain": "%s", "start_time": "%s", "cluster": "%s", "host": "%s"}' % (phase, code, domain, start_time.isoformat(), cluster, host)
    return msg.encode('utf-8')

def ERR1() -> bytes:
    return create_msg(1, "ERR1", "XPTO", datetime.datetime.now(datetime.timezone.utc), "sada", "a")

def ERR2() -> bytes:
    return create_msg(1, "ERR2", "XPTO", datetime.datetime.now(datetime.timezone.utc), "adasd", "a")

def ERR3() -> bytes:
    return create_msg(1, "ERR3", "XPTA", datetime.datetime.now(datetime.timezone.utc), "adasdas", "dasdsab")

def ERR4() -> bytes:
    return create_msg(1, "ERR4", "XPTA", datetime.datetime.now(datetime.timezone.utc), "adasdas", "dasdsab")

def ERR5() -> bytes:
    return create_msg(1, "ERR5", "XPTA", datetime.datetime.now(datetime.timezone.utc), "adasdas", "dasdsab")

def ERR6() -> bytes:
    return create_msg(1, "ERR6", "XPTA", datetime.datetime.now(datetime.timezone.utc), "adasdas", "dasdsab")

def produce() -> Generator[bytes, Any, Any]:
    options = (0,1,2,3,4,5,6)
    n_messages = 0.0
    while n_messages < MAX_MESSAGES:
        match random.choice(options):
            case 0:
                yield ERR1()
            case 1:
                yield ERR2()
            case 2:
                yield ERR2()
            case 3:
                yield ERR3()
            case 4:
                yield ERR4()
            case 5:
                yield ERR5()
            case 6:
                yield ERR6()
        n_messages += 1

def logic(producer: pulsar.Producer) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    c = 0
    for i, msg in enumerate(produce()):
        producer.send_async(content=msg, partition_key="key=%d" % (i,), callback=callback)
        c+=1

        time.sleep(random.random()/100)

        # if time.time().

        later = datetime.datetime.now(datetime.timezone.utc)
        diff = (later - now).total_seconds()
        if diff > 5:
            now = later
            print(f"{now}: Sent rate {c/float(diff):0.2f}, total {i}")
            c = 0

        # if i > 100_000_000:
        #     break


def work() -> None:
    client = pulsar.Client('pulsar://localhost:6650')
    producer = client.create_producer("persistent://public/default/in",
                block_if_queue_full=True,
                batching_enabled=True,
                batching_max_publish_delay_ms=1000,
                batching_max_messages=1000,
            )

    logic(producer)

    producer.flush()
    client.close()



if __name__=="__main__":
    #print(datetime.datetime.now(datetime.timezone.utc).isoformat())
    work()
