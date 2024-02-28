#!/usr/bin/env python3

import pulsar


client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe("persistent://public/default/out",
                            subscription_name='my-sub')

while True:
    msg = consumer.receive()
    print("id: {} Received message: {}".format(msg.partition_key(), msg.data()))
    consumer.acknowledge(msg)

client.close()