#!/usr/bin/env python

import time
import pika
from collections import defaultdict

#hdfs = pydoop.hdfs.fs.hdfs(host='mina')

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.exchange_declare(exchange='frags', type='fanout', auto_delete=False, durable=True)

result = channel.queue_declare(exclusive=True)

queue_name = result.method.queue

channel.queue_bind(exchange='frags', queue=queue_name)

games = defaultdict(list)

mc = []

first_ts = []

def callback(ch, method, properties, body):
    ts = properties.timestamp
    if len(first_ts) == 0:
        first_ts.append(ts)
    else:
        first_ts[0] = min(first_ts + [ts])
    mc.append(body)
    
    if len(mc) == 1000:
        now_file = "%d_%d"%(first_ts[0], ts)
        #with hdfs.open("/frags/" + now_file) as fi:
        with open("/tmp/hashit/" + now_file, "w") as fi:
            for record in mc:
                fi.write(record)
            fi.close()
        del first_ts[:]
        del mc[:]

channel.basic_consume(callback, queue=queue_name, no_ack=True)

channel.start_consuming()
