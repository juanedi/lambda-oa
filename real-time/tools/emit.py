#!/usr/bin/env python

import time
import datetime
import random
import pika
import sys

strs = [
          "20130510162904,j,batman",
          "20130510162904,El Eternestor,Tu vieja"
]

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='frags', type='fanout', auto_delete=False, durable=True)
c = 0


while True:
    message = random.choice(strs)
    
    channel.basic_publish(
        exchange='frags',
        routing_key='',
        body=message,
        properties = pika.BasicProperties(timestamp=time.time()))
    c = c + 1
    if c == 10000:
        time.sleep(1)
        c = 0
        print "sleeping"
connection.close()
