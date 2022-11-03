#!/usr/bin/env python

"""Generates a stream to Kafka from a time series csv file.
"""

import argparse
import csv
import json
import sys
import time
from dateutil.parser import parse
from confluent_kafka import Producer
import socket


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))


def main():
    topic = 'sample_topic'
    file_name = '/Users/kasidej/Documents/EA/divine/transaction/data/mock_data.csv'
    p_key = file_name
    # speed = 0.2
    
    conf = {'bootstrap.servers': "localhost:9092",
            'client.id': socket.gethostname()}
    producer = Producer(conf)

    rdr = csv.reader(open(file_name))
    next(rdr)  # Skip header
    firstline = True

    while True:

        try:

            if firstline is True:
                line1 = next(rdr)
                value = line1
                # Convert csv columns to key value pair
                result = {}
                # row = 0
                result = value
                # Convert dict to json as message format
                jresult = json.dumps(result)
                firstline = False
                producer.produce(topic, key=p_key, value=jresult, callback=acked)

            else:
                line = next(rdr, None)
                # time.sleep(speed)
                value = line
                if value == None:
                    sys.exit()
                result = {}
                # row += 1
                result = value
                jresult = json.dumps(result)
                producer.produce(topic, key=p_key, value=jresult, callback=acked)

            producer.flush()

        except TypeError:
            sys.exit()


if __name__ == "__main__":
    main()
