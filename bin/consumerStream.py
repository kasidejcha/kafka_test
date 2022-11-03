#!/usr/bin/env python

"""Consumes stream for printing all messages to the console.
"""

import argparse
import json
import sys
import time
import socket
from confluent_kafka import Consumer, KafkaError, KafkaException
import pandas as pd
from sqlalchemy import create_engine


def msg_process(msg, col):
    # Print the current time and the message.
    time_start = time.strftime("%Y-%m-%d %H:%M:%S")
    val = msg.value()
    dval = json.loads(val)
    df = pd.DataFrame(dval).T
    df.columns = col
    return df


def find_pair(test_out, card, time, route):
    out = test_out[(test_out['Card No.']==card) & (test_out['Receiving Time']>=time)]
    if len(out) == 0:
        station = 0
    else:
        out = out.iloc[0]
        station = out['Merchant order number']
    return station


def total_dis(dis, start, end, route, direction):
    route = int(route)
    # print(dis[(dis.station == start) & (dis.routes == route) & (dis.direction == direction)])
    start_idx = dis[(dis.station == start) & (dis.routes == route) & (dis.direction == direction)].index.tolist()[0]
    end_idx = dis[(dis.station == end) & (dis.routes == route) & (dis.direction == direction)].index.tolist()[0]
    if start_idx < end_idx:
        distance = dis[(dis.index>=start_idx) & (dis.index <= end_idx)].distance_km.sum()
    else:
        distance = dis[(dis.index<=start_idx) & (dis.index >= end_idx)].distance_km.sum()
    return distance


def main(dis):
    # Init
    col = ['NO', 'TRANS channel', 'TRANS Type', 'Merchant', 'Route Name',
       'Plate No.', 'Device ID', 'Serial No.', 'Card No.', 'Card Type',
       'Amount before transaction', 'Receivable', 'Autual Amount',
       'TRANS Date', 'TRANS Time', 'Receiving Time', 'Boarding Sign',
       'Station Name', 'Station Total', 'Direction', 'Merchant order number',
       'Reason for refusal', 'Liquidation status', 'TRANS Status', 'STATUS']
    station_col = ['Card No.','Route Name','Station Name', 'end_station', 'km']
    df = pd.DataFrame([], columns=col)
    output = pd.DataFrame([], columns=station_col)

    conf = {'bootstrap.servers': 'localhost:9092',
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'group.id': socket.gethostname()}
    topic = 'sample_topic'
    consumer = Consumer(conf)
    running = True

    username = 'test'
    password = 'postgres'
    host = '10.15.5.25'
    port = '5432'
    database = 'test'
    table_name = 'divine_trans'
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')
    conn = engine.connect()

    batch_wait = 10
    start_time = time.time()
    try:
        while running:
            consumer.subscribe([topic])
            # consumer.consume(num_messages=1000)
            msg = consumer.poll(1)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    sys.stderr.write('Topic unknown, creating %s topic\n' %
                                     (topic))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                tmp = msg_process(msg, col)
                df = pd.concat([df,tmp],axis=0)
                # test_out = df[df['Boarding Sign']=='OUT']

                end_time = time.time()
                time_diff = end_time-start_time
                # if len(test_out)==0 or time_diff<batch_wait:
                if time_diff<batch_wait:
                    continue;
                else:
                    start_time = time.time()
                    print(len(df))
                    # # preprocess
                    # test_in = df[df['Boarding Sign']=='IN'].sort_values('Receiving Time')
                    # test_out = test_out.sort_values('Receiving Time')
                    # # find match
                    # test_in['merchant_order_num_off'] = test_in.apply(lambda row: find_pair(test_out, row['Card No.'], row['Receiving Time'], row['Route Name']),axis=1)                
                    # station = test_in[test_in.merchant_order_num_off != 0]
                    # test_out.rename(columns= {'Merchant order number':'merchant_order_num_off'},inplace=True)
                    # out = test_out[['Station Name', 'Receiving Time', 'merchant_order_num_off']]
                    # out.columns = ['end_station', 'receiving_time_off', 'merchant_order_num_off']
                    # station = station.merge(out,how='left', on='merchant_order_num_off')
                    # # find kilometer
                    # station['km'] = station.apply(lambda row : total_dis(dis, row['Station Name'], row['end_station'], row['Route Name'], 1), axis=1)

                    # output_tmp = station[['NO', 'Receiving Time', 'Merchant order number', 
                    #                 'Merchant','Plate No.', 'Route Name', 'Direction', 'Station Name', 
                    #                 'Card No.', 'end_station', 'receiving_time_off', 'merchant_order_num_off', 'km']]
                    # output_tmp.columns = ['no', 'receiving_time_on', 'merchant_order_number_on', 
                    # 'merchat', 'plate_no', 'route', 'direction', 'start_station', 
                    # 'card_no', 'end_station', 'receiving_time_off', 'merchant_order_num_off', 'km']
                    # # output = pd.concat([output_tmp,out], axis=0)
                    # # print(output_tmp)
                    # df = df[~df['Card No.'].isin(output_tmp['card_no'].tolist())]

                    # # upload
                    # output_tmp.to_sql(f'{table_name}', conn, if_exists='append')
                

    except KeyboardInterrupt:
        pass

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == "__main__":
    dis = pd.read_csv('/Users/kasidej/Documents/EA/divine/transaction/data/distance_station.csv')
    main(dis)
