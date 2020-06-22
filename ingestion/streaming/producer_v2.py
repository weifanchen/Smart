import json
import pandas as pd
import datetime
import numpy as np
import random
from kafka import KafkaProducer
import boto3 
from time import sleep
from sys import argv
import csv

''' 
producer_v2
stimulate electricity usage event
event structure
'timestamp', 'machine_id', 'household_id', running, 'usage'
'''


def connect_kafka_producer(bootstrap_servers):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def read_profile_from_s3(s3,bucketname,_file):
    obj = s3.Object(bucketname, _file)
    body = obj.get()['Body'].read()
    data = json.loads(body.decode('utf-8'))
    return data

def initialize_events(start_time,households,history_stat,topic_name,producer):
    events = list()
    for household in households:
        event = dict()
        event['timestamp'] = start_time #string format
        event['household_id'] = household['household_id']
        event['usage'] = list()
        for machine in household['machines_info']:
            usage = dict()
            minn = history_stat[machine['machine_type']]['3%']
            maxx = history_stat[machine['machine_type']]['97%']
            usage['running'] = random.choice([True,False])
            usage['machine_id'] = machine['machine_id']
            usage['usage'] = usage['running'] * round(random.uniform(minn, maxx),4)
            #usage_name = 'DE_KN_{}_{}'.format(machine['machine_id'],machine['machine_type']) 
            #usage[usage_name] = usage['running'] * round(random.uniform(minn, maxx),4)
            event['usage'].append(usage)
        #ack = producer.send(topic_name,event) 
        events.append(event)
    return events

def following_events(date_str,previous_events,topic_name,producer):
    margin = 0.1
    for event in previous_events: # = for each household
        event['timestamp'] = date_str
        for usage in event['usage']: # = usage of each machine
            usage['running'] = usage['running'] and random.random()>=0.9
            usage['usage'] = usage['running'] * usage['usage'] * random.uniform(1-margin,1+margin) # increase/decrease 10%
            usage['machine_id'] = machine['machine_id']
            if event['running'] and random.random()>=0.99: # abnormal usage
                event['usage'] = event['usage']*2  

        
        event['usage'] = event['running'] * event['usage'] * random.uniform(1-margin,1+margin)
            if event['running'] and random.random()>=0.99: # abnormal usage
                event['usage'] = event['usage']*2  

        ack = producer.send(topic_name,event) 
    return previous_events
    

def main():
    bootstrap_servers = ['localhost:9092']
    topic_name = 'Usage'
    kafka_producer=connect_kafka_producer(bootstrap_servers)
    kafka_producer = []
    current_timestamp=datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    first_events = initialize_events(date_str,households,history_stat,topic_name,kafka_producer)
    time_delta = datetime.timedelta(seconds=1)
    events = first_events
    while True:
        current_timestamp += time_delta 
        date_str = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        temp = following_events(date_str,events,topic_name,kafka_producer)
        events = temp
        sleep(sleep_time)


if __name__ == "__main__":
    #random.seed(42)
    date_str = argv[0] if len(argv.len) else '2016-01-01 01:00:00'
    sleep_time = argv[1] if len(argv.len) else 1
    with open('./config.json') as cf:
    config = json.load(cf)

    ACCESS_ID = config['ACCESS_ID']
    ACCESS_KEY = config['ACCESS_KEY']
    s3 = boto3.resource('s3',aws_access_key_id=ACCESS_ID,aws_secret_access_key= ACCESS_KEY)
    bucketname = 'electricity-data2'
    #machine_file= 'machine_profile_1.json'
    household_file = 'household_profile_1.json'
    stat_file = 'stat.json'
    #machines = read_profile_from_s3(s3,bucketname,machine_file)
    households = read_profile_from_s3(s3,bucketname,household_file)
    history_stat = read_profile_from_s3(s3,bucketname,stat_file)
    main()
        



