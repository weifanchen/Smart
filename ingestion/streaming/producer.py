import json
import pandas as pd
import datetime
import numpy as np
import random
from kafka import KafkaProducer
import boto3 
from time import sleep

''' 
producer_v1
stimulate electricity usage event 
clear schema
event structure = ['timestamp', 'machine_id', 'household_id', running, 'usage']
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

def initialize_events(start_time,machines,history_stat,topic_name,producer):
    events = list()
    for machine in machines:
        event = dict()
        minn = history_stat[machine['machine_type']]['3%']
        maxx = history_stat[machine['machine_type']]['97%']
        event['timestamp'] = start_time #string format
        event['machine_id'] = machine['machine_id']
        event['household_id'] = machine['household_id']
        event['running'] = random.choice([True,False])
        event['usage'] = event['running'] * round(random.uniform(minn, maxx),4)
        
        ack = producer.send(topic_name,event) 
        events.append(event)
    return events

def following_events(date_str,previous_events,topic_name,producer):
    margin = 0.1
    for event in previous_events: # = for each machine
        event['timestamp'] = date_str
        event['running'] = event['running'] and random.random()>=0.9
        event['usage'] = event['running'] * event['usage'] * random.uniform(1-margin,1+margin)
        if event['running'] and random.random()>=0.99: # abnormal usage
            event['usage'] = event['usage']*2  

        ack = producer.send(topic_name,event) 
    return previous_events
    

def main(date_str,sleep_time):
    bootstrap_servers = ['localhost:9092']
    topic_name = 'Usage'
    kafka_producer=connect_kafka_producer(bootstrap_servers)
    current_timestamp=datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    first_events = initialize_events(date_str,machines,history_stat,topic_name,kafka_producer)
    time_delta = datetime.timedelta(seconds=1)
    events = first_events
    while current_timestamp < datetime.datetime.now():
        current_timestamp += time_delta 
        date_str = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        temp = following_events(date_str,events,topic_name,kafka_producer)
        events = temp
        sleep(sleep_time)


if __name__ == "__main__":
    #random.seed(42)
    date_str = '2016-01-01 01:00:00'
    sleep_time = 1
    with open('./config.json') as cf:
        config = json.load(cf)

    date_str = config['producer_start_date']
    sleep_time = config['producer_sleep_time']
    ACCESS_ID = config['ACCESS_ID']
    ACCESS_KEY = config['ACCESS_KEY']
    s3 = boto3.resource('s3',aws_access_key_id=ACCESS_ID,aws_secret_access_key= ACCESS_KEY)
    bucketname = 'electricity-data2'
    machine_file= 'machine_profile_1.json'
    stat_file = 'stat.json'
    machines = read_profile_from_s3(s3,bucketname,machine_file)
    history_stat = read_profile_from_s3(s3,bucketname,stat_file)
    main(date_str,sleep_time)
        

