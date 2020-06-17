import json
import pandas as pd
import datetime
import numpy as np
import random
from kafka import KafkaProducer
import boto3 
from time import sleep
import sys 
sys.path.insert(0, '../../') # locate to config folder
import config


'''
How to stimulate new household/ new machine
How to make the data consistent? 
> wash machine runs steadily for 30min. 

****
Unit  kWh/min -> Wh/s
'''

bucketname = 'electricity-data2'
#itemname= 'usage_newschema.csv'
machine_file= 'machine_profile_1.json'
stat_file = 'stat.json'
ACCESS_ID = config.aws_crediential['ACCESS_ID']
ACCESS_KEY = config.aws_crediential['ACCESS_KEY']

s3 = boto3.resource('s3',aws_access_key_id=ACCESS_ID,aws_secret_access_key= ACCESS_KEY)
obj = s3.Object(bucketname, machine_file)
body = obj.get()['Body'].read()
machines = json.loads(body.decode('utf-8'))

obj = s3.Object(bucketname, stat_file)
body = obj.get()['Body'].read()
history_stat = json.loads(body.decode('utf-8'))
# with open('stat.json') as f:
#   history_stat = json.load(f)

# with open('machine_profile.json', 'r') as macfile:
#     machines=json.load(macfile)



def initialize_events(start_time,machines,history_stat,topic_name,producer):
    events = list()
    for machine in machines:
        event = dict()
        minn = history_stat[machine['machine_type']]['3%']
        maxx = history_stat[machine['machine_type']]['97%']
        event['timestamp'] = start_time #time.isoformat() 
        event['machine_id'] = machine['machine_id']
        event['household_id'] = machine['household_id']
        event['running'] = random.choice([True,False])
        event['usage'] = event['running'] * round(random.uniform(minn, maxx),4)
        
        ack = producer.send(topic_name,event) 
        events.append(event)
    return events

def following_events(time_delta,previous_events,topic_name,producer):
    print(previous_events[0]['timestamp'])
    margin = 0.1
    for event in previous_events: # = for each machine
        event['timestamp'] = event['timestamp'] + time_delta #time.isoformat() 
        event['running'] = event['running'] and random.random()>=0.9
        event['usage'] = event['running'] *  event['usage'] * random.uniform(1-margin,1+margin)
        if event['running'] and random.random()>=0.99: # abnormal usage
            event['usage'] = event['usage']*2  

        ack = producer.send(topic_name,event) 
    return previous_events
    

if __name__ == "__main__":
    #random.seed(42)
    bootstrap_servers = ['localhost:9092']
    topic_name = 'Usage'
    producer = KafkaProducer(bootstrap_servers = bootstrap_servers,value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    date_str = '2015-01-01 01:00:00'
    start_time=datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    first_events = initialize_events(start_time,machines,history_stat,topic_name)
    time_delta = datetime.timedelta(seconds=1)
    events = first_events
    while True:
        temp = following_events(time_delta,events,topic_name)
        events = temp
        sleep(1)


