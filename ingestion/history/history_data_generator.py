import json
import pandas as pd
import datetime
import numpy as np
import random
import boto3 
from time import sleep
import psycopg2


'''generate history data from 2014-12-11 10:00:00 - 2019-05-01 15:11:00'''

with open('./config.json') as cf:
    config = json.load(cf)

ACCESS_ID = config['ACCESS_ID']
ACCESS_KEY = config['ACCESS_KEY']


bucketname = 'electricity-data2'
machine_file= 'machine_profile_1.json'
stat_file = 'stat.json'

s3 = boto3.resource('s3',aws_access_key_id=ACCESS_ID,aws_secret_access_key= ACCESS_KEY)

obj = s3.Object(bucketname, machine_file)
body = obj.get()['Body'].read()
machines = json.loads(body.decode('utf-8'))

obj = s3.Object(bucketname, stat_file)
body = obj.get()['Body'].read()
history_stat = json.loads(body.decode('utf-8'))

# connection = psycopg2.connect(user = config['postgre_user'],
#                                     password = config['postgre_pwd'],
#                                     host = config['postgre_host'],
#                                     port = config['postgre_port'],
#                                     database = config['postgre_db'])

# cursor = connection.cursor()


def initialize_events(start_time,machines,history_stat):
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
        
        events.append(event)
    return events


def following_events(date_str,previous_events):
    margin = 0.1
    for event in previous_events: # = for each machine
        event['timestamp'] = date_str
        event['running'] = event['running'] and random.random()>=0.9
        event['usage'] = event['running'] * event['usage'] * random.uniform(1-margin,1+margin)
        if event['running'] and random.random()>=0.99: # abnormal usage
            event['usage'] = event['usage']*2  

    return previous_events

start_str = '2014-12-11 10:00:00'
end_str = '2014-12-12 10:00:00'

#end_str = '2015-01-01 00:00:00'

#end_str = '2019-05-01 15:11:00'

current_timestamp = datetime.datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
end_timestamp = datetime.datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
time_delta = datetime.timedelta(minutes=1)

first_events = initialize_events(start_str,machines,history_stat)
events = first_events
events_chuck = list()
while current_timestamp< end_timestamp:
    current_timestamp += time_delta 
    date_str = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    temp = following_events(date_str,events)
    events = temp
    events_chuck += events

file_name = 'events_{}.json'.format(current_timestamp.strftime('%Y_%m'))

with open(file_name, 'w') as outfile:
    json.dump(events_chuck, outfile)