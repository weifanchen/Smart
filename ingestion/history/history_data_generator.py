import json
import pandas as pd
import datetime
import numpy as np
import random
import boto3 
from time import sleep
import psycopg2


'''generate history data from 2019-01-01 00:00:00 - 2020-01-01 00:00:00'''

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
    events = list()
    for pv in previous_events: # = for each machine
        current_event = dict()
        current_event['timestamp'] = date_str
        current_event['machine_id'] = pv['machine_id']
        current_event['household_id'] = pv['household_id']
        current_event['running'] = pv['running'] or (random.random()>0.85)
        current_event['usage'] = current_event['running'] * pv['usage'] * random.uniform(1-margin,1+margin)
        if current_event['running'] and random.random()>=0.99: # abnormal usage
            current_event['usage'] = pv['usage']*2  
        events.append(current_event)
    return events    

def write_file_to_s3(s3,bucketname,file_name,data):
    obj = s3.Object(bucketname, file_name)
    temp=obj.put(Body=json.dumps(data))
    

def write_file_to_BD(file_name):
    pass

def write_file(file_name,events_chuck):
    #with open(file_name, 'w') as outfile:
    #    json.dump(events_chuck, outfile)
    df = pd.DataFrame.from_records(events_chuck)
    df = df.drop(['running'],axis=1)
    df.to_json('./history_data/'+file_name,orient='records')



start_str = '2018-09-30 00:00:00'
end_str = '2018-12-31 00:00:00'

current_timestamp = datetime.datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
current_date=current_timestamp.strftime('%Y-%m-%d')
end_timestamp = datetime.datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
time_delta = datetime.timedelta(minutes=1)

first_events = initialize_events(start_str,machines,history_stat)
events = first_events
events_chuck = list()
#events_chuck += first_events
while current_timestamp < end_timestamp:
    #print(current_timestamp)
    events_chuck += events
    current_timestamp += time_delta 
    date_str = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    temp = following_events(date_str,events)
    #events_chuck += temp
    events = temp
    if date_str.split(' ')[1]=='00:00:00': # the end of the day
        #write_file_to_s3(s3,bucketname,date_str.split(' ')[0],events_chuck)
        file_name = 'events_{}.json'.format(current_date)
        write_file(file_name,events_chuck)
        current_date=current_timestamp.strftime('%Y-%m-%d')
        print(current_date)
        events_chuck = list()
        events_chuck += events

'''
file_name = 'events_{}.json'.format(current_timestamp.strftime('%Y_%m'))

with open(file_name, 'w') as outfile:
    json.dump(events_chuck, outfile)

df = pd.DataFrame.from_records(events_chuck)
df = pd.read_json('events_2018-01-01.json')
'''
