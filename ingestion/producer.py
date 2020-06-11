import random
import json
import pandas as pd
from datetime import datetime
import numpy as np
from random import randint, choices, sample, choice
import boto3
from kafka import KafkaProducer


'''
How to stimulate new household/ new machine

How to make the data consistent? 
> wash machine runs steadily for 30min. 
****
grid_export
grid_import
'''

# make it a file 
# running time specify? 
usage_range = {
    'pv_facade':[0,0.03],
    'pv_roof':[0,0.03],
    'pv':[0,0.03],
    'machine':[0,0.03],
    'refrigerator':[0,0.03],
    'ventilation':[0,0.03],
    'compressor':[0,0.03],
    'area_room':[0,0.03],
    'area_office':[0,0.03],
    'cooling_aggregate':[0,0.03],
    'cooling_pumps':[0,0.03],
    'ev':[0,0.03],
    'dishwasher':[0,0.03],
    'freezer':[0,0.03],
    'heat_pump':[0,0.03],
    'washing_machine':[0,0.03],
    'circulation_pump':[0,0.03],
    'grid_import':[0,0.03]
}

with open('machine_profile.json', 'r') as macfile:
    machines=json.load(macfile)

bootstrap_servers = ['localhost:9092']
topic_name = 'Usage'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

def usage_min_generator(usage_range,machines,topic_name,producer):
    time = datetime.now()
    for machine in machines:
        event = dict()
        minn,maxx=usage_range[machine['machine_type']]
        usage = random.gauss(minn, maxx)
        event['timestamp'] = time.isoformat() # format??????? 
        event['machine_id'] = machine['machine_id']
        event['usage'] = usage
        producer.send(topic_name,event) 
        # send one by one or send a whole chuck of machine at one time?




