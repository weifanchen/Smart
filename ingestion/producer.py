import json
import pandas as pd
from datetime import datetime
import numpy as np
import random
from kafka import KafkaProducer


'''
How to stimulate new household/ new machine

How to make the data consistent? 
> wash machine runs steadily for 30min. 
****
Unit  kWh/min -> Wh/min
grid_import
'''

with open('stat.json') as f:
  history_stat = json.load(f)

with open('machine_profile.json', 'r') as macfile:
    machines=json.load(macfile)

# make it a file 
# running time specify? 
usage_range = {
    'pv_facade':[history_stat['pv_facade']['3%'],history_stat['pv_facade']['97%']],
    'pv_roof':[history_stat['pv_roof']['3%'],history_stat['pv_roof']['97%']],
    'pv':[history_stat['pv']['3%'],history_stat['pv']['97%']],
    'machine':[0,0.8],
    'refrigerator':[0,0.03],
    'ventilation':[0,0.03],
    'compressor':[0,0.03],
    'area_room':[0.003,0.461],
    'area_office':[0.003,0.055],
    'cooling_aggregate':[0,0.03],
    'cooling_pumps':[0,0.03],
    'ev':[0,0.03],
    'dishwasher':[history_stat['dishwasher']['3%'],history_stat['dishwasher']['97%']],
    'freezer':[history_stat['freezer']['3%'],history_stat['freezer']['97%']],
    'heat_pump':[history_stat['heat_pump']['3%'],history_stat['heat_pump']['97%']],
    'washing_machine':[history_stat['washing_machine']['3%'],history_stat['washing_machine']['97%']],
    'circulation_pump':[history_stat['circulation_pump']['3%'],history_stat['circulation_pump']['97%']],
    'grid_import':[0,0.03]
}


# event = timestamp, machine_id, usage
def usage_min_generator(machines,history_stat,topic_name,producer):
    time = datetime.now()
    for machine in machines:
        event = dict()
        minn = history_stat[machine['machine_type']]['3%']
        maxx = history_stat[machine['machine_type']]['97%']
        #minn,maxx=usage_range[machine['machine_type']]
        event['timestamp'] = time.isoformat() # format??????? 
        event['machine_id'] = machine['machine_id']
        event['usage'] = round(random.uniform(minn, maxx),4)
        #print(event)
        producer.send(topic_name,event) 
        # send one by one or send a whole chuck of machine at one time?

if __name__ == "__main__":
    #random.seed(42)
    bootstrap_servers = ['localhost:9092']
    topic_name = 'Usage'
    producer = KafkaProducer(bootstrap_servers = bootstrap_servers,value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    usage_min_generator(machines,history_stat,topic_name,producer)


    git clone git@github.com:my-github-repo