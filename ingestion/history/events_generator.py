import json
import pandas as pd
import datetime
import random
import boto3 
import time
from time import sleep
import psycopg2


'''
generate history data from 2019-01-01 00:00:00 - 2020-01-01 00:00:00 in min interval
'''

def initialize_events(start_time,machines,history_stat):
    events = list()
    #for machine in machines:
    for machine_id, machine in machines.items():
        event = dict()
        minn = history_stat[machine['machine_type']]['3%']
        maxx = history_stat[machine['machine_type']]['97%']
        event['timestamp'] = start_time #string format
        event['machine_id'] = machine_id
        event['household_id'] = machine['household_id']
        event['running'] = random.choice([True,False])
        event['usage'] = event['running'] * round(random.uniform(minn, maxx),4)
        event['abnormal'] = False
        events.append(event)
    return events

def generate_abnormal_event(machine_id,previous_usage,previous_abnormal,machines,history_stat):
    machine_type = machines[machine_id]['machine_type']
    param = 1
    if previous_abnormal: param = 1.5
    elif previous_usage < history_stat[machine_type]['75%']: param =2 
    else: param =1
    current_usage = previous_usage * param * history_stat[machine_type]['std']
    return current_usage 

def following_events(date_str,previous_events):
    margin = 0.15
    events = list()
    for pv in previous_events: # = for each machine
        current_event = dict()
        current_event['timestamp'] = date_str
        current_event['machine_id'] = pv['machine_id']
        current_event['household_id'] = pv['household_id']
        current_event['abnormal'] = False
        current_event['running'] = pv['running'] or (random.random()>0.85)
        if current_event['running'] and random.random()>=0.995: # abnormal event 
            current_event['usage'] = generate_abnormal_event(pv['machine_id'],pv['usage'],pv['abnormal'],machines,history_stat) # 回傳值
            current_event['abnormal'] = True
        else:
            current_event['usage'] = current_event['running'] * pv['usage'] * random.uniform(1-margin,1+margin) 
        events.append(current_event)
    return events    

def read_profile_from_s3(s3,bucketname,_file):
    obj = s3.Object(bucketname, _file)
    body = obj.get()['Body'].read()
    data = json.loads(body.decode('utf-8'))
    return data

def write_file_to_s3(s3,bucketname,file_name,data):
    obj = s3.Object(bucketname, file_name)
    temp=obj.put(Body=json.dumps(data))
    

def write_file_to_BD(connection,events):
    cursor = connection.cursor()
    data = [(e['timestamp'],e['machine_id'],e['household_id'],round(e['usage'],3),e['abnormal']) for e in events]
    records_list_template = ','.join(['%s'] * len(data))
    insert_query = 'INSERT into history_events(timestamp, machine_id, household_id, usage, abnormal) VALUES {}'.format(records_list_template)
    query = cursor.mogrify(insert_query, data)
    cursor.execute(query)
    connection.commit()

insert_query = 'INSERT into history_events(timestamp, machine_id, household_id, usage) VALUES (%s,%s,%s,%s)'
start = time.time()
for e in day_events:
    cursor.execute(insert_query, (e['timestamp'],e['machine_id'],e['household_id'],e['usage']))
    connection.commit()
end = time.time()
print(end-start)


def write_file(file_name,events_day):
    #with open(file_name, 'w') as outfile:
    #    json.dump(events_day, outfile)
    df = pd.DataFrame.from_records(events_day)
    df = df.drop(['running'],axis=1)
    df.to_json('./history_data/'+file_name,orient='records')

def connect_to_DB(config):
    connection = psycopg2.connect(**config['postgres_conn'])
    return connection
    
def main():
    current_timestamp = datetime.datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    current_date=current_timestamp.strftime('%Y-%m-%d')
    end_timestamp = datetime.datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
    time_delta = datetime.timedelta(minutes=1)

    first_events = initialize_events(start_str,machines,history_stat)
    events = first_events
    events_day = list() # all the events in that day

    while current_timestamp < end_timestamp:
        #print(current_timestamp)
        events_day += events
        current_timestamp += time_delta 
        date_str = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        temp = following_events(date_str,events)
        events = temp
        #write_file_to_BD(connection,events)
        if date_str.split(' ')[1]=='00:00:00': # the end of the day
            #write_file_to_s3(s3,bucketname,date_str.split(' ')[0],events_day)
            file_name = 'events_{}.json'.format(current_date)
            write_file(file_name,events_day)
            current_date=current_timestamp.strftime('%Y-%m-%d')
            print(current_date)
            events_day = list()
            events_day += events

# def change_machines_structure(machines):
#     new_struct = dict()
#     for machine in machines:
#         new_struct[machine['machine_id']] = {'machine_type':machine['machine_type'],'household_id':machine['household_id']}
#     return new_struct

if __name__ == "__main__":
    with open('./config.json') as cf:
        config = json.load(cf)

    bucketname = 'electricity-data2'
    machine_file= 'machine_profile_1.json'
    stat_file = 'stat_wh_min_0629.json'

    s3 = boto3.resource('s3',aws_access_key_id=config['ACCESS_ID'],aws_secret_access_key= config['ACCESS_KEY'])
    machines = read_profile_from_s3(s3,bucketname,machine_file)
    history_stat = read_profile_from_s3(s3,bucketname,stat_file)

    start_str = '2019-02-01 00:00:00'
    end_str = '2019-03-01 00:00:00'
    connection = connect_to_DB(config)
    main()

    # with open('./initial_setup/machine_profile_1.json','w') as outfile:
    #     json.dump(new_struct_machine,outfile)

