import json
import pandas as pd
import datetime
import numpy as np
import random
import boto3 
import psycopg2

houshold_file = './household_profile_1.json'
machine_file = './machine_profile_1.json'

def read_json(filepath):
    with open(filepath,'r') as jsonfile:
        data = json.load(jsonfile)
    return data

def write_in_machine(connection,data):
    export = ['pv', 'pv_1', 'pv_2', 'pv_3', 'pv_facade', 'pv_roof']
    cursor = connection.cursor()
    for m in data:
        insert_query = """ INSERT INTO machines (machine_id, machine_type, household_id, export) VALUES (%s,%s,%s,%s)"""
        cursor.execute(insert_query, (m['machine_id'],m['machine_type'],m['household_id'],m['machine_type'] in export))
        connection.commit()
    print('machine profile inserted to machines table')

def write_in_household(connection,data):
    cursor = connection.cursor()
    for h in data:
        insert_query = """ INSERT INTO households (household_id, household_type) VALUES (%s,%s)"""
        cursor.execute(insert_query, (h['household_id'],h['household_type']))
        connection.commit()
    print('household profile inserted to household table')

with open('./config.json') as cf:
    config = json.load(cf)


machine_profile = read_json(machine_file)
household_profile = read_json(houshold_file)

try:
    connection = psycopg2.connect(user = "postgres",
                                password = "test123",
                                host = "ec2-54-177-63-46.us-west-1.compute.amazonaws.com",
                                port = "5432",
                                database = "electricity")
    write_in_machine(connection,machine_profile)
    write_in_household(connection,household_profile)
    
except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
        if(connection):
            connection.close()
            print("PostgreSQL connection is closed")

