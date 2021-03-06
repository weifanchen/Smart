import random
import json
import hashlib
import pandas as pd
from datetime import datetime
import numpy as np
from faker import Faker
from random import randint, choices, sample, choice
import uuid



'''Create household profiles, machine profiles'''

# data = pd.read_csv('./data/usage_newschema.csv')
fake = Faker('de_DE')

industrial_num = 20
residential_num = 30
public_num = 6

housetype_list = ['public','residential','industrial']

machine_list = {
    'pv_generators' : ['pv_facade','pv_roof','pv','pv'],
    'industrial_comsumption_machines' : ['machine','machine','machine','machine','refrigerator','ventilation','compressor','area_room','area_room','area_room','area_office','area_office','area_office','cooling_aggregate','cooling_pumps','ev','ev','ev'], #0~10
    'residential_comsumption_machines' : ['refrigerator','dishwasher','freezer','heat_pump','washing_machine','circulation_pump','ev','ev'], #0~2
    'public_comsumption_machines' : ['grid_import'], #1
    'TF' :[True,False]
}


def generate_household(housetype,num,machine_list):
    households = list()
    machine_profiles = list()
    for _ in range(1,num+1):
        household = dict()
        fake_address=fake.address()
        household_id = hashlib.sha1(str.encode(fake_address)).hexdigest()
        household['address'] = fake_address
        household['household_id'] = household_id
        household['household_type'] = housetype
        machines,machine_prof = generate_meter(housetype,machine_list,household_id)
        household['machines_info'] = machines
        households.append(household)
        machine_profiles += machine_prof
    return households, machine_profiles


def generate_meter(housetype,machine_list,household_id):
    if housetype=='industrial': 
        machines = choices(machine_list['industrial_comsumption_machines'],k=randint(6,20))
        generator = choices(machine_list['pv_generators'],k=randint(1,len(machines)//4))
        if generator: machines += generator
        machines.append('grid_import')
    elif housetype=='residential':
        machines = sample(machine_list['residential_comsumption_machines'],k=randint(1,8))
        generator = choices(machine_list['pv_generators'],k=randint(0,len(machines)//3))
        if generator: machines += generator
        machines.append('grid_import')
    elif housetype=='public':
        machines = ['grid_import'] * randint(1,4)
    
    result = list()
    machine_prof = list() 
    for m in machines:
        r = dict()
        r['machine_type'] = m
        r['machine_id'] = uuid.uuid4().hex  # totally random
        result.append(r)
        r['household_id'] = household_id
        machine_prof.append(r)

    return result, machine_prof
 
def change_machines_structure(machines):
    new_struct = dict()
    for machine in machines:
        new_struct[machine['machine_id']] = {'machine_type':machine['machine_type'],'household_id':machine['household_id']}
    return new_struct

household_profile = list()
machine_profile = list()
households_ind, machines_ind = generate_household('industrial',industrial_num,machine_list)
households_res, machines_res = generate_household('residential',residential_num,machine_list)
households_pub, machines_pub = generate_household('public',public_num,machine_list)

household_profile = households_ind + households_res + households_pub
machine_profile = machines_ind + machines_res + machines_pub
machine_profile=change_machines_structure(machine_profile)

with open('./initial_setup/household_profile_3.json', 'w') as houfile:
    json.dump(household_profile, houfile)

with open('./initial_setup/machine_profile_3.json', 'w') as macfile:
    json.dump(machine_profile, macfile)
    
