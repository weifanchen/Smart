import json
import psycopg2

'''write the household profile and machine profile to database'''

def read_json(filepath):
    with open(filepath,'r') as jsonfile:
        data = json.load(jsonfile)
    return data

def write_machine_toDB(connection,data):    
    export = ['pv', 'pv_1', 'pv_2', 'pv_3', 'pv_facade', 'pv_roof'] # machine that export electricity
    cursor = connection.cursor()
    for machine_id,values in data.items(): # 
        insert_query = """ INSERT INTO machines (machine_id, machine_type, household_id, export) VALUES (%s,%s,%s,%s)"""
        cursor.execute(insert_query, (machine_id,values['machine_type'],values['household_id'],values['machine_type'] in export))
        connection.commit()

    print('machines are inserted to machines table')

def write_household_to_DB(connection,data):
    cursor = connection.cursor()
    for h in data:
        insert_query = """ INSERT INTO households (household_id, household_type, household_address) VALUES (%s,%s,%s)"""
        cursor.execute(insert_query, (h['household_id'],h['household_type'],h['address']))
        connection.commit()
    print('households are inserted to households table')

if __name__ == "__main__":
    houshold_file = './initial_setup/household_profile_3.json'
    machine_file = './initial_setup/machine_profile_3.json'
    config_file = './config.json'
    machine_profile = read_json(machine_file)
    household_profile = read_json(houshold_file)
    config = read_json(config_file)

    try:
        connection = psycopg2.connect(**config['postgres_conn'])
        if connection:
            print('connected to DB')
        write_machine_toDB(connection,machine_profile)
        write_household_to_DB(connection,household_profile)
        
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
            if(connection):
                connection.close()
                print("PostgreSQL connection is closed")

