import sys
from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import psycopg2
import json


def consume(topic,cursor):
    bootstrap_servers = ['localhost:9092']
    topic_name = topic
    consumer = KafkaConsumer(
                    topic_name,
                    group_id = 'group1',
                    bootstrap_servers = bootstrap_servers,
                    auto_offset_reset='earliest',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    try:
        for message in consumer:
            print('%s:%d:%d: key=%s value=%s' %(message.topic, message.partition, message.offset, message.key, message.value))
            #cur.executemany(sql,vendor_list)
            
            insert_query = """ INSERT INTO testing (timestamp, machine_id, household_id, usage) VALUES (%s,%s,%s,%s)"""
            cursor.execute(insert_query, (message.value['timestamp'],message.value['machine_id'],message.value['household_id'],message.value['usage']))
            connection.commit()

    except KeyboardInterrupt:
        sys.exit()

def connect_to_DB(config):
    try:
        connection = psycopg2.connect(user = config['postgre_user'],
                                    password = config['postgre_pwd'],
                                    host = config['postgre_host'],
                                    port =config['postgre_port'],
                                    database = config['postgre_db'])
        
        #cursor = connection.cursor()
        #consume('Usage',cursor)
        return connection

    # Print PostgreSQL Connection properties


    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")



if __name__ == "__main__":
    with open('./config.json') as cf:
        config = json.load(cf)
    connection = connect_to_DB(config)
    cursor = connection.cursor()
    consume('Usage',cursor)
    