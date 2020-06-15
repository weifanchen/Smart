import sys
from kafka import KafkaConsumer
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





if __name__ == "__main__":
    try:
        connection = psycopg2.connect(user = "postgres",
                                    password = "test123",
                                    host = "ec2-54-177-63-46.us-west-1.compute.amazonaws.com",
                                    port = "5432",
                                    database = "electricity")
        
        cursor = connection.cursor()
        consume('Usage',cursor)

    # Print PostgreSQL Connection properties


    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
    