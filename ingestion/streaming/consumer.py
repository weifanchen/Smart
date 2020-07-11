from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
import json
import boto3
import datetime


#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

def abnormality_detection(event,machines): 
    machine_type = machines[event["machine_id"]][0]
    if event["usage"] > history_stat[machine_type]['97%'] *3: # abnormal
        print('anomalies ',event['machine_id'])
        return event
    else:
        pass 
    
def process_stream(rdd,machine_dict):
    if rdd.isEmpty():
        print('RDD is empty')
    else:
        # sum the usage data for 5 sec to one event
        df_anormaly = rdd.map(lambda x:abnormality_detection(x,machine_dict)).filter(lambda x: x is not None) \
                         .map(lambda x:[datetime.datetime.strptime(x['timestamp'], '%Y-%m-%d %H:%M:%S'),x['machine_id'],machine_dict[x['machine_id']][1],x['usage']]) \
                         .toDF(['timestamp','machine_id','household_id','usage'])
        write_to_DB(df_anormaly,'anomalies')
        current_time=rdd.map(lambda x:datetime.datetime.strptime(x['timestamp'],'%Y-%m-%d %H:%M:%S')).min()
        temp = rdd.map(lambda x:(x["machine_id"],x["usage"])).groupByKey().mapValues(sum) # machine_id,usage 
        df = temp.map(lambda x:[current_time,x[0],machine_dict[x[0]][1],x[1]]).toDF(['timestamp','machine_id','household_id','usage'])
        write_to_DB(df,'events')
        # df.show()

def consume_stream(ssc,topic,brokerAddresses):
    machine_dict=get_machine_profile() # load machine profile from S3
    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokerAddresses})
    rdd = kafkaStream.map(lambda x: json.loads(x[1]))
    rdd.foreachRDD(lambda x:process_stream(x,machine_dict))
    
    ssc.start()
    ssc.awaitTermination()

def write_to_DB(df,table_name):
    url = 'jdbc:postgresql://{}/{}'.format(config['postgres_conn']['host'],config['postgres_conn']['database'])
    properties = {'user': config['postgres_conn']['user'], 'password': config['postgres_conn']['password'],"driver": "org.postgresql.Driver"}
    #tablename = 'events'
    writer = DataFrameWriter(df)
    writer.jdbc(url=url, table= table_name, properties=properties,mode = 'append')

def read_from_DB(table_name):
    url = 'jdbc:postgresql://{}/{}'.format(config['postgres_conn']['host'],config['postgres_conn']['database'])
    properties = {'user': config['postgres_conn']['user'], 'password': config['postgres_conn']['password'],"driver": "org.postgresql.Driver"}
    df = spark.read.jdbc(url=url, table= table_name, properties=properties)
    return df

def read_profile_from_s3(s3,bucketname,_file):
    obj = s3.Object(bucketname, _file)
    body = obj.get()['Body'].read()
    data = json.loads(body.decode('utf-8'))
    return data

def get_machine_profile():
    df_machine = read_from_DB('machines') # table name
    df_machine = df_machine.drop('export')
    machine_dict = df_machine.rdd.map(lambda x:(x[0],(x[1],x[2]))).collectAsMap()
    return machine_dict

if __name__ == "__main__":
    with open('./config.json') as cf:
        config = json.load(cf)
    ACCESS_ID = config['ACCESS_ID']
    ACCESS_KEY = config['ACCESS_KEY']
    s3 = boto3.resource('s3',aws_access_key_id=ACCESS_ID,aws_secret_access_key= ACCESS_KEY)
    bucketname = 'electricity-data2'
    stat_file = 'stat_wh_sec_0629.json'
    history_stat = read_profile_from_s3(s3,bucketname,stat_file)

    topic_name = 'Usage'
    brokerAddresses = "localhost:9092"
    batchTime = 5
    sc = SparkContext("local[*]", "APP_NAME")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, batchTime) 
    spark = SparkSession(sc)
    consume_stream(ssc,topic_name,brokerAddresses)



