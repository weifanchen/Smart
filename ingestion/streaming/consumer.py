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

'''micro batch processing'''
# ./spark-2.2.3-bin-hadoop2.7/bin/pyspark --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.3

def abnormality_detection(event,machines):
    machine_type = machines[event[0]]
    if event[3] > history_stat[machine_type]['97%']*2: # abnormal
        df = event.map(lambda x:[datetime.datetime.strptime(x['timestamp'], '%Y-%m-%d %H:%M:%S'),x['machine_id'],x['household_id'],x['usage']]).toDF(['timestamp','machine_id','household_id','usage']).cache()
        write_to_DB(df,'anomalies')

        
    
def process_stream(rdd):
    if rdd.isEmpty():
        print('RDD is empty')
    else:
        df_machine = read_from_DB('machines') # table name
        df_machine = df_machine.drop('export')
        machine_dict = df_machine.rdd.map(lambda x:(x[0],(x[1],x[2]))).collectAsMap()

        # sum the usage data for 5 sec to one event
        rdd.map(lambda x:abnormality_detection(x,machine_dict))
        current_time=rdd.map(lambda x:datetime.datetime.strptime(x['timestamp'],'%Y-%m-%d %H:%M:%S')).min()
        temp = rdd.map(lambda x:(x[1],x[3])).groupByKey().mapValues(sum) # machine_id,usage 
        temp.map(lambda x:[current_time,x['machine_id'],])

        # before turned into dataframe, add another columns > household_id use machine_dict

        #df = rdd.map(lambda x:[datetime.datetime.strptime(x['timestamp'], '%Y-%m-%d %H:%M:%S'),x['machine_id'],x['household_id'],x['usage']]).toDF(['timestamp','machine_id','household_id','usage']).cache()
        
        #write_to_DB(df,'events')
        #df.show() # print(df.count())



def consume_stream(ssc,topic,brokerAddresses):
    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokerAddresses})
    rdd = kafkaStream.map(lambda x: json.loads(x[1]))
    rdd.foreachRDD(process_stream)
    
    # Starting the task run.
    ssc.start()
    ssc.awaitTermination()

def write_to_DB(df,table_name):
    url = 'jdbc:postgresql://{}/{}'.format(config['postgres_conn']['host'],config['postgres_conn']['database'])
    # url = 'jdbc:postgresql://ec2-52-8-144-26.us-west-1.compute.amazonaws.com/electricity' # new DB
    # url = 'jdbc:postgresql://ec2-54-177-63-46.us-west-1.compute.amazonaws.com/electricity' # old small one
    properties = {'user': config['postgres_conn']['user'], 'password': config['postgres_conn']['password'],"driver": "org.postgresql.Driver"}
    #tablename = 'events'
    writer = DataFrameWriter(df)
    writer.jdbc(url=url, table= table_name, properties=properties,mode = 'append')
    #sample.map(lambda x:(x['machine_id'],x['usage'])).groupByKey().map(lambda x:(x[0],sum(x[1]))).collect()

def read_from_DB(table_name):
    url = 'jdbc:postgresql://{}/{}'.format(config['postgres_conn']['host'],config['postgres_conn']['database'])
    # url = 'jdbc:postgresql://ec2-52-8-144-26.us-west-1.compute.amazonaws.com/electricity' # new DB
    # url = 'jdbc:postgresql://ec2-54-177-63-46.us-west-1.compute.amazonaws.com/electricity' # old small one
    properties = {'user': config['postgres_conn']['user'], 'password': config['postgres_conn']['password'],"driver": "org.postgresql.Driver"}
    df = spark.read.jdbc(url=url, table= table_name, properties=properties)
    return df

def read_profile_from_s3(s3,bucketname,_file):
    obj = s3.Object(bucketname, _file)
    body = obj.get()['Body'].read()
    data = json.loads(body.decode('utf-8'))
    return data

# def read_json_file():
#     with open('/Users/weifanchen/Documents/GitHub/Insight_project/initial_setup/machine_profile_1.json') as cf:
#         machine_profile = json.load(cf)

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


'''
kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokerAddresses})
rdd = kafkaStream.map(lambda x: json.loads(x[1]))
kafkaStream.window(5)
'''





