from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
import json
import datetime

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

'''micro batch processing'''
./spark-2.2.3-bin-hadoop2.7/bin/pyspark --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.3

def process_stream(rdd):
    if rdd.isEmpty():
        print('RDD is empty')
    else:
        df = rdd.map(lambda x:[datetime.datetime.strptime(x['timestamp'], '%Y-%m-%d %H:%M:%S'),x['machine_id'],x['household_id'],x['usage']]).toDF(['timestamp','machine_id','household_id','usage']).cache()
        write_to_DB(df)
        df.show()

def abnormality_detection():
    pass

def consume_stream(ssc,topic,brokerAddresses):
    # to-do
    # use groupbykey to gather 5 second data 
    # how to sum/usage with groupby can save the timestamp at the same time?
    window_length = 5
    sliding_interval = 5
           
    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokerAddresses})
    rdd = kafkaStream.map(lambda x: json.loads(x[1]))
    rdd.foreachRDD(process_stream)
    
    #parsed.map(lambda x:(x['machine_id'],[x['timestamp'],x['usage'],x['household_id']])).groupByKey() # value list can't groupbykey
    #parsed.map(lambda x:(x['machine_id'],x['usage'])).groupByKey().mapValues(sum)

    # Starting the task run.
    ssc.start()
    ssc.awaitTermination()

def write_to_DB(df):
    '''transform RDD to dataframe that fit the schema in the table'''
    # url = 'jdbc:postgresql://ec2-52-8-144-26.us-west-1.compute.amazonaws.com/electricity' # new DB
    url = 'jdbc:postgresql://ec2-54-177-63-46.us-west-1.compute.amazonaws.com/electricity' # old small one
    properties = {'user': 'postgres', 'password': 'test123',"driver": "org.postgresql.Driver"}
    tablename = 'events'
    writer = DataFrameWriter(df)
    writer.jdbc(url=url, table= tablename, properties=properties,mode = 'append')
    #sample.map(lambda x:(x['machine_id'],x['usage'])).groupByKey().map(lambda x:(x[0],sum(x[1]))).collect()


if __name__ == "__main__":
    with open('./config.json') as cf:
        config = json.load(cf)
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
rdd.
kafkaStream.window(5)
'''

    
