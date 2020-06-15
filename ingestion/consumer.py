import sys
from kafka import KafkaConsumer

def consume(topic):
    bootstrap_servers = ['localhost:9092']
    topic_name = topic
    consumer = KafkaConsumer(
                    topic_name,
                    group_id = 'group1',
                    bootstrap_servers = bootstrap_servers,
                    auto_offset_reset='earliest')
    try:
        for message in consumer:
            print('%s:%d:%d: key=%s value=%s' %(message.topic, message.partition, message.offset, message.key, message.value))
    except KeyboardInterrupt:
        sys.exit()

if __name__ == "__main__":
    consume('Usage')