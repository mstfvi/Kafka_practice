from kafka import KafkaProducer
from kafka import KafkaConsumer

def get_kafka_producer(servers=['localhost:9092']):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=servers) #, api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def publish(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print(f"Publish Succesful ({key}, {value}) -> {topic_name}")
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))
def get_kafka_consumer(topic_name, servers=['localhost:9092']):
    _consumer = None
    try:
        _consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest', bootstrap_servers=servers, api_version=(0, 10), consumer_timeout_ms=10000)
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _consumer

def subscribe(consumer_instance):
    try:
        for event in consumer_instance:
            key = event.key.decode("utf-8")
            value = event.value.decode("utf-8")
            print(f"Message Received: ({key}, {value})")
        consumer_instance.close()
    except Exception as ex:
        print('Exception in subscribing')
        print(str(ex))

if __name__ == '__main__':
    topic = input('enter topic: ')
    key = input('enter key: ')
    message = input('enter message: ')
    # set a producer
    producer = get_kafka_producer()
    consumer = get_kafka_consumer(topic)
    # receive messages from certain topic
    for i in range(5):
        # send a message by key and topics to kafka broker
        publish(producer, topic, key + str(i), message + str(i))
        # set a consumer
        subscribe(consumer)

