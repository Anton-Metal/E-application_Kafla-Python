from kafka import KafkaConsumer
import json
import datetime

bootstrap_servers = 'localhost:9092'
topic_name = 'e-application'

def consumer_1():
    # 1 Consumers: Number of orders between midnight and the current time.
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    total_orders = 0
    for message in consumer:
        order_time = datetime.datetime.strptime(message.value['ordertid'], "%m/%d/%Y-%H:%M:%S")
        current_time = datetime.datetime.now()
        if order_time.date() == current_time.date():
            total_orders += 1
    print("Antal ordrar sedan midnatt:", total_orders)

if __name__ == '__main__':
    consumer_1()
