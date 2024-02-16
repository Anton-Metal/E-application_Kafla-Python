from kafka import KafkaConsumer
import json
import datetime

bootstrap_servers = 'localhost:9092'
topic_name = 'e-application'

def consumer_2():
    # 2 Consumers: Daglig och senaste timmens försäljning
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    daily_sales = 0
    hourly_sales = 0
    current_hour = datetime.datetime.now().hour
    for message in consumer:
        order_time = datetime.datetime.strptime(message.value['ordertid'], "%m/%d/%Y-%H:%M:%S")
        current_time = datetime.datetime.now()
        if order_time.date() == current_time.date():
            daily_sales += sum([product[1] * product[2] for product in message.value['orderdetaljer']])
            if order_time.hour == current_hour:
                hourly_sales += sum([product[1] * product[2] for product in message.value['orderdetaljer']])
    print("Dagens totala försäljning:", daily_sales)
    print("Senaste timmens försäljning:", hourly_sales)

if __name__ == '__main__':
    consumer_2()
