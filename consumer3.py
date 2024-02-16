from kafka import KafkaConsumer
import json
import datetime

bootstrap_servers = 'localhost:9092'
topic_name = 'e-application'

def consumer_3():
    # 3 Consumers: Create daily report
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    daily_report = {'Antal ordrar': 0, 'Summa försäljning': 0, 'Försäljning per produkt': {}}
    for message in consumer:
        order_time = datetime.datetime.strptime(message.value['ordertid'], "%m/%d/%Y-%H:%M:%S")
        current_time = datetime.datetime.now()
        if order_time.date() == current_time.date():
            daily_report['Antal ordrar'] += 1
            daily_report['Summa försäljning'] += sum([product[1] * product[2] for product in message.value['orderdetaljer']])
            for product in message.value['orderdetaljer']:
                product_name = product[0]
                if product_name not in daily_report['Försäljning per produkt']:
                    daily_report['Försäljning per produkt'][product_name] = 0
                daily_report['Försäljning per produkt'][product_name] += product[1] * product[2]
    # Sparar rapporten till en fil med dagens datum 
    with open(f"daily_report_{datetime.datetime.now().date()}.json", "w") as f:
        json.dump(daily_report, f, indent=4)
    print("Daglig rapport skapad och sparad.")

if __name__ == '__main__':
    consumer_3()
