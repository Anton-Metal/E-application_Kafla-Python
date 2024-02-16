from kafka import KafkaConsumer
import json
import time

consumer = KafkaConsumer(
    'Orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-processed-consumer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_commit_interval_ms=3000
)

try:
    while True:
        time.sleep(3)
        
        messages = consumer.poll(10)
        
        new_orders = []
        for key,value in messages.items():
            totalt_antal_meddelanden = len(value)
            for msg in value:
                new_orders.append(msg.value)

        with open('orders_done.txt','a') as f:
            f.write(json.dumps(new_orders,indent=4) + '\n')

        print(f'Totalt nya ordrar skrivna till fil är {len(new_orders)}')

except KeyboardInterrupt:
    print('Stänger')
finally:
    consumer.close()