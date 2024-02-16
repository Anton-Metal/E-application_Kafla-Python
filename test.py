from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

while True: 
    msg = input("write something: ")
    if msg == 'q':
        break
    producer.send('e-application', bytes(msg, 'utf-8'))
                         

