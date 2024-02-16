from kafka import KafkaProducer
import random
import json
import datetime
import time
import sqlite3

bootstrap_servers = 'localhost:9092'
topic_name = 'e-application'

conn = sqlite3.connect('products.db')
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS products
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
             product_band TEXT,
             product_album TEXT,
             price INTEGER,
             quantity INTEGER)''')

def get_random_products(num):
    c.execute("SELECT * FROM products ORDER BY RANDOM() LIMIT ?", (num,))
    return c.fetchall()

def create_random_order(order_id):
    kund_id = random.randint(10000, 12999)
    products = get_random_products(random.randint(1, 5))
    order_time = datetime.datetime.now().strftime("%m/%d/%Y-%H:%M:%S")
    order = {'orderid': order_id, 'kundid': kund_id, 'orderdetaljer': products, 'ordertid': order_time}
    return order

def send_order_to_kafka(producer, order):
    producer.send(topic_name, value=order)
    producer.flush()

if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        while True:
            time.sleep(1)
            num_orders = int(random.gauss(mu=25, sigma=2))
            for _ in range(num_orders):
                order_id = random.randint(100000, 999999)
                new_order = create_random_order(order_id)
                send_order_to_kafka(producer, new_order)

    except KeyboardInterrupt:
        print('Shutting down!')

    finally:
        producer.close()











# from kafka import KafkaProducer
# import random
# import json
# import datetime
# import time

# # Dictionary för att hålla typen av varje produkt
# PRODUKTTYP = {
#     "product_band": str,
#     "product_album": str,
#     "price": int,
#     "quantity": int
# }

# dict_med_produkter = {}
# num_of_prod = 0
# order_id = 100000
# KUND_ID = [id for id in range(10000, 13000)]

# # öppnar produkterna från txt filen
# with open("products.txt", "r", encoding='utf-8') as f:
#     for prod in f.readlines():
#         num_of_prod += 1
#         p = prod.strip().strip("(").strip(")").strip("\n").replace(" ", "")
#         new_prod = tuple(p.split(","))
#         dict_med_produkter[num_of_prod] = new_prod


# def kvantitet(typ: type) -> int | float:
#     heltal = random.randint(1, 10)
#     if typ == float and random.randint(0, 1):
#         heltal += 0.5

#     return heltal


# def random_products(nr: int) -> list[tuple]:
#     list_of_random_products = []
#     for i in range(nr):
#         rand_prod_id = random.randint(1, num_of_prod)
#         prod = dict_med_produkter[rand_prod_id]
#         kvant = kvantitet(PRODUKTTYP["quantity"])
#         list_of_random_products.append((prod[0], prod[1], prod[2], kvant))

#     return list_of_random_products


# def random_order(order_id: int) -> dict:
#     kund_id = random.choice(KUND_ID)
#     products = random_products(random.randint(1, 5))
#     order_tid = datetime.datetime.now().strftime("%m/%d/%Y-%H:%M:%S")

#     new_order = dict(orderid=order_id,
#                      kundid=kund_id,
#                      orderdetaljer=products,
#                      ordertid=order_tid)
#     return new_order


# if __name__ == '__main__':

#     producer = KafkaProducer(
#         bootstrap_servers='localhost:9092',
#         value_serializer=lambda v: json.dumps(v).encode('utf-8')
#     )
#     try:
#         while True:
#             time.sleep(1)
#             slumpar_ungefär_25 = int(random.gauss(mu=25, sigma=2))
#             for _ in range(slumpar_ungefär_25):
#                 order_id += 1
#                 new_order = random_order(order_id)
#                 producer.send("Orders", new_order)

#             producer.flush()

#     except KeyboardInterrupt:
#         print('Shutting down!')

#     finally:
#         producer.flush()
#         producer.close()
