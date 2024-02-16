from kafka import KafkaProducer
import random
import json
import datetime
import time
import sqlite3

conn = sqlite3.connect('users.db')
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS products
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
             product_band TEXT,
             product_album TEXT,
             price INTEGER,
             quantity INTEGER)''')

with open("products.txt", "r", encoding="utf-8") as f:
    for prod in f:
        p = prod.strip().strip("(").strip(")").strip("\n").replace(" ","").split(",")
        c.execute(f"INSERT INTO products (product_band, product_album, price, quantity) VALUES (?, ?, ?, ?)", (p[0], p[1], int(p[2]), int(p[3])))
        conn.commit()

PRODUKTTYP = {"Kilopris":float,
              "Styckpris":int,
              "Literpris":float}
order_id = 100000
KUND_ID = [id for id in range(10000,13000)]

def kvantitet(typ:PRODUKTTYP) ->int|float: 
    heltal = random.randint(1,10)
    if PRODUKTTYP[typ]==float and random.randint(0,1):
        heltal += 0.5

    return heltal 


def random_products(nr:int, num_of_prod: int, list_of_prod) -> list[tuple]:
    list_of_random_products = []
    for i in range(nr):
        # kan sluppa samma produkt flera gånger så problemet är att vi inte tar ut proukten efter den har slumapts 
        # så om man har slumpat produkten tidigare så gör man det igen bara 
        rand_prod_id = random.randint(1,num_of_prod)
        prod = list_of_prod[rand_prod_id]
        kvant = kvantitet(prod[2])
        list_of_random_products.append((prod[0],prod[1],kvant, prod[2]))
    
    return list_of_random_products        


def random_order(order_id:int, num_of_prod: int, list_of_prod) -> dict:
    kund_id = random.choice(KUND_ID)
    products = random_products(random.randint(1,5), num_of_prod, list_of_prod)
    order_tid = datetime.datetime.now().strftime("%m/%d/%Y-%H:%M:%S")

    new_order = dict(orderid=order_id,
                     kundid=kund_id,
                     orderdetaljer=products,
                     ordertid=order_tid) 
    return new_order

if __name__ == "__main__":
    number_of_products_in_database = c.execute("SELECT COUNT(*) FROM products")
    number_of_products_in_database = number_of_products_in_database.fetchone()
    print(number_of_products_in_database)

    products = c.execute("SELECT * FROM products").fetchall()
    print(products)

    test = random_products(3, number_of_products_in_database, products)
    print(test)

    print(json.dumps(random_order(666), number_of_products_in_database, products), indent=4)



    c.close

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    try:
        while True:
            time.sleep(1)
            slumpar_ungefär_25 = int(random.gauss(mu=25, sigma=2))
            for _ in range(slumpar_ungefär_25):
                order_id += 1
                new_order = random_order(order_id, number_of_products_in_database-1, products)
                producer.send("Orders", new_order)

            producer.flush()

    except KeyboardInterrupt:
        print('Shutting down!')
    
    finally:
        producer.flush()
        producer.close()
        
        

