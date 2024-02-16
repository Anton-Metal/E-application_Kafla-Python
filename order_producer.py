from kafka import KafkaProducer
import random
import json
import datetime
import time

PRODUKTTYP = {"Kilopris":float,
              "Styckpris":int,
              "Literpris":float}

dict_med_produkter = {}
num_of_prod = 0
order_id = 100000
KUND_ID = [id for id in range(10000,13000)]


# öppnar produketna från txt filen 
with open("products.txt", "r", encoding='utf-8') as f:
    for prod in f.readlines():
        num_of_prod += 1
        p = prod.strip().strip("(").strip(")").strip("\n").replace(" ","")
        new_prod = tuple(p.split(","))
        dict_med_produkter[num_of_prod] = new_prod

def kvantitet(typ:PRODUKTTYP) ->int|float: 
    heltal = random.randint(1,10)
    if PRODUKTTYP[typ]==float and random.randint(0,1):
        heltal += 0.5

    return heltal 

def random_products(nr:int) -> list[tuple]:
    list_of_random_products = []
    for i in range(nr):
        # kan sluppa samma produkt flera gånger så problemet är att vi inte tar ut proukten efter den har slumapts 
        # så om man har slumpat produkten tidigare så gör man det igen bara 
        rand_prod_id = random.randint(1,num_of_prod)
        prod = dict_med_produkter[rand_prod_id]
        kvant = kvantitet(prod[2])
        list_of_random_products.append((prod[0],prod[1],kvant, prod[2]))
    
    return list_of_random_products

def random_order(order_id:int) -> dict:
    kund_id = random.choice(KUND_ID)
    products = random_products(random.randint(1,5))
    order_tid = datetime.datetime.now().strftime("%m/%d/%Y-%H:%M:%S")

    new_order = dict(orderid=order_id,
                     kundid=kund_id,
                     orderdetaljer=products,
                     ordertid=order_tid) 
    return new_order


if __name__ == '__main__':

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
                new_order = random_order(order_id)
                producer.send("Orders", new_order)

            producer.flush()

    except KeyboardInterrupt:
        print('Shutting down!')
    
    finally:
        producer.flush()
        producer.close()
        
        