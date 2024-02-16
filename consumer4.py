from kafka import KafkaConsumer
import json
import sqlite3

bootstrap_servers = 'localhost:9092'
topic_name = 'e-application'

conn = sqlite3.connect('products.db')
c = conn.cursor()

def update_product_quantity(product_band, product_album, quantity):
    # Update the inventory balance(quantity) for a product in the database.
    c.execute("UPDATE products SET quantity = quantity - ? WHERE product_band = ? AND product_album = ?", (quantity, product_band, product_album))
    conn.commit()

def consumer_4():
    # 4 Consumers: Update the inventory balance for each sale
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer:
        for product in message.value['orderdetaljer']:
            # Update the inventory balance for each sold product.
            update_product_quantity(product['product_band'], product['product_album'], product['quantity'])

if __name__ == '__main__':
    consumer_4()



# from kafka import KafkaConsumer
# import json
# import sqlite3

# # Kafka inställningar
# bootstrap_servers = 'localhost:9092'
# topic_name = 'e-application'

# # Skapa och anslut till SQLite-databasen
# conn = sqlite3.connect('users.db')
# c = conn.cursor()

# # Skapa produkttabellen om den inte redan existerar
# c.execute('''CREATE TABLE IF NOT EXISTS products
#              (id INTEGER PRIMARY KEY AUTOINCREMENT,
#              product_band TEXT,
#              product_album TEXT,
#              price INTEGER,
#              quantity INTEGER)''')

# # Läsa in produkter från filen och lägg till dem i databasen om de inte redan finns där
# with open("products.txt", "r", encoding="utf-8") as f:
#     for prod in f:
#         p = prod.strip().strip("(").strip(")").strip("\n").replace(" ", "").split(",")
#         c.execute("INSERT OR IGNORE INTO products (product_band, product_album, price, quantity) VALUES (?, ?, ?, ?)", (p[0], p[1], int(p[2]), int(p[3])))
#         conn.commit()

# def update_product_quantity(product_band, product_album, quantity):
#     # Uppdatera lagersaldot för en produkt i databasen
#     c.execute("UPDATE products SET quantity = quantity - ? WHERE product_band = ? AND product_album = ?", (quantity, product_band, product_album))
#     conn.commit()

# def consumer_4():
#     # 4:e konsumenten: Uppdatera lagersaldot för varje försäljning
#     consumer = KafkaConsumer(
#         topic_name,
#         bootstrap_servers=bootstrap_servers,
#         auto_offset_reset='earliest',
#         value_deserializer=lambda x: json.loads(x.decode('utf-8'))
#     )
#     for message in consumer:
#         for product in message.value['orderdetaljer']:
#             # Uppdatera lagersaldot för varje såld produkt
#             update_product_quantity(product['product_band'], product['product_album'], product['quantity'])

# if __name__ == '__main__':
#     consumer_4()
