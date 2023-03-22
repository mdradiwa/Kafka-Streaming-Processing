from time import sleep
from json import dumps
from kafka import KafkaProducer
import requests

topic_name='product'
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))

url = 'https://dummyjson.com/products'
# request data
response = requests.get(url)
# read and parse the data using json()
res = response.json()
products = res["products"]
data = []

for product in products:
    data.append({'product':product['title'],
           'description':product['description'],
           'price': product['price'],
           'category': product['category']
           })
    print(data)
    producer.send(topic_name, value=data)
    sleep(5)