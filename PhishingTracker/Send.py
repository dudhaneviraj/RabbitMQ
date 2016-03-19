import pika
import requests


connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='data')
data=requests.get("https://www.openphish.com/feed.txt")
list=data.text.split('\n')
for temp in list:
    channel.basic_publish(exchange='',
                      routing_key='data',
                      body=temp)
connection.close()