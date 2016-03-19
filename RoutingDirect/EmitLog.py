import pika
import sys
import MySQLdb
logConnection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channelLog = logConnection.channel()

channelLog.exchange_declare(exchange='logs_direct',
                         type='direct')

message =  "info: Hello World!"
channelLog.basic_publish(exchange='logs',
                      routing_key='',
                      body=message)
print " [x] Sent %r" % (message,)
logConnection.close()
