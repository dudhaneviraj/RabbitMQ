import pika
import time
import tldextract
import MySQLdb
import traceback
import os
worker="WORKER3"

def insert(data):
    if data.strip():
        con = MySQLdb.connect(host="localhost", # your host, usually localhost
                             user="root", # your username
                              passwd="1234", # your password
                              db="rabbitmq") # name of the data base

        cur = con.cursor()
        query="insert into rabbitmq (url,domain,ttl,class,type,ip,worker)values(%s,%s,%s,%s,%s,%s,%s)"
        tld=""
        try:
            tld=tldextract.extract(data).registered_domain
        except:
            traceback.format_exc()
        try:
            digs= os.popen("dig +tries=1 +timeout=1 +noall +answer "+tldextract.extract(tld).registered_domain).read()
            digs=str(digs).split('\n')
            for dig in digs:
                if(dig.strip()):
                    try:
                        dig=dig.replace("\t\t","\t")
                        dig=dig.replace("\t\t","\t")
                        temp=dig.split('\t')
                        print "Data: "+temp[0] +"\t Data: "+ temp[1]+"\t Data: "+ temp[2]+"\t Data: "+ temp[3]+"\t Data: "+ temp[4]
                        params=(data.strip(),tld.strip(),temp[1].strip(),temp[2].strip(),temp[3].strip(),temp[4].strip(),worker)
                        cur.execute(query,params)
                    except:
                        params=(data.strip(),tld.strip(),"","","","",worker)
                        cur.execute(query,params)
        except:
            params=(data.strip(),tld.strip(),"","","","",worker)
            cur.execute(query,params)
        con.commit()
        cur.close()
        con.close()





connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='data')


def callback(ch, method, properties, body):
    insert(body)

channel.basic_consume(callback,
                      queue='data',
                      no_ack=True)

channel.start_consuming()