from google.cloud import pubsub
import os
import base64
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/sivaa/grab/subtle-seer-113110-183172fcd165.json"
import warnings
warnings.filterwarnings('ignore')
import sys
from faker import Faker
fake = Faker()
import random
from datetime import datetime
from time import sleep
import json

task = sys.argv[1]
send = sys.argv[2]

# Instantiates a client
publisher = pubsub.PublisherClient()
topic = 'projects/subtle-seer-113110/topics/input_stream_beam'
subscription_name = 'projects/subtle-seer-113110/subscriptions/ouput_stream_beam_subscriber'

def pubrun(send):

    indo_cities=['Denpasar','Bandung','Jakarta','Serang','Tegal']
    # orders = []
    for i in range(0,10000):
        # sleep(30)
        order = {'order_number':'AP-{}'.format(str(i)),
                      'service_type':'GO_INTERVIEW',
                      'driver_id':fake.random_int(),
                      'customer_id':fake.random_int(),
                      'service_area_name':random.choice(indo_cities),
                      'payment_type':"GO_HASH",
                      'status':'COMPLETED',
                      'event_timestamp':datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
        print json.dumps(order)
        # orders.append(json.dumps(order))
        publisher.publish(topic,b'{}'.format(json.dumps(order)))

def subrun(send):
    subscriber = pubsub.SubscriberClient()
    print 'inside subscriber'
    def callback(message):
        print message.data
        message.ack()
    print subscriber.subscribe(subscription_name, callback)



if __name__ == '__main__':
    print 'starting {}'.format(task)
    if task=='publish':
        pubrun(send)

    if task=='subscribe':
        subrun(send)