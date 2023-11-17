import json

import pika
from faker import Faker
from mongoengine import *
from HW_part_2.moduls import Contact


connect('web16', host="mongodb+srv://userweb16:Yfenbkec1@cluster0.ppmihwz.mongodb.net/?retryWrites=true&w=majority")


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='email_queue')

fake = Faker()


for _ in range(5):
    contact = Contact(
        full_name=fake.name(),
        email=fake.email()
    )
    contact.save()

    message_body = {'contact_id': str(contact.id)}
    channel.basic_publish(exchange='', routing_key='email_queue', body=json.dumps(message_body))

print("Producer: The fake contacts are saved")
connection.close()
