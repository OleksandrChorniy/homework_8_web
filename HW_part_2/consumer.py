import pika
from mongoengine import connect
from bson import ObjectId
from time import sleep
import json
from moduls import Contact


connect('web16', host="mongodb+srv://userweb16:Yfenbkec1@cluster0.ppmihwz.mongodb.net/?retryWrites=true&w=majority")


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='email_queue')


def send_email_dummy(contact_id):
    print(f"An email has been sent to a contact with ID {contact_id}")
    sleep(2)
    return True


def callback(ch, method, properties, body):
    message = json.loads(body)
    contact_id = ObjectId(message['contact_id'])

    contact = Contact.objects.get(id=contact_id)

    if send_email_dummy(contact_id):
        contact.message_sent = True
        contact.save()
        print(f"Consumer: An email has been sent to a contact {contact_id}")
    else:
        print(f"Consumer: An email has been sent to a contact {contact_id}")


channel.basic_consume(queue='email_queue', on_message_callback=callback, auto_ack=True)

print("Consumer: Waiting for messages. To exit, press Ctrl+C")
channel.start_consuming()
