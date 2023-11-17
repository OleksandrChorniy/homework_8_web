from mongoengine import *

class Contact(Document):
    full_name = StringField(required=True)
    email = StringField(required=True)
    is_message_sent = BooleanField(default=False)
