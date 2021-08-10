"""
Module for conducting POC of Python Faust.
"""

from confluent_kafka import Producer
import socket
import json
import yaml

conf = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}

producer = Producer(conf)
topic = 'quickstart-events-v5'

with open('users.json', 'r') as json_file:
    users = json.load(json_file)
    for i, user in enumerate(users):
        serialized_user = json.dumps(user)
        # print(type(serialized_user))
        producer.produce(topic, key=str(user['id']), value=serialized_user)
        
    producer.flush()