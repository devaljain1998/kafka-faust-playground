import faust
import json
import ast

app = faust.App(
    'faust_poc',
    broker='kafka://localhost:9092',
    value_serializer='raw',
)

class User(faust.Record):
    id: str
    username: str
    email: str

users_topic = app.topic('quickstart-events-v5', key_type=str, value_type=User, value_serializer='json')
users_table = app.Table('users', default=int)

enriched_users_topic = app.topic('quickstart-enriched')

# def enrich_user(user):

@app.agent(users_topic)
async def say_hi(users):
    async for user in users:
        print(f'Hi {user.username}!')
        
@app.agent(users_topic)
async def save(users):
    async for user in users:
        print(user, type(user))
        users_table[user.id] += 1
        print(f"{user.id} : {users_table[user.id]}")