import faust
import json
import ast
from constants import PARTITIONS

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

# # POC:
# new_topic = app.topic('new-topic', partitions=PARTITIONS['USER'])
# new_topic_v1 = app.topic('new-topic-v1', partitions=PARTITIONS['USER'])

# @app.agent(new_topic)
# async def see_new_topic_data(data):
#     async for datum in data:
#         print(datum)
        
# @app.agent(new_topic_v1)
# async def see_new_topic_v1_data(data):
#     async for datum in data:
#         print(datum)
        

def json_serialize(obj):
    return json.dumps(obj)

async def enrich_user(user) -> dict:
    return {'id': user.id, 'email': user.email, 'visits': users_table[user.id]}


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
        
@app.agent(users_topic)
async def enrich_users(users):
    async for user in users:
        user_data = await enrich_user(user)
        serialized_data = json_serialize(user_data)
        fm = enriched_users_topic.as_future_message(key=user.id, value=serialized_data)
        metadata = await enriched_users_topic.publish_message(fm)
        print(f'User: {user.username} - pushed to - enriched-users')
        print(metadata)
