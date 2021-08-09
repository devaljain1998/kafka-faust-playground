# Imports:
import os
from flask import jsonify, make_response, abort
import requests
from datetime import datetime, timedelta
import urllib.parse
import json
import time
import base64
import re
from haversine import haversine
from google.cloud import bigquery, secretmanager
from elasticsearch import Elasticsearch, exceptions as EsExceptions

secret_version = 'latest'

def exit_cf():
    print('No ENV detected! Exiting...')
    os._exit(os.EX_OK)

if os.getenv('ENV'):
        
    DATASET_NAME = os.getenv("DATASET_NAME", '')
    if os.getenv('ENV') == 'development':
        GCP_PROJECT_NAME = os.getenv("GCP_PROJECT_NAME", '')
    elif os.getenv('ENV') in ['production', 'staging']:
        project_id = "978462882991"
        if os.getenv('ENV') == 'production':
            secret_id = "DB_CONFIG_PROD"
        elif os.getenv('ENV') == 'staging':
            secret_id = "DB_CONFIG_STG"
        client = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/{secret_version}"
        response = client.access_secret_version(name=secret_name)
        secret_config = json.loads(response.payload.data.decode("UTF-8"))

        GCP_PROJECT_NAME = secret_config['GCP_PROJECT_NAME']

    else:
        exit_cf()
else:
    exit_cf()


def get_user_data(limit: int = None):
    try:
        bigquery_client = bigquery.Client()

        query = """SELECT * FROM `apnatime-fbc72.dataset_postgres_production.core_user` """+ (f'LIMIT {limit}' if limit else '')

        query_job = bigquery_client.query(query)
        results = query_job.result()

        return results

    except Exception as bigquery_ex:
        print('bigquery_ex:', bigquery_ex)
        raise bigquery_ex

def user_serializer(user):
    return {
        "id": user.get('id'),
        "username": user.get('username'),
        "email": user.get('email'),
    }

def push_to_kafka(users):
    topic = ''
    
    producer = None

    for user in users:
        producer.produce(topic, key=user.id, value=user)

    # Flushing:
    producer.flush()
    producer.close()

def hello_world(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    users = get_user_data(10)

    # DEBUG
    results = []
    print('users:', users)
    for user in users:
        user_data = user_serializer(user)
        results.append(user_data)

    print(results)

    return 'Working'