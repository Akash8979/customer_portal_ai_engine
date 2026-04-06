import psycopg2
from app.env import DNS_TYPE_CONNECTION_STRING


def get_connection():
    return psycopg2.connect(DNS_TYPE_CONNECTION_STRING)