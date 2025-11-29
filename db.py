import os
import psycopg2
import psycopg2.extras

def get_conn():
    url = os.environ["DATABASE_URL"]
    return psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
