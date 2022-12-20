import os

def get_connection():
    connection = {
    'host': os.environ.get('DPO_DB_HOST'),
    'password': os.environ.get('DPO_DB_PASSWORD'),
    'user': 'student',
    'database': os.environ.get('DPO_DB_DATABASE')
}

def load_connection():
    connection_load = {
    'host': os.environ.get('DPO_DB_HOST'),
    'password': os.environ.get('DPO_DB_PASSWORD_2'),,
    'user': 'student-rw',
    'database': 'test'
}