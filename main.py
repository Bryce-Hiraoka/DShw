from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud import logging
from flask import Flask, request
from waitress import serve
from google.cloud.sql.connector import Connector
import sqlalchemy

app = Flask(__name__)

HTTP_METHODS = ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH']

# initialize parameters
INSTANCE_CONNECTION_NAME = f"{'ds-561-398918'}:{'us-east1'}:{'hw5-database'}"
print(f"Your instance connection name is: {INSTANCE_CONNECTION_NAME}")
DB_USER = "mainUser"
DB_PASS = "Ryozo2011"
DB_NAME = "file_store"

connector = Connector()

# function to return the database connection object
def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    return conn

# create connection pool with 'creator' argument to our connection object function
pool = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn,
    pool_size=20,
    max_overflow=30
)

def db_entry(method=None, country=None, Client_IP=None, TimeStamp=None, RequestedFile=None, Gender=None, Age=None, Income=None, isBanned=None, exists=True):

    with pool.connect() as db_conn:
        insert_stmt = sqlalchemy.text(f'INSERT INTO RequestErrors (TimeOfRequest, RequestedFile, ErrorCode) VALUES (:TimeOfRequest, :RequestedFile, :ErrorCode)')
        if country and country.lower() in banned_countries else 0:
            db_conn.execute(insert_stmt, parameters={"TimeOfRequest": TimeStamp, "RequestedFile": RequestedFile, "ErrorCode": 400})
        if method != "GET":
            db_conn.execute(insert_stmt, parameters={"TimeOfRequest": TimeStamp, "RequestedFile": RequestedFile, "ErrorCode": 501})
        if not exists:
            db_conn.execute(insert_stmt, parameters={"TimeOfRequest": TimeStamp, "RequestedFile": RequestedFile, "ErrorCode": 404})
        
        db_conn.execute(sqlalchemy.text(f"INSERT INTO Requests (Country, Client_IP, TimeStamp, RequestedFile, Gender, Age, Income, isBanned) VALUES ('{country}', '{Client_IP}', '{TimeStamp}', '{RequestedFile}', '{Gender}', '{Age}', '{Income}', {isBanned})"))
        db_conn.commit()

    return

# set up pub sub
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('ds-561-398918', 'hw4-topic')

# set up logging; log into web-server-hw04
client = logging.Client()
logging_client = client.logger('web-server-hw04')

@app.route('/', defaults={'path': ''}, methods=HTTP_METHODS)
@app.route('/<path:path>', methods=HTTP_METHODS)
def get_file(path):
  # get country from header X-country
  country = request.headers.get('X-country')
  client_IP = request.headers.get('X-client-IP')
  time = request.headers.get('X-time')
  gender = request.headers.get('X-gender')
  age = request.headers.get('X-age')
  income = request.headers.get('X-income')
  file_name = '/'.join(path.split('/')[1:])

  # publish to banned-countries topic if country is banned
  # (North Korea, Iran, Cuba, Myanmar, Iraq, Libya, Sudan, Zimbabwe and Syria)
  banned_countries = ['north korea', 'iran', 'cuba', 'myanmar', 'iraq', 'libya', 'sudan', 'zimbabwe', 'syria']

  # only accept GET method
  if request.method != 'GET':
    logging_client.log_text(f'Method not implemented: {request.method}')
    db_entry(
            method=request.method, 
            country=country, 
            Client_IP=client_ip, 
            TimeStamp=time, 
            RequestedFile=filename, 
            Gender=gender, 
            Age=age, 
            Income=income, 
            isBanned=1 if country in banned_countries else 0
        )
    return 'Method not implemented', 501

  # if the country is banned, publish to banned-countries topic
  if country and country.lower() in banned_countries:
    publisher.publish(topic_path, country.encode('utf-8'))
    logging_client.log_text(f'Banned country: {country}')
    db_entry(
            method=request.method, 
            country=country, 
            Client_IP=client_IP, 
            TimeStamp=time, 
            RequestedFile=file_name, 
            Gender=gender, 
            Age=age, 
            Income=income, 
            isBanned=1 if country and country.lower() in banned_countries else 0
        )
    return 'Banned country', 400

  # get dirname/filename.html from path
  # path should be bucket_name/dirname/filename.html
  bucket_name = path.split('/')[0]

  if file_name is None:
    print('file_name is required')
    return 'file_name is required', 400
  
  if bucket_name is None:
    print('bucket_name is required')
    return 'bucket_name is required', 400
  
  # get file from bucket
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(file_name)

  if blob.exists():
    return blob.exists()
    blob_content = blob.download_as_string()
    db_entry(
            method=request.method, 
            country=country, 
            Client_IP=client_IP, 
            TimeStamp=time, 
            RequestedFile=file_name, 
            Gender=gender, 
            Age=age, 
            Income=income, 
            isBanned=1 if country and country.lower() in banned_countries else 0
        )
    return blob_content, 200, {'Content-Type': 'text/html; charset=utf-8'}
  
  logging_client.log_text(f'File not found: {bucket_name}/{file_name}')
  db_entry(
            method=request.method, 
            country=country, 
            Client_IP=client_IP, 
            TimeStamp=time, 
            RequestedFile=file_name, 
            Gender=gender, 
            Age=age, 
            Income=income, 
            isBanned=1 if country and country.lower() in banned_countries else 0,
            exists=False
        )
  return 'File not found', 404

serve(app, host='0.0.0.0', port=8080)