
import boto3
import awswrangler as wr
import pandas as pd
import re
import argparse
import os
import pymysql
import logging
import json
import sys
import urllib.request


logging.basicConfig(stream=sys.stdout, level=logging.INFO)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)


# OPTIONAL ARGUMENT PARSER (getResolvedOptions doesn't support optional arguments at the moment)
arg_parser = argparse.ArgumentParser(
    description="Reads a CSV file from S3 and loads records into a table", 
    epilog="""
    NOTE: If the table does not exist, the columns and column types will be inferred from the CSV. The underlying method for donig this is to call pandas.DataFrame.convert_dtypes(). You may need to predefine the table/schema if you need more control.

    If using an AWS Secrets Manager secret, it must be formmated in JSON like this example:
    {
        "username": "myusername",
        "password": "mypassword",
        "host": "host.domain.com",
        "port": "3306",
        "dbname": "my-database-name"
    }
    """
)
arg_parser.add_argument('--s3_object', required=True, help="The S3 path where the CSV object is located. Must be formatted 's3://bucket-name/prefix/object.csv'")
arg_parser.add_argument('--db_host', required=False, default=None, help="The hostname of the database server to connect to. If not set, will be retrieved from secrets manager secret.")
arg_parser.add_argument('--db_port', type=int, required=False, default=None, help="The port of the database server to connect to. If not set, will be retrieved from secrets manager secret or default port will be used.")
arg_parser.add_argument('--db_name', required=False, default=None, help="The name of the database where the data will be inserted. If not set, will be retrieved from secrets manager secret.")
arg_parser.add_argument('--db_user', required=False, default=None, help="The database user to connect as. It is HIGHLY recommended you store your credentials in secrets manager and use db_secret_arn instead!")
arg_parser.add_argument('--db_password', required=False, default=None, help="The database user password. It is HIGHLY recommended you store your credentials in secrets manager and use db_secret_arn instead!")
arg_parser.add_argument('--db_secret_arn', required=False, help="ARN of AWS Secrets Manager secret containing the credentials for conneting to the database server")
arg_parser.add_argument('--table_name', required=True, help="The name of the table that records will be inserted into. NOTE: The table will be created if it does not exist.")
arg_parser.add_argument('--drop_table', action='store_true', required=False, default=False, help="When set, the database table will be dropped if it already exists and recreated.")
arg_parser.add_argument('--delete_rows', action='store_true', required=False, default=False, help="When set, all existing rows in the table will be dropped.")
arg_parser.add_argument('--delete_mode', type=str, required=False, default="TRUNCATE", help="Set to TRUNCATE OR DELETE to determine how to delete existing rows when --delete_rows is set")
arg_parser.add_argument('--chunk_size', type=int, required=False, default=10000, help="How many rows form the csv to process per iteration. More rows requires more memory. Default is 10,000")
arg_parser.add_argument('--ssl_ca_url', type=str, required=False, default="https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem", help="A URL to the SSL CA that will be downloaded and used for RDS authentication.")


# Not used, but included because Glue passes these arguments in
arg_parser.add_argument('--extra-py-files', type=str, required=False, default=None, help="NOT USED")
arg_parser.add_argument('--scriptLocation', type=str, required=False, default=None, help="NOT USED")
arg_parser.add_argument('--job-bookmark-option', type=str, required=False, default=None, help="NOT USED")
arg_parser.add_argument('--job-language', type=str, required=False, default=None, help="NOT USED")
arg_parser.add_argument('--connection-names', type=str, required=False, default=None, help="NOT USED")


args = vars(arg_parser.parse_args())
log.info({i:args[i] for i in args if i not in ['db_password']})

S3_OBJECT = args["s3_object"]
DB_HOST = args["db_host"]
DB_PORT = args["db_port"]
DB_NAME = args["db_name"]
DB_USER = args["db_user"]
DB_PASSWORD = args["db_password"]
DB_SECRET_ARN = args["db_secret_arn"]
TABLE_NAME = args["table_name"]
DROP_TABLE = args["drop_table"]
DELETE_ROWS = args["delete_rows"]
DELETE_MODE = args["delete_mode"].strip().upper()
if DELETE_MODE not in ['TRUNCATE', 'DELETE']: raise arg_parser.error("{} is not a valid delete mode".format(DELETE_MODE))
CHUNK_SIZE = args["chunk_size"]
SSL_CA = args["ssl_ca_url"]

log.info('Getting CA Bundle from URL.')
try:
    urllib.request.urlretrieve(SSL_CA, './rds-combined-ca-bundle.pem')
except:
    log.error('An Unexpected Error Occurred when retrieving the SSL CA bundle...')

if not DB_SECRET_ARN is None:
    # Fetch the password from Secrets Manager
    log.info('Fetching Secret: {}'.format(DB_SECRET_ARN))
    secretsmanager_client = boto3.client(service_name='secretsmanager')
    db_credentials = secretsmanager_client.get_secret_value(SecretId=DB_SECRET_ARN)['SecretString']
    log.info('Retrieved secret')
    db_credentials = json.loads(db_credentials)
    if DB_USER is None: DB_USER = db_credentials['username']
    if DB_PASSWORD is None: DB_PASSWORD = db_credentials['password']
    if DB_HOST is None: DB_HOST = db_credentials['host']
    if DB_NAME is None: DB_NAME = db_credentials['dbname']
    if DB_PORT is None: int(db_credentials.get('port', 0))

if DB_USER is None: raise arg_parser.error("Database username must be specified either in --db_user argument or as ""username"" in secrets manager secret.")
if DB_PASSWORD is None: raise arg_parser.error("Database password must be specified either in --db_password argument or as ""password"" in secrets manager secret.")
if DB_HOST is None: raise arg_parser.error("Database hostname must be specified either in --db_host argument or as ""host"" in secrets manager secret.")
if DB_NAME is None: raise arg_parser.error("Database hostname must be specified either in --db_name argument or as ""dbname"" in secrets manager secret.")


con = pymysql.connect(
    host=DB_HOST,                         
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    port=DB_PORT
)

dfs = wr.s3.read_csv(
    path=[S3_OBJECT], 
    chunksize=CHUNK_SIZE
)

row_count = 0
def table_exists(con, table_name, db_name):
    with con.cursor() as cursor:
        cursor.execute("SELECT 1 as exist FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME='{}'".format(db_name, table_name))
        if cursor.fetchone():
            return True
        else: 
            return False
    
if DROP_TABLE:
    log.info("DROP TABLE IF EXISTS {}.{}".format(DB_NAME, TABLE_NAME))
    with con.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS {}.{}".format(DB_NAME, TABLE_NAME))
elif DELETE_ROWS:
    if table_exists(con, TABLE_NAME, DB_NAME):
        if DELETE_MODE.strip().upper() == 'TRUNCATE':
            log.info("TRUNCATE TABLE {}.{}".format(DB_NAME, TABLE_NAME))
            with con.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE {}.{}".format(DB_NAME, TABLE_NAME))
        else:
            log.info("DELETE FROM {}.{}".format(DB_NAME, TABLE_NAME))
            with con.cursor() as cursor:
                cursor.execute("DELETE FROM {}.{}".format(DB_NAME, TABLE_NAME))   
    else:
        log.info('Table doesn\'t exist, skipping row deletion.')
                


for df in dfs:
    df = df.convert_dtypes()
    row_count = row_count+len(df.index)
    log.info("Inserting {} rows".format(len(df.index)))
    wr.mysql.to_sql(
        df=df,
        table=TABLE_NAME,
        index=False,
        schema=DB_NAME,
        mode="append",
        con=con,
        ssl_ca='./rds-combined-ca-bundle.pem'
    )
    

db_count = wr.mysql.read_sql_query(
    sql="SELECT count(*) as count FROM {}.{}".format(DB_NAME, TABLE_NAME),
    con=con
)
con.close()
log.info("DB Table row count: {}".format(db_count['count'][0]))
log.info("CSV row count: {}".format(row_count))

log.info("Finished")


