#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from time import time
import configparser
import matplotlib.pyplot as plt
import pandas as pd
import boto3  #https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html
import psycopg2


# In[ ]:


#retrieve credentials
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
KEY=config.get('AWS','key')
SECRET= config.get('AWS','secret')

DWH_DB= config.get("DWH","DWH_DB")
DWH_DB_USER= config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD= config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH","DWH_PORT")

print(KEY, SECRET)


# In[ ]:


#use boto3 sdk to read files from s3
s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )

sampleDbBucket =  s3.Bucket("udacity-labs")

for obj in sampleDbBucket.objects.filter(Prefix="tickets"):
    print(obj)


# In[ ]:


# FILL IN THE REDSHIFT ENPOINT HERE
# e.g. DWH_ENDPOINT="redshift-cluster-1.xxxxxxx.us-west-2.redshift.amazonaws.com" 
DWH_ENDPOINT="redshift-cluster-1.xxxxxxx.us-west-1.redshift.amazonaws.com" 
    
#FILL IN THE IAM ROLE ARN you got in step 2.2 of the previous exercise
#e.g DWH_ROLE_ARN="arn:aws:iam::988332130976:role/dwhRole"
DWH_ROLE_ARN="arn:aws:iam::xxxxxxx:role/xxxxxxx"


# In[ ]:


#connect to Redshift Cluster
conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)


# In[ ]:


#create Redshift DB Connection
connection = psycopg2.connect(conn_string)
cursor = connection.cursor()


# In[ ]:


#create table

cursor.execute(
"""
select * from "sporting_event_ticket";
""")

cursor.execute("""
CREATE TABLE "sporting_event_ticket" (
    "id" double precision DEFAULT nextval('sporting_event_ticket_seq') NOT NULL,
    "sporting_event_id" double precision NOT NULL,
    "sport_location_id" double precision NOT NULL,
    "seat_level" numeric(1,0) NOT NULL,
    "seat_section" character varying(15) NOT NULL,
    "seat_row" character varying(10) NOT NULL,
    "seat" character varying(10) NOT NULL,
    "ticketholder_id" double precision,
    "ticket_price" numeric(8,2) NOT NULL
);
""")


cursor.execute("""
commit;
""")


# In[ ]:


#Load Partitioned data into the cluster

qry = """
    copy sporting_event_ticket from 's3://udacity-labs/tickets/split/part'
    credentials 'aws_iam_role={}'
    gzip delimiter ';' compupdate off region 'us-west-2';
""".format(DWH_ROLE_ARN)

cursor.execute(qry)


# In[ ]:


#Create Tables for the non-partitioned data
%%sql
DROP TABLE IF EXISTS "sporting_event_ticket_full";
CREATE TABLE "sporting_event_ticket_full" (
    "id" double precision DEFAULT nextval('sporting_event_ticket_seq') NOT NULL,
    "sporting_event_id" double precision NOT NULL,
    "sport_location_id" double precision NOT NULL,
    "seat_level" numeric(1,0) NOT NULL,
    "seat_section" character varying(15) NOT NULL,
    "seat_row" character varying(10) NOT NULL,
    "seat" character varying(10) NOT NULL,
    "ticketholder_id" double precision,
    "ticket_price" numeric(8,2) NOT NULL
);


%%time

#Load non-partitioned data into the cluster

qry = """
    copy sporting_event_ticket_full from 's3://udacity-labs/tickets/full/full.csv.gz' 
    credentials 'aws_iam_role={}' 
    gzip delimiter ';' compupdate off region 'us-west-2';
""".format(DWH_ROLE_ARN)

get_ipython().run_line_magic('sql', '$qry')

