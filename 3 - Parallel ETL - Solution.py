#!/usr/bin/env python
# coding: utf-8

# # Exercise 3: Parallel ETL

# In[ ]:


#!pip install boto3
#!pip install sql
#!pip install ipython-sql


# In[ ]:


from time import time
import configparser
import matplotlib.pyplot as plt
import pandas as pd
import boto3


# # STEP 1: Get the params of the created redshift cluster 
# - We need:
#     - The redshift cluster <font color='red'>endpoint</font>
#     - The <font color='red'>IAM role ARN</font> that give access to Redshift to read from S3

# In[ ]:


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


# FILL IN THE REDSHIFT ENPOINT HERE
# e.g. DWH_ENDPOINT="redshift-cluster-1.xxxxxxx.us-west-2.redshift.amazonaws.com" 
DWH_ENDPOINT="redshift-cluster-1.xxxxxxx.us-west-1.redshift.amazonaws.com" 
    
#FILL IN THE IAM ROLE ARN you got in step 2.2 of the previous exercise
#e.g DWH_ROLE_ARN="arn:aws:iam::988332130976:role/dwhRole"
DWH_ROLE_ARN="arn:aws:iam::xxxxxxx:role/myRedshiftRole"


# # STEP 2: Connect to the Redshift Cluster

# In[ ]:


conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
conn_string


# In[ ]:


import psycopg2


# In[ ]:


connection = psycopg2.connect(conn_string)
cursor = connection.cursor()


# In[ ]:


cursor


# In[ ]:


import boto3

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )

sampleDbBucket =  s3.Bucket("udacity-labs")

for obj in sampleDbBucket.objects.filter(Prefix="tickets"):
    print(obj)


# # STEP 3: Create Tables

# In[ ]:



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


# # STEP 4: Load Partitioned data into the cluster

# In[ ]:


qry = """
    copy sporting_event_ticket from 's3://udacity-labs/tickets/split/part'
    credentials 'aws_iam_role={}'
    gzip delimiter ';' compupdate off region 'us-west-2';
""".format(DWH_ROLE_ARN)

cursor.execute(qry)


# In[ ]:


cursor.close()


# # STEP 4: Create Tables for the non-partitioned data

# In[ ]:


get_ipython().run_cell_magic('sql', '', 'DROP TABLE IF EXISTS "sporting_event_ticket_full";\nCREATE TABLE "sporting_event_ticket_full" (\n    "id" double precision DEFAULT nextval(\'sporting_event_ticket_seq\') NOT NULL,\n    "sporting_event_id" double precision NOT NULL,\n    "sport_location_id" double precision NOT NULL,\n    "seat_level" numeric(1,0) NOT NULL,\n    "seat_section" character varying(15) NOT NULL,\n    "seat_row" character varying(10) NOT NULL,\n    "seat" character varying(10) NOT NULL,\n    "ticketholder_id" double precision,\n    "ticket_price" numeric(8,2) NOT NULL\n);')


# # STEP 5: Load non-partitioned data into the cluster
# - Note how it's slower than loading partitioned data

# In[ ]:


get_ipython().run_cell_magic('time', '', '\nqry = """\n    copy sporting_event_ticket_full from \'s3://udacity-labs/tickets/full/full.csv.gz\' \n    credentials \'aws_iam_role={}\' \n    gzip delimiter \';\' compupdate off region \'us-west-2\';\n""".format(DWH_ROLE_ARN)\n\n%sql $qry')


# In[ ]:




