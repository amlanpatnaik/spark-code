#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from time import time
import configparser
import matplotlib.pyplot as plt
import pandas as pd
import boto3  #https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html


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


s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )

sampleDbBucket =  s3.Bucket("udacity-labs")

for obj in sampleDbBucket.objects.filter(Prefix="tickets"):
    print(obj)


# In[ ]:


for bucket in s3.buckets.all():
    print(bucket.name)


# In[ ]:


# Upload a new file
data = open('pagila-star.png', 'rb')
s3.Bucket('amlan').put_object(Key='test.jpg', Body=data)

