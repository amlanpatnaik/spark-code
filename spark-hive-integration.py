
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession


# In[4]:


spark = SparkSession.builder.appName("Spark Hive Int").config("spark.some.config.option","some-value").enableHiveSupport().getOrCreate()


# In[18]:


spark.sql("create table if not exists aj.saleshive (custid string,orderid string,orderdate string,shipdate string,shipclass string,profit float, tax float, total float) row format delimited fields terminated by ','")


# In[19]:


spark.sql("LOAD DATA INPATH 'hdfs://nameservice1/user/edureka_252252/edureka_252252/sales.csv' INTO TABLE aj.saleshive")


# In[20]:


spark.sql("SELECT * from aj.saleshive limit 5").show()


# In[22]:


df = spark.sql("SELECT orderid,shipdate,shipclass from aj.saleshive limit 5")


# In[23]:


df.write.saveAsTable("aj.shippinginfo")


# In[29]:


df.write.parquet("shippinginfoPARQUET1")


# In[33]:


df.write.orc("shippinginfoORC1")

