#!/usr/bin/env python
# coding: utf-8

# In[4]:


from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
    example spark program
    """
    
    spark = SparkSession        .builder        .appName("LowerSongTitles")        .getOrCreate()
    
    log_of_songs = [
        "Despicable",
        "Nice for what"
    ]
    
    distributed_song = spark.sparkContext.parallelize(log_of_songs)
    
    print(distributed_song.map(lambda x: x.lower()).collect())
    
    spark.stop()


# In[ ]:




