#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os; import sys; import re

# common spark import
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

# connect to spark if we haven't already
if not 'spark' in locals():
  spark = SparkSession.builder       .master("local[*]")       .appName('development')       .config("spark.sql.debug.maxToStringFields", str(1024 * 1024))       .getOrCreate()
  sc = spark.sparkContext

print("Connected to Spark!")


# In[2]:


df = spark.read.option("header", "true").csv("address-linkage-key/address_link/data/test/*medium*.gz")
df.limit(10).toPandas()


# In[3]:


df = df.withColumn('dpc', f.lit(None))


# In[4]:


df = df.withColumn('housenumber', (f.regexp_extract(f.col('address_line_1'),'(^[0-9]([0-9A-Z.*-]+)?)', 1)))
df.limit(10).toPandas()


# In[6]:


#rule1
df = df.withColumn('dpc',
                   f.when(
                       f.col('address_line_2').isNull() &
                       f.col('housenumber').isNotNull() &
                       f.col('housenumber').rlike('^[0-9]*$'),
                       f.col('housenumber').substr(-2,2)
))
df.limit(10).toPandas()


# In[7]:


df.filter(df.address_line_1.contains("Box")).limit(10).toPandas()


# In[ ]:





# In[6]:


#rule2
df = df.withColumn('dpc', 
                   f.when((f.col('dpc').isNull()) & (f.col('housenumber').isNull()), 
                          f.lit('99'))
                   .otherwise(f.col('dpc'))
                  )
df.limit(10).toPandas()


# In[7]:


#172
df.filter(df.dpc == '1').count()


# In[8]:


df.filter(df.dpc == '01').count()


# In[9]:


#df.filter(df.dpc == '1').limit(10).toPandas()
df.filter(f.length('dpc') == 1).limit(10).toPandas()


# In[ ]:


#rule 3 with lpad
#df = df.withColumn('dpc', f.lpad('dpc', 2, '0'))
#df.limit(20).toPandas()


# In[13]:


#rule 3 with lpad and more logic
#only lpad if dpc is not length=2
df = df.withColumn('dpc', 
                   f.when(
                       f.length('dpc') == 2, 
                       f.col('dpc'))
                   .otherwise( 
                       f.lpad('dpc', 2, '0')))
df.limit(20).toPandas()


# In[20]:


df.filter(f.length('dpc') == 1).limit(10).toPandas()


# In[16]:


df.filter(df.dpc == '1').count()


# In[17]:


#2289
df.filter(df.dpc == '01').count()


# df = df.withColumn('dpc', f.when(f.col('dpc').isNotNull(), f.col('dpc')).otherwise(
#     whatever you want to do
# )

# In[ ]:


#rule3 mod
df = df.withColumn('dpc',
                   f.when(
                       f.col('housenumber').rlike('\d{2}'),
                       f.col('dpc'))
                   .otherwise(f.concat(f.lit('0'), f.col('dpc')))
                  )


# In[ ]:


#rule3 
#when house number contains a single digit, add a leading zero
#when housenumber contains two digits, leave existing dpc, otherwise add a leading 0
df = df.withColumn('dpc',
                   f.when(
                       f.col('dpc').rlike('\d{2}'),
                       f.col('dpc'))
                   .otherwise(f.concat(f.lit('0'), f.col('dpc')))
                  )
df.filter(df.housenumber == '1').limit(10).toPandas()


# In[ ]:


df.limit(30).toPandas()


# In[ ]:


df.filter(df.housenumber.rlike('^\d{1}$')).limit(10).toPandas()


# In[ ]:


df.filter(df.housenumber.rlike('\d{2}')).limit(10).toPandas()


# In[ ]:


df.filter(df.dpc == '01' & df.housenumber == '1').count()


# In[ ]:


df.limit(30).toPandas()


# In[ ]:


# rule 3
df = df.withColumn('dpc',
                   f.when(
                       f.col('dpc').rlike('\d{2,}'),f.col('dpc'))
                          .otherwise(f.lit('test'))
                         )


df = df.withColumn('dpc', 
                   f.when((f.col('housenumber') == "1")),
                         f.regexp_extract(f.col('housenumber'),'(\d{2,})', 1)
                   .otherwise(f.col('dpc'))
                  )


# In[ ]:




