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


df = spark.read.option("header", "true").csv("address-linkage-key/address_link/data/test/*small*.gz")
df.limit(10).toPandas()


# In[3]:


def blanks_to_null(x):
    return f.when(f.col(x) != "", f.col(x)).otherwise(None)


# In[4]:


def house_number_extract(df):
    #make address_line_1 all uppercase
    df = df.withColumn('address_line_1', f.upper('address_line_1'))
    
    #extract house number or box number into column housenumber
    df = df.withColumn('housenumber',
                      f.when(
                          df.address_line_1.rlike('^[A-Z]{2}'),
                          f.regexp_extract(f.col('address_line_1'),'(BOX\\s)([0-9]+[0-9A-Z.*-]*)', 2))
                       .otherwise(f.regexp_extract(f.col('address_line_1'),'^([A-Z]*[0-9]+[0-9A-Z.*-]*)', 1)))
    return df


# In[5]:


df = house_number_extract(df)
df.limit(30).toPandas()


# In[6]:


# run blank function on 'housenumber' column to replace blanks with 'None'.
df = df.withColumn('housenumber', blanks_to_null('housenumber'))


# In[7]:


#rule1
df = df.withColumn('dpc', 
        f.when(
            f.col('address_line_2').isNull() &
            f.col('housenumber').isNotNull() & 
            f.col('housenumber').rlike('^[0-9]*$'),
            f.col('housenumber').substr(-2,2)))
df.limit(10).toPandas()


# In[8]:


#rule5
df = df.withColumn('dpc', 
                   f.when(
                       f.col('address_line_2').isNull() &
                       f.col('housenumber').isNotNull() & 
                       f.col('dpc').isNull() &
                       #f.col('housenumber').rlike('^[a-zA-Z0-9]*$'),
                       f.col('housenumber').rlike('^[0-9]+[A-Z]+$'),
                       f.regexp_extract(f.col('housenumber'),'(\d+)',1).substr(-2,2))
                   .otherwise(f.col('dpc')))
df.limit(10).toPandas()


# In[ ]:





# In[ ]:





# In[9]:


#rule8

# create new column that selects the first word from the address_line_1 string
df = df.withColumn('alphas', (f.regexp_extract(f.col('address_line_1'),'(^[A-Z0-9]+[0-9]\\w)', 1)))
    
# update dpc when alphas contains a value, add_line_2 is null and dpc is null
df = df.withColumn('dpc', 
        f.when(
            f.col('alphas').isNotNull() &
            f.col('address_line_2').isNull() &
            f.col('dpc').isNull(),
            f.regexp_extract(f.col('alphas'),'([0-9]{1,2}$)',1)).otherwise(f.col('dpc')))

df = df.withColumn('dpc', blanks_to_null('dpc'))


# In[ ]:





# In[11]:


#rule10
df = df.withColumn('dpc', 
            f.when(
                #This specifies that if 'dpc' is not null, then that value should be retained.
                f.col('dpc').isNotNull(),
                f.col('dpc'))
                             .otherwise(f.regexp_extract(f.col('housenumber'), '([0-9]+)([.*-])([0-9]+)', 3)))

df = df.withColumn('dpc', blanks_to_null('dpc'))

df.limit(30).toPandas()


# In[12]:


#rule13
df = df.withColumn('dpc', f.coalesce(f.col('dpc'), f.lit('99')))


# In[13]:


#rule3
df = df.withColumn('dpc', f.lpad('dpc', 2, '0'))
df.limit(30).toPandas()


# In[ ]:




