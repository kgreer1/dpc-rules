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


# In[4]:


df = spark.read.option("header", "true").csv("address-linkage-key/address_link/data/test/*small*.gz")
df.limit(10).toPandas()


# In[5]:


df.printSchema()


# In[6]:


#add dpc column to dataframe
df = df.withColumn('dpc', f.lit(None))


# In[7]:


df.printSchema()


# In[9]:


from pyspark.sql.functions import col
import re


# In[10]:


##df = df.filter(df.dwellingtype == "S")


# In[13]:


df.count()


# In[16]:


'''
def get_dpc(address_line):
    #split address line string elements into a list
    address_split = address_line.split()

    #use regex to match and then return the last two values in the specified element
    #returns no value if address line does not match pattern
    if re.match(r'^\d{2}', address_line):
        res = address_split[0]
        return res[-2] + res[-1]
    elif re.match(r'PO BOX \d{2,5}', address_line):
        res = address_split[2]
        return res[-2] + res[-1]
    elif re.match(r'RR \d+ BOX \d{2,5}', address_line):
        res = address_split[3]
        return res[-2] + res[-1]
    elif re.match(r'HC \d+ BOX \d{2,5}', address_line):
        res = address_split[3]
        return res[-2] + res[-1]
    else:
        return
'''


# In[15]:


#get_dpc('1234 MAIN ST')


# In[11]:





# In[12]:


#convert python function into PySpark SQL UDF
#returns a string
#getdpcUDF = f.udf(lambda z: get_dpc(z),StringType())


# In[13]:


#df = df.withColumn('dpc', getdpcUDF(col('address_line_1')))


# In[14]:


#df.limit(10).toPandas()


# In[15]:


#dpc = dpcUDF(col('address_line_1'))

#create new dataframe with calculated dpc
#uses when function to exclude rows that where dwelling type is equal to M

#df2 = df.withColumn('dpc',
                    #f.when(
                        #f.col('dwellingtype') != "M", 
                        #getdpcUDF(col('address_line_1'))
                    #).otherwise(f.col('dpc'))
                   #)


# In[27]:


df4 = df.withColumn('dpc', (f.regexp_extract(f.col('address_line_1'),'((?<!\S)\d+(?!\S))', 1)).substr(-2,2))
df4.limit(100).toPandas() 


# In[ ]:





# In[ ]:





# In[ ]:


"""
test_values = ['1234 MAIN ST', 'PO BOX 4444', 'RR 12 BOX 154', 'HC 1 BOX 1264', 
               '4 Main St', 'Main St', 'RR 12', 'RR 1 BOX 123/124/12']

for i in range(len(test_values)):
    address_line = test_values[i]
    address_split = address_line.split()
    print('Address Line 1: ' + address_line)

    if re.match(r'^\d{2}', address_line):
        res = address_split[0]
        print('DPBC: ' + res[-2] + res[-1])
    elif re.match(r'PO BOX \d{2,5}', address_line):
        res = address_split[2]
        print('DPBC: ' + res[-2] + res[-1])
    elif re.match(r'RR \d+ BOX \d{2,5}', address_line):
        res = address_split[3]
        print('DPBC: ' + res[-2] + res[-1])
    elif re.match(r'HC \d+ BOX \d{2,5}', address_line):
        res = address_split[3]
        print('DPBC: ' + res[-2] + res[-1])
    else:
        print('exception not accounted for')
"""


# In[ ]:




