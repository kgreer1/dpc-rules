#!/usr/bin/env python
# coding: utf-8

# In[2]:


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


# In[88]:


data = [("Main St","Suite 100098","100098","","98"),
        ("Main St","Suite 2","2","","02"),
        ("Main St","Suite 98","98","","98"),
        ("Main St","Suite 100 99","100 99","","72"),
        ("75 Joseph Ave","Apt 306","306","","81"),
        ("Algonquin Way","Apt 683","683","","58"),
        ("1950 N Point Blvd","Unit 1001","1001","","51"),
        ("A Main St","Apt 8874","8874","","24"),
        ("2 Muirfield Run","Apt 14-102","14-102","","58"),
        ("1234A MAIN ST","Suite 98","98","","98"),
        ("525 Circle Dr","Unit 14B","14B","","25")]

columns = ["address_line_1","address_line_2","unitdesignatornumber","dpc","expected_dpc"]
df = spark.createDataFrame(data = data, schema = columns)


# In[89]:


def blanks_to_null(x):
    return f.when(f.col(x) != "", f.col(x)).otherwise(None)


# In[90]:


df = df.withColumn('dpc', blanks_to_null('dpc'))


# In[91]:


#df = df.withColumn('unitNumber', (f.regexp_extract(f.col('unitdesignatornumber'),'(^[0-9]([0-9]+)?)', 1)))
#df.limit(10).toPandas()


# In[92]:


df = df.withColumn('unitnumber_int', f.regexp_replace(f.col('unitdesignatornumber'),'[- ]','').cast('integer'))
df.limit(15).toPandas()


# In[76]:


df = df.withColumn('unitnumber_hundreds', ((f.col('unitnumber_int') /100) % 10).cast('integer'))
df.limit(15).toPandas()


# In[77]:


df = df.withColumn('unitnumber_thousands', ((f.col('unitnumber_int') / 1000) % 10).cast('integer'))
df.limit(15).toPandas()


# In[78]:


df = df.withColumn('dpc2',
                   f.when(
                       f.col('unitnumber_hundreds') + f.col('unitnumber_thousands') == 0,
                       f.col('unitdesignatornumber').substr(-2,2)))
df.limit(15).toPandas()


# In[79]:


df = df.withColumn('dpc2',
                   f.when(
                       f.col('dpc2').isNotNull(),
                       f.lpad('dpc2', 2, '0')))
df.limit(15).toPandas()teams


# In[80]:


#df = df.withColumn('unitnumber_int', 
                   #(f.regexp_extract(f.col('unitdesignatornumber'),'(^[0-9]([0-9]+)?)', 1)))
#df.limit(10).toPandas()


# In[81]:


df = df.withColumn('unitnumber_x', 
                   (f.col('unitnumber_int') / f.lit(100)).cast('integer'))
df.limit(15).toPandas()


# In[82]:


df = df.withColumn('unitnumber_y', 
                   f.col('unitnumber_int') % f.lit(100))
df.limit(15).toPandas()


# In[83]:


df = df.withColumn('unitnumber_sum', 
            f.lit(25) * (f.col('unitnumber_x') % 4) + (f.col('unitnumber_y') % 25))
df.limit(15).toPandas()


# In[ ]:





# In[87]:


df = df.withColumn('dpc2',
                   f.when(
                       f.col('dpc2').isNotNull(),
                       f.col('dpc2'))
                   .when(
                       f.col('unitnumber_int') > f.lit(100),
                       f.col('unitnumber_sum').cast('string')))
                       
                       #f.col('unitnumber_int') > f.lit(100)
                       #f.col('unitnumber_sum').cast('string')))
df.limit(15).toPandas()


# In[ ]:





# In[ ]:


example = '14102'


# In[ ]:


X = int(example[-4] + example[-3])
Y = int(example[-2] + example[-1])


# In[ ]:


X


# In[ ]:


X % 4


# In[ ]:


X_mod * 25


# In[ ]:


Y


# In[ ]:


Y % 25


# In[ ]:


dpc = (25 * (X % 4)) + (Y % 25)
dpc


# In[ ]:


example2 = '11001'


# In[ ]:


if int(example2[-4] + example2[-3]) > 0:
    print('true')


# In[ ]:


def secondaryrule5(unitnumber):
    # return None when unit number is less than 3 values, does not apply
    if len(unitnumber) < 3:
        return None
    # applies if unit number is 3 values
    if len(unitnumber) == 3:
        X = int(unitnumber[-3])
        Y = int(unitnumber[-2] + unitnumber[-1])
        dpc = (25 * (X % 4)) + (Y % 25)
        return dpc
    # applies if unit number is greater than 3 values
    # and hundreds and thousands place is greater than 0
    if len(unitnumber) > 3 & int(unitnumber[-4] + unitnumber[-3]) > 0:
        X = int(unitnumber[-4] + unitnumber[-3])
        Y = int(unitnumber[-2] + unitnumber[-1])
        dpc = (25 * (X % 4)) + (Y % 25)
        return dpc


# In[ ]:


secondaryrule5('1001')


# In[ ]:


secondaryrule5('306')


# In[ ]:


secondaryrule5('14102')


# In[ ]:


secondaryrule5('10001')


# In[ ]:


@f.udf(returnType = StringType())
def spark_secondaryrule5(unitnumber):
    return secondaryrule5(unitnumber)


# In[ ]:


df2 = df.withColumn('dpc2', spark_secondaryrule5(f.col('unitdesignatornumber')))
df2.show()


# In[ ]:





# In[ ]:





# In[ ]:




