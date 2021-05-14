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


columns = ["address_line_1", "address_line_2", "expected_dpc"]
data = [("PO BOX 44", None, "44"),
        ("44 PO BOX", None, "44"),
        ("RR 1 BOX 154", None, "54"), 
        ("HC 1 BOX 1264", None, "64"),
        ("HC 1", None, "99"),
        ("RR 1 Box 2", None, "02"),
        ("RR 1 Box 154A", None, "54"),
        ("HC 1 Box AB", None, "99"),
        ("1234 Main St", None, "34"),
        ("RR 1 BOX 1.23", None, "23"),
        ("A Main St", None, "99"),
        ("PO Box AA", None, "99"),
        ("8 MAIN St", None, "08"),
        ("N6845 Dan Way", None, "45"),
        ("23S4 MAIN ST", None, "04"),
        ("1550 Ridge Way", "APT 13", "13"),
        ("75 Joseph Ave", None, "75"),
        ("1950 N Point Blvd", None, "50"),
        ("HC 3 BOX 11*7", None, "07"),
        ("1.23 MAIN ST", None, "23"),
        ("23S41 MAIN ST", None, "41"),
        ("W3S1 MAIN ST", None, "01"),
        ("PO Box 1-3", None, "03")]

df_test = spark.createDataFrame(data = data, schema = columns)


# In[3]:


df_test.show(30)


# In[ ]:


#df = df_test.withColumn('housenumber', (f.regexp_extract(f.col('address_line_1'),'(^[0-9]([0-9A-Z.*-]+)?)', 1)))
#df.show()


# In[ ]:


#df = df.withColumn('boxnumber', (f.regexp_extract(f.col('address_line_1'),'([Bb][Oo][Xx]\s)([0-9]+[0-9A-Z.*-]*)', 2)))
#df.show()


# In[ ]:


#df.filter(df.address_line_1.rlike('^[A-Z]')).limit(10).toPandas()


# In[4]:


def blanks_to_null(x):
    return f.when(f.col(x) != "", f.col(x)).otherwise(None)


# In[5]:


def house_number_extract(df):
    #make address_line_1 all uppercase
    df = df.withColumn('address_line_1', f.upper('address_line_1'))
    
    #extract house number or box number into column housenumber
    df = df.withColumn('housenumber',
                      f.when(
                          f.col('address_line_1').rlike('^[A-Z]{2}'),
                          f.regexp_extract(f.col('address_line_1'),'(BOX\\s)([0-9]+[0-9A-Z.*-]*)', 2))
                       .otherwise(f.regexp_extract(f.col('address_line_1'),'^([A-Z]*[0-9]+[0-9A-Z.*-]*)', 1)))
    return df


# In[6]:


df_test = house_number_extract(df_test)
df_test.show(30)


# In[ ]:


# run blank function on 'housenumber' column to replace blanks with 'None'.
df_test = df_test.withColumn('housenumber', blanks_to_null('housenumber'))
df_test.show(30)


# In[ ]:





# In[ ]:


#rule1
df_test = df_test.withColumn('dpc', 
        f.when(
            f.col('address_line_2').isNull() &
            f.col('housenumber').isNotNull() & 
            f.col('housenumber').rlike('^[0-9]*$'),
            f.col('housenumber').substr(-2,2)))
df_test.show(30)


# In[ ]:


#rule5
df_test = df_test.withColumn('dpc', 
              f.when(
                   f.col('address_line_2').isNull() &
                   f.col('housenumber').isNotNull() & 
                   f.col('dpc').isNull() &
                   #f.col('housenumber').rlike('^[a-zA-Z0-9]*$'),
                   f.col('housenumber').rlike('^[0-9]+[A-Z]+$'),
                   f.regexp_extract(f.col('housenumber'),'(\d+)',1).substr(-2,2)).otherwise(f.col('dpc')))
df_test.show(30)


# In[ ]:





# In[ ]:





# In[ ]:


#rule8

# create new column that selects the first word from the address_line_1 string
df_test = df_test.withColumn('alphas', (f.regexp_extract(f.col('address_line_1'),'(^[A-Z0-9]+[0-9]\\w)', 1)))
    
# update dpc when alphas contains a value, add_line_2 is null and dpc is null
df_test = df_test.withColumn('dpc', 
        f.when(
            f.col('alphas').isNotNull() &
            f.col('address_line_2').isNull() &
            f.col('dpc').isNull(),
            f.regexp_extract(f.col('alphas'),'([0-9]{1,2}$)',1)).otherwise(f.col('dpc')))

df_test = df_test.withColumn('dpc', blanks_to_null('dpc'))

df_test.show(30)


# In[ ]:





# In[ ]:


#rule10
df_test = df_test.withColumn('dpc', 
            f.when(
                #This specifies that if 'dpc' is not null, then that value should be retained.
                f.col('dpc').isNotNull(),
                f.col('dpc'))
                             .otherwise(f.regexp_extract(f.col('housenumber'), '([0-9]+)([.*-])([0-9]+)', 3)))

df_test = df_test.withColumn('dpc', blanks_to_null('dpc'))

df_test.show(30)


# In[ ]:


#rule13
df_test = df_test.withColumn('dpc', f.coalesce(f.col('dpc'), f.lit('99')))
df_test.show(30)


# In[ ]:


#rule3
df_test = df_test.withColumn('dpc', f.lpad('dpc', 2, '0'))
df_test.show(30)


# In[ ]:




