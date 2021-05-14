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


# # Test Data

# In[70]:


data = [("Main St","","","99"),
        ("75 Joseph Ave","Apt 205","","75"),
        ("Algonquin Way","","","99"),
        ("2 Muirfield Run","","","02"),
        ("1950 N Point Blvd","","","50"),
        ("N8603 Carper Rd","","","03"),
        ("1234A MAIN ST","","","34"),
        ("HC 1 Box 1264","","","64"),
        ("525 Circle Dr","","","25"),
        ("RR Box AA","","","99"),
        ("A Main St","Apt 12","","99")]

columns = ["address_line_1","address_line_2","dpc","expected_dpc"]
df_test = spark.createDataFrame(data = data, schema = columns)


# In[71]:


df_test.show()


# In[72]:


df_test = df_test.withColumn('housenumber', 
                             (f.regexp_extract(f.col('address_line_1'),'(^[0-9]([0-9A-Z.*-]+)?)', 1)))
df_test.show()


# In[73]:


# rule 1
#df_test = df_test.withColumn('dpc', (f.regexp_extract(f.col('address_line_1'),'(^[0-9]([0-9A-Z.*-]+)?)', 1)))
df_test = df_test.withColumn('dpc', f.when((f.col('address_line_2') == "") &
(f.col('housenumber')).isNotNull() & f.col('housenumber').rlike('^[0-9]*$'),
(f.regexp_extract(f.col('housenumber'),'(\d+)',1)).substr(-2,2)
).otherwise(f.col('dpc')))
df_test.show()


# In[74]:


# rule 2
df_test = df_test.withColumn('dpc', 
                   f.when((f.col('dpc') == "") & (f.col('housenumber') == ""), 
                          f.lit('99'))
                   .otherwise(f.col('dpc'))
                  )
df_test.show()


# In[ ]:


#if address line 1 does not start with a number, use 99
#from pyspark.sql.functions import col, when

#df2 = df_test.withColumn('dpc', f.when(f.col('address_line_1') == 'Main St', f.lit('99'))
                    #.otherwise(f.col('dpc')))

#df2.show()


# In[ ]:


#pattern = '^[a-z,A-Z]{2,}\s[a-z,A-Z]+'

#df3 = df_test.withColumn('dpc', 
                    f.when(f.col('address_line_1').rlike(pattern), f.lit('99'))
                    .otherwise(f.col('dpc')))


# # Dataframe

# In[75]:


df = spark.read.option("header", "true").csv("address-linkage-key/address_link/data/test/*medium*.gz")
df.limit(10).toPandas()


# In[76]:


df = df.withColumn('dpc', f.lit(None))


# In[77]:


df = df.withColumn('housenumber', (f.regexp_extract(f.col('address_line_1'),'(^[0-9]([0-9A-Z.*-]+)?)', 1)))


# In[78]:


df.limit(10).toPandas()


# In[79]:


#rule1
df = df.withColumn('dpc', f.when((f.col('address_line_2').isNull()) &
(f.col('housenumber')).isNotNull() & f.col('housenumber').rlike('^[0-9]*$'),
(f.regexp_extract(f.col('housenumber'),'(\d+)',1)).substr(-2,2)
).otherwise(f.col('dpc')))
df.limit(10).toPandas()


# In[81]:


# rule 2
df = df.withColumn('dpc', 
                   f.when((f.col('dpc').isNull()) & (f.col('housenumber').isNull()), 
                          f.lit('99'))
                   .otherwise(f.col('dpc'))
                  )


# In[82]:


df.limit(50).toPandas()


# In[ ]:





# In[ ]:


df = df.withColumn('dpc', 
                    f.when(f.col('address_line_1').rlike(pattern), f.lit('99'))
                    .otherwise(f.col('dpc')))


# In[ ]:


df.limit(25).toPandas()


# In[ ]:


df.groupBy('dpc').count().show()


# In[ ]:


df.filter(df.dpc == '99').limit(20).toPandas()


# In[ ]:


def apply_rule_2(df):
    # Use 99 when address contains no house number
    df = df.withColumn('dpc',f.when((f.col('dpc').isNull()) & (f.col('housenumber').isNull()),f.lit('99'))
                       .otherwise(f.col('dpc')))
    return df

class TestRule2(SparkTestCase):
    def test(self):
        # Test Case: Rule 2 should use 99 when the address contains no house number.
        # run a query that gives me the input that the function expects and the expected value from that
        # parse operation
        testdf = self.spark.sql("SELECT 'MAIN ST' AS address_line_1, CAST(NULL AS string) AS housenumber, CAST(NULL AS string) AS dpc, '99' AS expected")
        #run the function
        testdf = apply_rule_2(testdf)
        #gather the results [row 0 only] and compare the returned dpc to the expected dpc
        row = testdf.collect()[0]
        self.assertEquals(row.expected, row.dpc, "rule 2 fail; expected does not match dpc")


# In[ ]:


class TestRule1(SparkTestCase):
    def test(self):
        # Test Case: Rule 1 (General Rule) should provide the last two digits of a primary street number, 
        # post office box, rural route box, or highway contract route number
        # run a query that gives me the input that the function expects and the expected value from that
        # parse operation
        testdf = self.spark.sql("SELECT '1234 MAIN ST' AS address_line_1, CAST(NULL AS string) AS dpc, '34' AS expected")
        #run the function
        testdf = apply_rule_1(testdf)
        #gather the results [row 0 only] and compare the returned dpc to the expected dpc
        row = testdf.collect()[0]
        self.assertEquals(row.expected, row.dpc, "rule 1 created match")

