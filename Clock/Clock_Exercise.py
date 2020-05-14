# -*- coding: utf-8 -*-
"""
Created on Tue May 12 13:43:50 2020

@author: Prashant
"""


from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField,StringType
from pyspark.sql.functions import col,split,expr
import sys




spark = SparkSession.builder.appName("angle between the hands on a clock").getOrCreate()

#location for file to read
file_input_path = sys.argv[1]
file_output_path = sys.argv[2]
print ("The File Path is " + sys.argv[1])

#Creating schema for the File. ( Expecting only one column with "Time" Value 3:20) 
base_schema = StructType([ StructField("Time",StringType(),True)
                    ])

#Reading data from File
raw_data = spark.read.schema(base_schema).format("csv").option("header","true")\
          .option("inferSchema", "false").load(file_input_path)\


# =============================================================================
# Logic for Calculating Angle is This -: 
#A clock is a circle, and a circle always contains 360 degrees. Since there are 60 minutes on a clock, each minute mark is 6 degrees.
# 
# 360o total60 minutes total=6 degrees per minute
# 
# The minute hand on the clock will point at 15 minutes, allowing us to calculate it's position on the circle.
# 
# (15 min)(6)=90o
# 
# Since there are 12 hours on the clock, each hour mark is 30 degrees.
# 
# 360o total 12 hours total=30 degrees per hour
#However, the hour hand will actually be between the 8 and the 9, 
#since we are looking at 8:15 rather than an absolute hour mark. 15 minutes is
#equal to one-fourth of an hour. Use the same equation to find the additional position of the hour hand.
# =============================================================================

#Calculating angle 
angle_Cal = raw_data.select("Time",split(col("Time"), ":").alias("array_col"))\
   .selectExpr("Time","array_col[0]","array_col[1]").withColumnRenamed("array_col[0]","Hour")\
       .withColumnRenamed("array_col[1]","Min")\
           .select("Time",expr("((Hour * 30)+((Min/60)*30)) - (Min * 6)").alias("Angle"))

#writting data into file
angle_Cal.write.format("csv").mode("overwrite").option("sep", "\t").option("header","true")\
.save(file_output_path)   #"D:/PRASHANT/Study/Spark/Dataflair/Codes/Clock/clock_data_output.txt"


