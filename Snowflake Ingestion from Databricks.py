# Databricks notebook source
# DBTITLE 1,Set parameters
dbutils.widgets.text("filename","")
dbutils.widgets.text("extension","")
file_name=dbutils.widgets.get("filename")
extension=dbutils.widgets.get("extension")

print(file_name)
print(extension)

# COMMAND ----------

# DBTITLE 1,Set incoming path
root_path="/mnt/test_container/SNOWFLAKE/INCOMING/"
root_path2="/mnt/snowflake_container/INCOMING/"
csv_path2="/mnt/test_container/MASTER/INBOUND/master_20230706.csv"

csv_path=root_path+file_name
print(csv_path)

# COMMAND ----------

# DBTITLE 1,Get files in container
import os

file_paths=[]
file_paths2=[]

# Check if system name is in files and append those to a list
for path,subdirs,files in os.walk("/dbfs"+root_path):
    for name in files:
        if (file_name in name) & (extension in name):            
            file_paths.append(os.path.join(path,name))

for fi in file_paths:
    file_paths2.append(fi.replace("/dbfs", ""))

print(file_paths2)

# COMMAND ----------

# DBTITLE 1,Load data into dataframe
try:
    df_csv = spark.read\
        .option("header", "false")\
        .option("delimiter","|")\
        .format("csv")\
        .load(file_paths2)

    display(df_csv)

except Exception as err:
    print(err)
    pass
    #logging_entries.append(str(getMYTtime())+" Error reading CSV:"+err)
    #logging_entry()

# COMMAND ----------

# DBTITLE 1,Cleaning Data
from pyspark.sql.functions import *

df_csv = df_csv.withColumn('_c8',regexp_replace('_c8','GPRS=','').cast("Boolean"))\
    .withColumn('_c9',regexp_replace('_c9','EDGE=','').cast("Boolean"))\
    .withColumn('_c10',regexp_replace('_c10','UMTS=','').cast("Boolean"))\
    .withColumn('_c11',regexp_replace('_c11','HSDPA=','').cast("Boolean"))\
    .withColumn('_c12',regexp_replace('_c12','HSUPA=','').cast("Boolean"))\
    .withColumn('_c13',regexp_replace('_c13','GSM850=','').cast("Boolean"))\
    .withColumn('_c14',regexp_replace('_c14','GSM900=','').cast("Boolean"))\
    .withColumn('_c15',regexp_replace('_c15','GSM1800=','').cast("Boolean"))\
    .withColumn('_c16',regexp_replace('_c16','GSM1900=','').cast("Boolean"))\
    .withColumn('_c17',regexp_replace('_c17','WCDMA2100=','').cast("Boolean"))\
    .withColumn('_c18',regexp_replace('_c18','WCDMA1900=','').cast("Boolean"))\
    .withColumn('_c19',regexp_replace('_c19','WCDMA1800=','').cast("Boolean"))\
    .withColumn('_c20',regexp_replace('_c20','WCDMA1700=','').cast("Boolean"))\
    .withColumn('_c21',regexp_replace('_c21','WCDMA850=','').cast("Boolean"))\
    .withColumn('_c22',regexp_replace('_c22','WCDMA2600=','').cast("Boolean"))\
    .withColumn('_c23',regexp_replace('_c23','WCDMA900=','').cast("Boolean"))\
    .withColumn('_c24',regexp_replace('_c24','LTE=','').cast("Boolean"))\
    

display(df_csv)

# COMMAND ----------

# DBTITLE 1,Import Schema from MASTER_TABLE_DICTIONARY
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

df_tbl_dic = pd.read_csv("/dbfs/mnt/test_container/MASTER/MASTER_TABLE_DICTIONARY.txt", sep="|", header=None)
column_count=len(df_tbl_dic.columns)

df=df_tbl_dic.transpose()
df.iloc[:,0] = df.iloc[:,0].str.replace('MASTER_TABLE=','')

df=df.iloc[:,0].str.split(':',expand=True)

df.iloc[:,2] = df.iloc[:,2].str.replace('Y','True')
df.iloc[:,2] = df.iloc[:,2].str.replace('N','False')

df = "StructField('" + df.iloc[:,0]+"',"+df.iloc[:,1]+","+df.iloc[:,2]+"),"

#print(df.to_string())

x = df.to_string(header=False,index=False).split('\n')
vals = [' '.join(ele.split()) for ele in x]

# initialize an empty string
str1 = ""
# traverse in the string
for ele in vals:
    str1 += ele
str1="StructType(["+str1[:-1]+"])"
str1=str1.replace('String','StringType()')
str1=str1.replace('Boolean','BooleanType()')
str1.lstrip(' ')
#print(str1)
df_schema=eval(str1)


# COMMAND ----------

# DBTITLE 1,Enforce schema
df_csv = spark.createDataFrame(df_csv.rdd,df_schema)
df_csv.printSchema
display(df_csv)

# COMMAND ----------

# DBTITLE 1,Add load date
from pyspark.sql import functions as F
from datetime import datetime,timedelta
import pytz

df_mod=df_csv.withColumn("Load_date",F.lit(datetime.now(pytz.timezone('Asia/Kuala_Lumpur')).strftime('%Y-%m-%d')).cast('Date'))

display(df_mod)

# COMMAND ----------

# DBTITLE 1,Set Snowflake configuration
# Use dbutils secrets to get Snowflake credentials.
username = dbutils.secrets.get(scope="Az-KeyVault-Secreet", key="PRDSnowflake-UName")
password = dbutils.secrets.get(scope="Az-KeyVault-Secreet", key="PRDSnowflake-Pwd")
 
options = {
  "sfUrl": "xxxxxx.southeast-asia.privatelink.snowflakecomputing.com",
  "sfUser": username,
  "sfPassword": password,
  "sfWarehouse": "COMPUTE_WH",
  "truncate_table" : "ON",
  "usestagingtable" : "OFF",
  "continue_on_error": "OFF"
}

options2 = dict(
  sfUrl= "xxxxxx.southeast-asia.privatelink.snowflakecomputing.com",
  sfUser= username,
  sfPassword= password,
  sfDatabase= "SOR",
  sfSchema= "MASTER",
  sfWarehouse= "COMPUTE_WH")
  
sfUtils = sc._jvm.net.snowflake.spark.snowflake.Utils

# COMMAND ----------

# DBTITLE 1,Load(Overwrite) data to snowflake - MASTER_TMP
# Write the dataset to Snowflake.
try:
    df_mod.write \
        .format("snowflake") \
        .options(**options) \
        .option("sfDatabase","SOR") \
        .option("sfSchema","TEMPSOR") \
        .option("dbtable", "MASTER_TMP") \
        .mode("Overwrite")\
        .save()

    print("Row count of SOR.TEMPSOR.MASTER_TMP : "+str(df_mod.count()))
    
except Exception as err:
    print(err)


# COMMAND ----------

# DBTITLE 1,Load(Append) data to snowflake - MASTER_HIST
# Write the dataset to Snowflake.
try:
    df_mod.write \
        .format("snowflake") \
        .options(**options) \
        .option("sfDatabase","SOR") \
        .option("sfSchema","MASTER") \
        .option("dbtable", "MASTER_HIST") \
        .mode("Append")\
        .save()

    print("Row count of SOR.TEMPSOR.MASTER_HIST : "+str(df_mod.count()))

except Exception as err:
    print(err)
    #logging_entries.append(str(getMYTtime())+" Error writing to snowflake:"+err)
    #logging_entry()

# COMMAND ----------

# DBTITLE 1,Read table from Snowflake - MASTER_TMP
try:
    # Read the data written by the previous cell back.
    snowflake_df = spark.read \
        .format("snowflake") \
        .options(**options) \
        .option("sfDatabase","SOR") \
        .option("sfSchema","TEMPSOR") \
        .option("query", """SELECT * FROM SOR.TEMPSOR.MASTER_TMP""") \
        .load()
 
    print("Row count of SOR.TEMPSOR.MASTER_TMP : "+str(snowflake_df.count()))
except Exception as err:
    print(err)

# COMMAND ----------

# DBTITLE 1,Read table from Snowflake - MASTER_HIST
try:
    # Read the data written by the previous cell back.
    snowflake_df = spark.read \
        .format("snowflake") \
        .options(**options) \
        .option("sfDatabase","SOR") \
        .option("sfSchema","MASTER") \
        .option("query", """SELECT * FROM SOR.MASTER_HIST""") \
        .load()
 
    print("Row count of SOR.MASTER_HIST : "+str(snowflake_df.count()))
except Exception as err:
    print(err)

# COMMAND ----------

# DBTITLE 1,Execute MERGE Query
# Open and read the file as a single buffer
fd = open('/dbfs/mnt/config_container/adms/Snowflake_Merge_Query.sql', 'r')
sqlFile = fd.read()
fd.close()

# all SQL commands (split on ';')
sqlCommands = sqlFile.split(';')
commands=[]
# Execute every command from the input file
for command in sqlCommands:
    commands.append(command)

#print(commands[0])

#Run query in Snowflake
queryObj = sfUtils.runQuery(options2,commands[0])
if queryObj:
    print("Success: MERGE Query, "+str(queryObj))

# COMMAND ----------

# DBTITLE 1,Resultset of Query_History - Take Latest MERGE QueryID
try:
    # Read the data written by the previous cell back.
    Query_History_df = spark.read \
        .format("snowflake") \
        .options(**options) \
        .option("sfDatabase","SOR") \
        .option("sfSchema","MASTER") \
        .option("query", "select QUERY_ID from table(information_schema.query_history()) where QUERY_TEXT like 'MERGE%' order by start_time desc limit 1;") \
        .load()
    #display(Query_History_df)
    MERGE_QUERY_ID=Query_History_df.collect()[0][0]
    print("MERGE_QUERY_ID : "+str(MERGE_QUERY_ID))
except Exception as err:
    print(err)

# COMMAND ----------

Query="SELECT * FROM TABLE(RESULT_SCAN('"+MERGE_QUERY_ID+"'));"

try:
    # Read the data written by the previous cell back.
    Rows_Affected_df = spark.read \
        .format("snowflake") \
        .options(**options) \
        .option("sfDatabase","SOR") \
        .option("sfSchema","MASTER") \
        .option("query", Query) \
        .load()
    #display(Rows_Affected_df)
    number_of_rows_inserted=Rows_Affected_df.collect()[0][0]
    number_of_rows_updated=Rows_Affected_df.collect()[0][1]
    print("Number_of_rows_inserted : "+str(number_of_rows_inserted))
    print("Number_of_rows_updated : "+str(number_of_rows_updated))
except Exception as err:
    print(err)

# COMMAND ----------


