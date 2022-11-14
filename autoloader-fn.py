# Databricks notebook source
dbutils.fs.ls("s3a://databricks-seungdon-bkt1/loader")

# COMMAND ----------

query = (spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format','json')
        .option('inferSchema','True')
        .option('cloudFiles.schemaLocation','/tmp/autoloader/schema') 
        .option('cloudFiles.useNotifications','True')
        .load("s3a://databricks-seungdon-bkt1/loader")
    
        .writeStream.format('delta')
                .option('checkpointLocation','/tmp/autoloader/_checkpoint') 
        .trigger(processingTime='10 seconds')
         .start('/tmp/sample-table')
       
        )

# COMMAND ----------

display(query)

# COMMAND ----------

# MAGIC %fs ls /tmp/sample-table

# COMMAND ----------


