# Databricks notebook source
# MAGIC %md
# MAGIC # Weather Alerts using Real-time Data Processing with Azure Databricks (and Event Hubs)
# MAGIC
# MAGIC This notebook demonstrates a real-time data processing workflow in Databricks using Structured Streaming to ingest data from Event Hubs. Designed a Bronze-Silver-Gold architecture to refine and transform data to find the weather conditions, with an automated email alert to notify stakeholders of the top results.
# MAGIC
# MAGIC - Data Sources: Streaming data from IoT devices or social media feeds. (Simulated in Event Hubs)
# MAGIC - Ingestion: Azure Event Hubs for capturing real-time data.
# MAGIC - Processing: Azure Databricks for stream processing using Structured Streaming.
# MAGIC - Storage: Processed data stored Azure Data Lake (Delta Format).
# MAGIC
# MAGIC ### Azure Services Required
# MAGIC - Databricks Workspace
# MAGIC - Azure Data Lake Storage
# MAGIC - Azure Event Hub
# MAGIC
# MAGIC ### Azure Databricks Configuration Required
# MAGIC - Single Node Compute Cluster: `12.2 LTS (includes Apache Spark 3.3.2, Scala 2.12)`
# MAGIC - Maven Library installed on Compute Cluster: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data:  
# MAGIC ###     {
# MAGIC ###     "temperature": 40,  
# MAGIC ###     "humidity": 20,  
# MAGIC ###     "windSpeed": 10,   
# MAGIC ###     "windDirection": "NW",  
# MAGIC ###     "precipitation": 0,  
# MAGIC ###     "conditions": "Partly Cloudy"  
# MAGIC ### }
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Importing the libraries.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC The code block below creates the catalog and schemas for our solution. 
# MAGIC
# MAGIC The approach utilises a multi-hop data storage architecture (medallion), consisting of bronze, silver, and gold schemas

# COMMAND ----------

try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.bronze;")
except:
    print('check if bronze schema already exists')

try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.silver;")
except:
    print('check if silver schema already exists')

try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metadata.gold;")
except:
    print('check if gold schema already exists')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC * Set up Azure Event hubs connection string.
# MAGIC * Defining JSON Schema
# MAGIC * Reading and writing the stream to Bronze Layer

# COMMAND ----------

# Config
# Replace with your Event Hub namespace, name, and key
connectionString = "Endpoint=sb://jack-namespace-demo.servicebus.windows.net/;SharedAccessKeyName=databricks;SharedAccessKey=########################################;EntityPath=eh_demo"


ehConf = {
  "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
}

# Define JSON schema
json_schema = StructType([
    StructField("temperature", IntegerType()),
    StructField("humidity", IntegerType()),
    StructField("windSpeed", IntegerType()),
    StructField("windDirection", StringType()),
    StructField("precipitation", IntegerType()),
    StructField("conditions", StringType())
])

# Read raw streaming data from Event Hubs
# No JSON parsing in Bronze layer, store raw data

df_bronze = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()

# Write to Bronze Table
df_bronze.writeStream \
    .option("checkpointLocation", "/mnt/checkpoints/bronze/weather") \
    .outputMode("append") \
    .format("delta") \
    .toTable("hive_metastore.bronze.weather")

df_bronze.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Checking whether data is stored in Bronze Weather Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.bronze.weather;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC * Reading the stream from the bronze 
# MAGIC * Categorizing weather conditions into 
# MAGIC    - Heatwave, Storm, Thunderstorm
# MAGIC * Writing the transformed stream into silver layer

# COMMAND ----------

# Read Bronze Data and Parse JSON in Silver Layer
df_silver = spark.readStream \
    .format("delta") \
    .table("hive_metastore.bronze.weather") \
    .withColumn("body", col("body").cast("string")) \
    .withColumn("body", from_json(col("body"), json_schema)) \
    .withColumn("eventId", expr("uuid()")) \
    .select("eventId", "body.*", col("enqueuedTime").alias("timestamp")) \
    .dropDuplicates(["eventId"])

# Categorizing weather conditions based on the data
df_silver = df_silver \
    .withColumn("wind_category", 
                when(col("windSpeed") < 5, "Low")
                .when(col("windSpeed") < 15, "Moderate")
                .otherwise("High")) \
    .withColumn("precipitation_category",
                when(col("precipitation") == 0, "None")
                .when(col("precipitation") < 5, "Light")
                .when(col("precipitation") < 20, "Moderate")
                .otherwise("Heavy")) \
    .withColumn("anomaly_type", 
                when(col("temperature") > 45, "Heatwave")
                .when(col("windSpeed") > 100, "Storm")
                .when((col("humidity") > 90) & (col("precipitation") > 10), "Thunderstorm")
                .otherwise("Normal"))


# Write to Silver Table
df_silver.writeStream \
    .option("checkpointLocation", "/mnt/checkpoints/silver/weather") \
    .outputMode("append") \
    .format("delta") \
    .toTable("hive_metastore.silver.weather")

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC Checking whether data is stored in Silver Weather Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.silver.weather;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold Layer

# COMMAND ----------

# MAGIC %md
# MAGIC * Reading, aggregating and writing the stream from the silver to the gold layer
# MAGIC * 5 minute Sliding Window 

# COMMAND ----------

# Aggregating Stream: Read from 'streaming.silver.weather', apply watermarking and windowing, and calculate average weather metrics
# **Sliding Window Aggregations (5-minute window, 5-minute slide)**
df_gold = df_silver \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes")) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
        avg("windSpeed").alias("avg_windSpeed"),
        avg("precipitation").alias("avg_precipitation"),
        sum(when(col("anomaly_type") != "Normal", 1).otherwise(0)).alias("anomaly_count")
  
    ) \
    .selectExpr("window.start as start_time", "window.end as end_time", "*")

# Write to Gold Table
df_gold.writeStream \
    .option("checkpointLocation", "/mnt/checkpoints/gold/weather_summary") \
    .outputMode("append") \
    .format("delta") \
    .toTable("hive_metastore.gold.weather_summary")

df_gold.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Checking whether data is stored in Gold Weather_Summary Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from hive_metastore.gold.weather_summary

# COMMAND ----------

# MAGIC %md
# MAGIC Email Notification is sent whenever there is a anomaly 

# COMMAND ----------

import smtplib
from email.mime.text import MIMEText

def send_email_alert(alert_data):
    sender_email = "***********"
    receiver_email = "**********"
    app_password = "tcxd kowt ppge eujr"  

    subject = "🚨 Weather Alert: Anomalies Detected!"
    body = f"Anomalies detected:\n\n{alert_data}"

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = receiver_email

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, app_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()
        print("✅ Alert email sent successfully!")
    except Exception as e:
        print(f"⚠️ Error sending email: {e}")

# Email Alerting System 
def log_anomalies(batch_df, batch_id):
    anomalies = batch_df.filter(col("anomaly_count") > 0)
    if anomalies.count() > 0:
        anomalies.select("start_time", "end_time", "anomaly_count").show(truncate=False)
        anomalies.write.format("delta").mode("append").saveAsTable("hive_metastore.gold.weather_alerts")

        # Convert DataFrame to string for email
        alert_text = anomalies.toPandas().to_string(index=False)
        send_email_alert(alert_text)
    

#Checking for new anomalies 
    if anomalies.count() > 0:
        # Check for new anomalies that haven't been processed
        existing_alerts = spark.table("hive_metastore.gold.weather_alerts")

        print("📌 Existing Alerts 📌")
        existing_alerts.show(truncate=False)

        new_anomalies = anomalies.alias("a").join(
            existing_alerts.alias("b"),
            on=["start_time", "end_time"],
            how="left_anti"
        )

        print("✅ New Anomalies ✅")
        new_anomalies.show(truncate=False)

        if new_anomalies.count() > 0:
            new_anomalies.write.format("delta").mode("complete").saveAsTable("hive_metastore.gold.weather_alerts")

            # Convert DataFrame to string for email
            alert_text = new_anomalies.toPandas().to_string(index=False)
            send_email_alert(alert_text)

df_gold.writeStream \
    .foreachBatch(log_anomalies) \
    .outputMode("update") \
    .start()

df_gold.display()
