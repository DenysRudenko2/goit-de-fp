"""
Complete Streaming Pipeline Implementation - FIXED VERSION
"""
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DoubleType
import os
from config import get_kafka_config, get_jdbc_config, get_kafka_topics, SPARK_MASTER, SPARK_APP_NAME

# Configure Spark packages for Kafka and MySQL
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# Load configuration from environment variables
kafka_config = get_kafka_config()
jdbc_read_config = get_jdbc_config()  # For reading from olympic_dataset
topics_config = get_kafka_topics()

# JDBC configuration for WRITING to neo_data (using same connection but different database)
jdbc_write_config = get_jdbc_config().copy()
jdbc_write_config["url"] = jdbc_write_config["url"].replace("olympic_dataset", "neo_data")

print("🚀 Initializing Spark Streaming Session...")

# Initialize Spark session
spark = (
    SparkSession.builder
    .config("spark.jars", "/opt/airflow/mysql-connector-j-8.0.32.jar")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/streaming_checkpoint")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") 
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .appName("Olympic_Data_Streaming_Pipeline_Fixed")
    .master("local[*]")
    .getOrCreate()
)

print(f"✅ Spark version: {spark.version}")

# Define schema for Kafka message (athlete_event_results)
kafka_schema = StructType([
    StructField("result_id", IntegerType(), True),
    StructField("athlete_id", IntegerType(), True), 
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("country_noc", StringType(), True),
    StructField("edition", StringType(), True),
])

try:
    print("📖 Step 1: Reading athlete_bio data from olympic_dataset database...")
    
    # Step 1 & 2: Read athlete_bio from olympic_dataset (NOT neo_data!)
    athlete_bio_df = (
        spark.read.format("jdbc")
        .options(
            url=jdbc_read_config["url"],  # Use read config
            driver=jdbc_read_config["driver"],
            dbtable="athlete_bio",
            user=jdbc_read_config["user"],
            password=jdbc_read_config["password"],
            partitionColumn="athlete_id",
            lowerBound=1,
            upperBound=200000,
            numPartitions="10",
        )
        .load()
    )
    
    print("🔍 Step 2: Filtering athletes with valid height and weight data...")
    # Step 2: Filter data where height and weight are not empty and are numeric
    athlete_bio_filtered = athlete_bio_df.filter(
        (col("height").isNotNull()) &
        (col("weight").isNotNull()) &
        (col("height").cast("double").isNotNull()) &
        (col("weight").cast("double").isNotNull()) &
        (col("height") > 0) &
        (col("weight") > 0)
    )
    
    print(f"📊 Original athlete_bio records: {athlete_bio_df.count()}")
    print(f"📊 Filtered athlete_bio records: {athlete_bio_filtered.count()}")
    
    print("📋 athlete_bio schema:")
    athlete_bio_filtered.printSchema()
    
    print("🔍 Sample athlete_bio data:")
    athlete_bio_filtered.show(5)
    
    print("📡 Step 4: Setting up Kafka streaming to read athlete_event_results...")
    
    # Step 4: Read from Kafka topic and convert JSON to DataFrame
    kafka_streaming_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"])
        .option("subscribe", topics_config["athlete_event_results"])
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "100")  # Process in batches
        .option("failOnDataLoss", "false")
        .load()
    )
    
    # Parse JSON data from Kafka - Updated schema to match actual data
    full_schema = StructType([
        StructField("edition", StringType(), True),
        StructField("edition_id", IntegerType(), True),
        StructField("country_noc", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("event", StringType(), True),
        StructField("result_id", IntegerType(), True),
        StructField("athlete", StringType(), True),
        StructField("athlete_id", IntegerType(), True),
        StructField("pos", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("isTeamSport", StringType(), True),
    ])
    
    parsed_kafka_df = (
        kafka_streaming_df
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), full_schema).alias("data"))
        .select("data.*")
        .filter(col("athlete_id").isNotNull())
    )
    
    print("🤝 Step 4: Joining Kafka stream with athlete_bio data...")
    
    # Step 4: Join Kafka stream data with athlete_bio using athlete_id
    # Select specific columns from each DataFrame to avoid ambiguity
    kafka_selected = parsed_kafka_df.select(
        col("athlete_id"),
        col("sport"),
        col("medal"),
        col("country_noc").alias("event_country")  # Rename to avoid conflict
    )
    
    bio_selected = athlete_bio_filtered.select(
        col("athlete_id"),
        col("sex"),
        col("height"),
        col("weight"),
        col("country_noc")  # Keep this as the main country
    )
    
    joined_streaming_df = kafka_selected.join(
        bio_selected,
        "athlete_id",
        "inner"
    )
    
    print("📊 Step 5: Creating aggregations...")
    
    # Step 5: Calculate average height and weight by sport, medal, sex, country_noc + timestamp
    aggregated_streaming_df = (
        joined_streaming_df
        .groupBy("sport", "medal", "sex", "country_noc")  # Now unambiguous
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight"), 
            current_timestamp().alias("timestamp")
        )
        .withColumn("avg_height", round(col("avg_height"), 4))
        .withColumn("avg_weight", round(col("avg_weight"), 4))
    )
    
    print("📤 Step 6: Setting up forEachBatch to write to Kafka and MySQL...")
    
    # Step 6: Function for forEachBatch to write to both Kafka and MySQL
    def foreach_batch_function(batch_df, epoch_id):
        print(f"🔄 Processing batch {epoch_id}")
        
        if batch_df.count() > 0:
            print(f"📊 Batch {epoch_id} contains {batch_df.count()} records")
            
            batch_df.show(10, False)
            
            try:
                # Step 6a: Write to output Kafka topic
                print(f"📤 Step 6a: Writing batch {epoch_id} to Kafka topic...")
                (
                    batch_df
                    .selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
                    .write
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
                    .option("kafka.security.protocol", kafka_config["security_protocol"])
                    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
                    .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"])
                    .option("topic", topics_config["enriched_athlete_avg"])
                    .save()
                )
                print(f"✅ Step 6a: Successfully wrote batch {epoch_id} to Kafka")
                
                # Step 6b: Write to MySQL database (neo_data)
                print(f"💾 Step 6b: Writing batch {epoch_id} to MySQL neo_data database...")
                (
                    batch_df
                    .write
                    .format("jdbc")
                    .options(
                        url=jdbc_write_config["url"],  # Use write config for neo_data
                        driver=jdbc_write_config["driver"],
                        dbtable=topics_config["enriched_athlete_avg"],
                        user=jdbc_write_config["user"],
                        password=jdbc_write_config["password"],
                    )
                    .mode("append")
                    .save()
                )
                print(f"✅ Step 6b: Successfully wrote batch {epoch_id} to MySQL neo_data.{topics_config['enriched_athlete_avg']}")
                
            except Exception as e:
                print(f"❌ Error in batch {epoch_id}: {str(e)}")
                import traceback
                traceback.print_exc()
        else:
            print(f"ℹ️  Batch {epoch_id} is empty, skipping...")
    
    print("🎯 Starting streaming processing...")
    
    # Step 6: Start the streaming query with forEachBatch
    streaming_query = (
        aggregated_streaming_df
        .writeStream
        .outputMode("complete")
        .foreachBatch(foreach_batch_function)
        .option("checkpointLocation", "/tmp/streaming_checkpoint")
        .trigger(processingTime="30 seconds")  # Process every 30 seconds
        .start()
    )
    
    print("🚀 Streaming pipeline is running...")
    print("📊 Monitoring for data...")
    print("💡 Press Ctrl+C to stop the pipeline")
    
    # Wait for the streaming to finish (or until interrupted)
    streaming_query.awaitTermination()
    
except KeyboardInterrupt:
    print("\n⏹️  Stopping pipeline...")
    if 'streaming_query' in locals():
        streaming_query.stop()
    print("✅ Pipeline stopped by user")
    
except Exception as e:
    print(f"❌ Error occurred: {str(e)}")
    import traceback
    traceback.print_exc()
    
finally:
    print("🔄 Stopping Spark session...")
    spark.stop()
    print("✅ Streaming pipeline completed!")