"""
Step 3: Read athlete_event_results from MySQL and write to Kafka topic
"""
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import os
from config import get_kafka_config, get_jdbc_config, get_kafka_topics, SPARK_MASTER, SPARK_APP_NAME

# Configure Spark to use Kafka packages
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# Load configuration from environment variables
kafka_config = get_kafka_config()
jdbc_config = get_jdbc_config()
topics_config = get_kafka_topics()

print("🚀 Initializing Spark Session...")

# Initialize Spark session with MySQL connector
spark = (
    SparkSession.builder
    .config("spark.jars", "/opt/airflow/mysql-connector-j-8.0.32.jar")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .appName(SPARK_APP_NAME)
    .master(SPARK_MASTER)
    .getOrCreate()
)

print(f"✅ Spark version: {spark.version}")

try:
    print("📖 Reading athlete_event_results from MySQL...")
    
    # Step 3: Read athlete_event_results from MySQL
    # Try to load more data - remove partitioning constraints
    jdbc_df = (
        spark.read.format("jdbc")
        .options(
            url=jdbc_config["url"],
            driver=jdbc_config["driver"],
            dbtable="(SELECT * FROM athlete_event_results LIMIT 10000) as subset",  # Load first 10000 records
            user=jdbc_config["user"],
            password=jdbc_config["password"],
        )
        .load()
    )
    
    print(f"📊 Total records read from MySQL: {jdbc_df.count()}")
    print("📋 Schema of athlete_event_results:")
    jdbc_df.printSchema()
    
    print("🔍 Sample data:")
    jdbc_df.show(5)
    
    print("📤 Writing data to Kafka topic...")
    
    # Step 3: Write to Kafka topic
    kafka_write_result = (
        jdbc_df.selectExpr(
            "CAST(result_id AS STRING) AS key",
            "to_json(struct(*)) AS value",
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"])
        .option("topic", topics_config["athlete_event_results"])
        .save()
    )
    
    print(f"✅ Successfully wrote athlete_event_results data to Kafka topic: {topics_config['athlete_event_results']}")
    print(f"📊 Records written: {jdbc_df.count()}")
    
except Exception as e:
    print(f"❌ Error occurred: {str(e)}")
    import traceback
    traceback.print_exc()
    
finally:
    print("🔄 Stopping Spark session...")
    spark.stop()
    print("✅ Pipeline completed!")