from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
import os

# Налаштування для використання Kafka в Spark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# Параметри підключення до Kafka
kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],
    "username": "admin", 
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

# Створення сесії Spark для обробки даних
spark = SparkSession.builder.appName("KafkaTableDisplay").master("local[*]").getOrCreate()

print("🚀 Starting Kafka table display...")

# Опис структури даних, що надходять у форматі JSON
schema = StructType(
    [
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("country_noc", StringType(), True),
        StructField("avg_height", DoubleType(), True),
        StructField("avg_weight", DoubleType(), True), 
        StructField("timestamp", StringType(), True),
    ]
)

try:
    # Читання потокових даних із Kafka
    kafka_streaming_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";',
        )
        .option("subscribe", "rudenko_enriched_athlete_avg")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "50")
        .option("failOnDataLoss", "false")
        .load()
    )
    
    # Парсинг JSON даних з Kafka
    parsed_df = (
        kafka_streaming_df
        .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
        .select(from_json(col("json_value"), schema).alias("data"), "kafka_timestamp")
        .select("data.*", "kafka_timestamp")
        .filter(col("sport").isNotNull())  # Фільтр валідних записів
    )
    
    def process_batch(batch_df, batch_id):
        print(f"\n" + "="*120)
        print(f"📊 Batch: {batch_id}")
        print("="*120)
        
        if batch_df.count() > 0:
            # Відображення даних в табличному форматі
            batch_df.select(
                col("sport"),
                col("medal"),
                col("sex"), 
                col("country_noc"),
                format_number(col("avg_height"), 1).alias("avg_height"),
                format_number(col("avg_weight"), 1).alias("avg_weight"),
                date_format(col("kafka_timestamp"), "yyyy-MM-dd HH:mm:ss").alias("timestamp")
            ).show(50, truncate=False)
            
            print(f"📈 Records in batch: {batch_df.count()}")
        else:
            print("⚠️  Empty batch - no new data")
    
    # Виведення результатів потоку з обробкою батчів
    query = (
        parsed_df
        .writeStream
        .foreachBatch(process_batch)
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    print("📡 Monitoring Kafka topic: rudenko_enriched_athlete_avg")
    print("💡 Press Ctrl+C to stop...")
    
    query.awaitTermination(120)  # Run for 2 minutes
    
except KeyboardInterrupt:
    print("\n⏹️ Stopping...")
    if 'query' in locals():
        query.stop()
        
except Exception as e:
    print(f"❌ Error: {str(e)}")
    import traceback
    traceback.print_exc()
    
finally:
    spark.stop()
    print("✅ Completed!")