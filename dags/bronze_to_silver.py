from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, regexp_replace
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_PATH = '/tmp/data_lake'

def bronze_to_silver(table_name):
    bronze_path = f'{BASE_PATH}/bronze'
    silver_path = f'{BASE_PATH}/silver'
    
    logger.info(f'Starting bronze to silver process for {table_name}')
    
    # Use local mode to avoid distributed file system access issues
    spark = SparkSession.builder \
        .appName(f'Bronze_to_Silver_{table_name}') \
        .master('local[*]') \
        .config('spark.sql.adaptive.enabled', 'true') \
        .getOrCreate()
    
    try:
        input_path = os.path.join(bronze_path, table_name)
        df = spark.read.parquet(input_path)
        
        # Clean text columns using built-in Spark functions instead of UDF
        string_columns = [field.name for field in df.schema.fields 
                         if field.dataType.simpleString() == 'string'
                         and field.name not in ['ingestion_timestamp', 'source_file']]
        
        for col_name in string_columns:
            # Remove special characters (keep only letters, numbers, spaces, commas, quotes, periods, hyphens)
            df = df.withColumn(col_name, regexp_replace(col(col_name), '[^a-zA-Z0-9,."\'\s-]', ''))
            # Replace empty strings with null
            df = df.withColumn(col_name, 
                             when(trim(col(col_name)) == '', None)
                             .otherwise(trim(col(col_name))))
        
        # Clean numeric columns
        if 'height' in df.columns:
            df = df.withColumn('height', regexp_replace(col('height'), '[^0-9.]', ''))
        
        if 'weight' in df.columns:
            df = df.withColumn('weight', regexp_replace(col('weight'), '[^0-9.]', ''))
        
        if 'age' in df.columns:
            df = df.withColumn('age', regexp_replace(col('age'), '[^0-9]', ''))
        
        # Remove duplicates
        df_deduped = df.dropDuplicates()
        
        
        # Display final DataFrame for this step
        print(f"=== Final DataFrame for {table_name} Bronze to Silver ===")
        df_deduped.show(15, truncate=False)
        print(f"Total records after deduplication: {df_deduped.count()}")

        os.makedirs(silver_path, exist_ok=True)
        output_path = os.path.join(silver_path, table_name)
        
        df_deduped.write.mode('overwrite').option('compression', 'snappy').parquet(output_path)
        
        logger.info(f'Successfully wrote {table_name} to silver layer at {output_path}')
        logger.info(f'Record count: {df_deduped.count()}')
        
    except Exception as e:
        logger.error(f'Error processing {table_name}: {str(e)}')
        raise
    finally:
        spark.stop()

if __name__ == '__main__':
    tables = ['athlete_bio', 'athlete_event_results']
    for table in tables:
        bronze_to_silver(table)