from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import requests
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_PATH = '/tmp/data_lake'

def download_data(local_file_path):
    url = 'https://ftp.goit.study/neoversity/'
    downloading_url = url + os.path.basename(local_file_path) + '.csv'
    print(f'Downloading from {downloading_url}')
    response = requests.get(downloading_url)

    if response.status_code == 200:
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        with open(local_file_path + '.csv', 'wb') as file:
            file.write(response.content)
        print(f'File downloaded successfully and saved as {local_file_path}.csv')
    else:
        exit(f'Failed to download the file. Status code: {response.status_code}')

def landing_to_bronze(table_name):
    landing_path = f'{BASE_PATH}/landing'
    bronze_path = f'{BASE_PATH}/bronze'
    
    logger.info(f'Starting landing to bronze process for {table_name}')
    
    # Use local mode to avoid distributed file system access issues
    spark = SparkSession.builder \
        .appName(f'Landing_to_Bronze_{table_name}') \
        .master('local[*]') \
        .config('spark.sql.adaptive.enabled', 'true') \
        .getOrCreate()
    
    try:
        os.makedirs(landing_path, exist_ok=True)
        os.makedirs(bronze_path, exist_ok=True)
        
        csv_file_path = os.path.join(landing_path, table_name)
        download_data(csv_file_path)
        
        # Verify file exists before reading
        csv_full_path = f'{csv_file_path}.csv'
        if not os.path.exists(csv_full_path):
            raise FileNotFoundError(f'CSV file not found: {csv_full_path}')
        
        logger.info(f'Reading CSV file: {csv_full_path}')
        df = spark.read.option('header', 'true').option('inferSchema', 'true').option('multiLine', 'true').option('escape', '"').csv(csv_full_path)
        
        df = df.withColumn('ingestion_timestamp', current_timestamp()).withColumn('source_file', lit(f'{table_name}.csv'))
        
        # Display final DataFrame for this step
        print(f"=== Final DataFrame for {table_name} Landing to Bronze ===")
        df.show(15, truncate=False)
        print(f"Total records: {df.count()}")

        
        output_path = os.path.join(bronze_path, table_name)
        
        df.write.mode('overwrite').option('compression', 'snappy').parquet(output_path)
        
        record_count = df.count()
        logger.info(f'Successfully wrote {table_name} to bronze layer at {output_path}')
        logger.info(f'Record count: {record_count}')
        
    except Exception as e:
        logger.error(f'Error processing {table_name}: {str(e)}')
        raise
    finally:
        spark.stop()

if __name__ == '__main__':
    tables = ['athlete_bio', 'athlete_event_results']
    for table in tables:
        landing_to_bronze(table)