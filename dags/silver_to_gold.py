from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum as spark_sum, when, round, current_timestamp
from pyspark.sql.types import FloatType, IntegerType
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_PATH = '/tmp/data_lake'

def silver_to_gold():
    silver_path = f'{BASE_PATH}/silver'
    gold_path = f'{BASE_PATH}/gold'
    
    logger.info('Starting silver to gold process')
    
    # Use local mode to avoid distributed file system access issues
    spark = SparkSession.builder \
        .appName('Silver_to_Gold') \
        .master('local[*]') \
        .config('spark.sql.adaptive.enabled', 'true') \
        .getOrCreate()
    
    try:
        bio_path = os.path.join(silver_path, 'athlete_bio')
        athlete_bio_df = spark.read.parquet(bio_path)
        
        results_path = os.path.join(silver_path, 'athlete_event_results')
        athlete_results_df = spark.read.parquet(results_path)
        
        logger.info('Bio columns: ' + str(athlete_bio_df.columns))
        logger.info('Results columns: ' + str(athlete_results_df.columns))
        
        # Convert numeric columns
        if 'height' in athlete_bio_df.columns:
            athlete_bio_df = athlete_bio_df.withColumn('height', col('height').cast(FloatType()))
        
        if 'weight' in athlete_bio_df.columns:
            athlete_bio_df = athlete_bio_df.withColumn('weight', col('weight').cast(FloatType()))
        
        # Calculate age from born year if available
        if 'born' in athlete_bio_df.columns:
            athlete_bio_df = athlete_bio_df.withColumn('age', 
                                                     when(col('born').isNotNull() & (col('born').cast(IntegerType()) > 1800), 
                                                          2024 - col('born').cast(IntegerType())).otherwise(None))
        
        # Join and select specific columns to avoid conflicts
        joined_df = athlete_bio_df.join(
            athlete_results_df, 
            athlete_bio_df.name == athlete_results_df.athlete, 
            'inner'
        ).select(
            # From bio table
            athlete_bio_df.athlete_id.alias('bio_athlete_id'),
            athlete_bio_df.name,
            athlete_bio_df.sex,
            athlete_bio_df.born,
            athlete_bio_df.height,
            athlete_bio_df.weight,
            athlete_bio_df.country,
            athlete_bio_df.country_noc.alias('bio_country_noc'),
            col('age'),
            # From results table  
            athlete_results_df.athlete_id.alias('results_athlete_id'),
            athlete_results_df.edition,
            athlete_results_df.sport,
            athlete_results_df.event,
            athlete_results_df.medal,
            athlete_results_df.pos,
            athlete_results_df.country_noc.alias('results_country_noc')
        )
        
        # Calculate BMI
        if 'height' in joined_df.columns and 'weight' in joined_df.columns:
            joined_df = joined_df.withColumn(
                'bmi',
                when((col('height') > 0) & (col('weight') > 0),
                     round(col('weight') / ((col('height') / 100) * (col('height') / 100)), 2))
                .otherwise(None)
            )
        
        # Medal indicators
        if 'medal' in joined_df.columns:
            joined_df = joined_df.withColumn(
                'has_medal',
                when((col('medal').isNotNull()) & (col('medal') != 'NA') & (col('medal') != ''), 1).otherwise(0)
            )
        
        # Sport aggregations
        agg_exprs = [
            count('*').alias('participant_count'),
            avg('height').alias('avg_height'),
            avg('weight').alias('avg_weight'),
            spark_sum('has_medal').alias('medal_count')
        ]
        
        # Add age aggregation only if age column exists
        if 'age' in joined_df.columns:
            agg_exprs.append(avg('age').alias('avg_age'))
        
        sport_stats = joined_df.groupBy('sport', 'sex').agg(*agg_exprs).withColumn('processing_timestamp', current_timestamp())
        
        
        # Display final DataFrames for this step
        print("=== Individual Athlete Features DataFrame (joined_df) ===")
        joined_df.show(15, truncate=False)
        print(f"Total individual records: {joined_df.count()}")

        os.makedirs(gold_path, exist_ok=True)
        
        # Write results
        individual_output = os.path.join(gold_path, 'athlete_features')
        logger.info(f'Writing individual features to {individual_output}')
        joined_df.write.mode('overwrite').parquet(individual_output)
        
        sport_output = os.path.join(gold_path, 'sport_statistics')
        logger.info(f'Writing sport statistics to {sport_output}')
        
        # Display sport statistics DataFrame
        print("=== Sport Statistics DataFrame (sport_stats) ===")
        sport_stats.show(15, truncate=False)
        print(f"Total sport statistics records: {sport_stats.count()}")

        sport_stats.write.mode('overwrite').parquet(sport_output)
        
        individual_count = joined_df.count()
        sport_count = sport_stats.count()
        
        logger.info(f'Successfully created gold layer at {gold_path}')
        logger.info(f'Individual features: {individual_count} records')
        logger.info(f'Sport statistics: {sport_count} records')
        
        # Show sample data
        logger.info('Sample sport statistics:')
        sport_stats.show(5)
        
    except Exception as e:
        logger.error(f'Error in silver to gold process: {str(e)}')
        raise
    finally:
        spark.stop()

if __name__ == '__main__':
    silver_to_gold()