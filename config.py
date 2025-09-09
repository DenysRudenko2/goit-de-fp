import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_required_env(var_name: str) -> str:
    """Get required environment variable or raise error if not set"""
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"❌ Environment variable '{var_name}' is required but not set. Please check your .env file.")
    return value

def get_optional_env(var_name: str, default_value: str) -> str:
    """Get optional environment variable with default value"""
    return os.getenv(var_name, default_value)

# User Configuration
USER_NAME = get_required_env("USER_NAME")

# Kafka Configuration
def get_kafka_config():
    username = get_required_env("KAFKA_USERNAME")
    password = get_required_env("KAFKA_PASSWORD")
    
    return {
        "bootstrap_servers": get_required_env("KAFKA_BOOTSTRAP_SERVERS"),
        "username": username,
        "password": password,
        "security_protocol": get_optional_env("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
        "sasl_mechanism": get_optional_env("KAFKA_SASL_MECHANISM", "PLAIN"),
        "sasl_jaas_config": (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{username}" '
            f'password="{password}";'
        ),
    }

# MySQL Configuration
def get_jdbc_config():
    host = get_required_env("MYSQL_HOST")
    port = get_optional_env("MYSQL_PORT", "3306")
    database = get_required_env("MYSQL_DATABASE")
    user = get_required_env("MYSQL_USER")
    password = get_required_env("MYSQL_PASSWORD")
    driver = get_optional_env("MYSQL_DRIVER", "com.mysql.cj.jdbc.Driver")
    
    return {
        "url": f"jdbc:mysql://{host}:{port}/{database}",
        "user": user,
        "password": password,
        "driver": driver,
    }

# Data Sources
FTP_URL = get_optional_env("FTP_URL", "https://ftp.goit.study/neoversity/")

# Data Lake Configuration
DATA_LAKE_PATH = get_optional_env("DATA_LAKE_PATH", "/tmp/data_lake")

# Spark Configuration
SPARK_MASTER = get_optional_env("SPARK_MASTER", "local[*]")
SPARK_APP_NAME = get_optional_env("SPARK_APP_NAME", "OlympicDataPipeline")

# Kafka Topics
def get_kafka_topics():
    return {
        "athlete_event_results": f"{USER_NAME}_athlete_event_results",
        "enriched_athlete_avg": f"{USER_NAME}_enriched_athlete_avg",
    }