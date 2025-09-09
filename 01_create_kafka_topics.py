from confluent_kafka.admin import AdminClient, NewTopic
from colorama import Fore, Style
import sys
from config import get_kafka_config, get_kafka_topics, USER_NAME

# Load configuration from environment variables
kafka_config = get_kafka_config()
topics_config = get_kafka_topics()

# Setup admin client
admin_client = AdminClient(
    {
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": kafka_config["security_protocol"],
        "sasl.mechanisms": kafka_config["sasl_mechanism"],
        "sasl.username": kafka_config["username"],
        "sasl.password": kafka_config["password"],
    }
)

# Topic names from configuration
athlete_event_results = topics_config["athlete_event_results"]
enriched_athlete_avg = topics_config["enriched_athlete_avg"]
num_partitions = 2
replication_factor = 1

print(f"🚀 Creating Kafka topics for {USER_NAME}")
print(f"📋 Topics to create:")
print(f"   - {athlete_event_results}")
print(f"   - {enriched_athlete_avg}")

# Create topics
topics = [
    NewTopic(athlete_event_results, num_partitions, replication_factor),
    NewTopic(enriched_athlete_avg, num_partitions, replication_factor),
]

# Delete topics (optional, if they exist)
try:
    delete_futures = admin_client.delete_topics(
        [athlete_event_results, enriched_athlete_avg]
    )
    for topic, future in delete_futures.items():
        try:
            future.result()  # Block until deletion completes
            print(Fore.RED + f"Topic '{topic}' deleted successfully." + Style.RESET_ALL)
        except Exception as e:
            print(Fore.YELLOW + f"Topic '{topic}' might not exist: {e}" + Style.RESET_ALL)
except Exception as e:
    print(Fore.YELLOW + f"Note during topic deletion: {e}" + Style.RESET_ALL)

# Create topics
try:
    create_futures = admin_client.create_topics(topics)
    for topic, future in create_futures.items():
        try:
            future.result()  # Block until creation completes
            print(
                Fore.GREEN + f"✅ Topic '{topic}' created successfully." + Style.RESET_ALL
            )
        except Exception as e:
            print(Fore.RED + f"❌ Failed to create topic '{topic}': {e}" + Style.RESET_ALL)
except Exception as e:
    print(Fore.RED + f"An error occurred during topic creation: {e}" + Style.RESET_ALL)
    sys.exit(1)

# List existing topics to verify
try:
    metadata = admin_client.list_topics(timeout=10)
    print(f"\n📋 Kafka topics containing '{USER_NAME}':")
    for topic in metadata.topics.keys():
        if USER_NAME in topic:
            print(Fore.YELLOW + f"   - {topic}" + Style.RESET_ALL)
except Exception as e:
    print(Fore.RED + f"Failed to list topics: {e}" + Style.RESET_ALL)

print("\n🎉 Kafka topic setup completed!")