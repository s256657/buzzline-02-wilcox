"""
kafka_consumer_wilcox.py

Consume messages from a Kafka topic and process them.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
from collections import Counter

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> int:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("KAFKA_CONSUMER_GROUP_ID_JSON", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Define a function to process messages
# #####################################


# Global counter for player name counts
player_counter = Counter()

def extract_player_name(message: str) -> str:
    """
    Extracts the player name from a standardized message.
    Example: "My Favorite Chiefs player is Mahomes!" -> "Mahomes"
    """
    prefix = "My Favorite Chiefs player is "
    if message.startswith(prefix):
        return message[len(prefix):].replace("!", "").strip()
    return "Unknown"

def process_message(message: str) -> None:
    """
    Process a single message by extracting and counting the player name.
    """
    player_name = extract_player_name(message)
    player_counter[player_name] += 1

    logger.info(f"Processed message for player: {player_name}")
    logger.info(f"{player_name} count: {player_counter[player_name]}")


#####################################
# Main Function
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

        # Show final count summary
        logger.info("=== Final Player Message Counts ===")
        for player, count in player_counter.items():
            logger.info(f"{player}: {count}")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
