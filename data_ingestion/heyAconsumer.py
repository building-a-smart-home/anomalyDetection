from kafka import KafkaConsumer
import json

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'smart_home_data'

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # Start reading at the beginning of the topic
    enable_auto_commit=True,
    group_id='smart_home_consumer_group',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON messages
)

print(f"Listening for messages on topic: {topic_name}")

# Consume messages from the topic
try:
    for message in consumer:
        # Each message is a JSON object as sent by the producer
        print(f"Received message: {message.value}")
except KeyboardInterrupt:
    print("Stopped by user")
finally:
    # Close the consumer connection
    consumer.close()
