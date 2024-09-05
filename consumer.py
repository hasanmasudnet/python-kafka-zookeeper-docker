from confluent_kafka import Consumer, KafkaException

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest'
}

# Create Kafka consumer
consumer = Consumer(**conf)

# Subscribe to the topic 'test-topic'
consumer.subscribe(['test-topic'])

try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll for messages
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        
        received_message = msg.value().decode('utf-8')
        print(f"Yes! I have received your '{received_message}'")

except KeyboardInterrupt:
    pass
finally:
    # Clean up the consumer before exiting
    consumer.close()
