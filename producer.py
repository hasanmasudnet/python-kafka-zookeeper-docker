from confluent_kafka import Producer

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:29092'  # Kafka broker
}

# Create Kafka producer
producer = Producer(**conf)

# Delivery callback to confirm message delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Sent: {msg.value().decode('utf-8')}")

# Produce messages based on user input
try:
    while True:
        message = input("Enter your message: ")
        if message.lower() == 'exit':
            break  # Stop the producer if the user types 'exit'
        producer.produce('test-topic', value=message.encode('utf-8'), callback=delivery_report)
        producer.poll(0)  # Trigger delivery callback
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
