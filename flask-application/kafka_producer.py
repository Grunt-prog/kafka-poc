from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',  # Wait for all replicas to acknowledge
    retries=3,
    max_in_flight_requests_per_connection=1  # Ensure ordering
)

def send_to_kafka(topic, data):
    try:
        # Send and wait for confirmation
        future = producer.send(topic, data)
        record_metadata = future.get(timeout=10)
        
        print(f"✅ Message sent to {record_metadata.topic} "
              f"partition {record_metadata.partition} "
              f"offset {record_metadata.offset}")
        
        producer.flush()
        return True
    except Exception as e:
        print(f"❌ Failed to send message: {e}")
        return False