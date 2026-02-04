from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
import json
import time


print("🔌 Testing Kafka connection at localhost:9092...\n")

# First, test if we can reach Kafka at all
try:
    admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'], request_timeout_ms=5000)
    cluster_metadata = admin_client.describe_cluster()
    print(f"✅ Kafka cluster found: {cluster_metadata}\n")
    admin_client.close()
except Exception as e:
    print(f"❌ Cannot reach Kafka: {e}")
    print("   Make sure Docker containers are running:")
    print("   docker-compose -f kafka-docker/docker-compose.yml ps\n")
    exit(1)

print("🔌 Attempting to connect consumer...\n")

def deserialize_value(data):
    """Custom deserializer with error handling"""
    try:
        decoded = data.decode('utf-8')
        print(f"   Raw value (string): {decoded}")
        parsed = json.loads(decoded)
        print(f"   Parsed JSON: {parsed}")
        return parsed
    except Exception as e:
        print(f"   ❌ Deserialization error: {e}")
        print(f"   Raw bytes: {data}")
        return data

while True:
    try:
        consumer = KafkaConsumer(
            'flask-topic',
            bootstrap_servers=['localhost:9092'],
            group_id='test_group',
            auto_offset_reset='earliest',
            value_deserializer=deserialize_value,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            request_timeout_ms=60000,
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            max_poll_records=10,
            fetch_min_bytes=1,
            fetch_max_wait_ms=1000
        )
        
        print("✅ Successfully connected to Kafka!")
        print(f"📋 Subscribed to topic: flask-topic")
        print(f"👥 Consumer group: test_group")
        print(f"📚 Available topics: {consumer.topics()}")
        print("⏳ Waiting for messages... (This will show NEW messages sent after consumer starts)\n")

        # 2. The Consumption Loop
        message_count = 0
        for message in consumer:
            message_count += 1
            print(f"\n✅ [{message_count}] Message Received!")
            print(f"   Value: {message.value}")
            print(f"   Topic: {message.topic} | Partition: {message.partition} | Offset: {message.offset}")

    except KafkaError as e:
        print(f"❌ Kafka Error: {e}")
        print(f"⏳ Retrying in 5 seconds...\n")
        time.sleep(5)
    except KeyboardInterrupt:
        print("\n👋 Consumer stopped by user")
        break
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        print(f"⏳ Retrying in 5 seconds...\n")
        time.sleep(5)
