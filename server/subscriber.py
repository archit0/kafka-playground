import ssl
import json
from kafka import KafkaConsumer


def task(key, value):
    print(key, value)


WORKER_TOPIC_MAPPING = {
    'SAMPLE_TOPIC3': task,
}


def consume():
    print("Beginning Worker")
    context = ssl.SSLContext()
    context.verify_mode = ssl.CERT_NONE
    context.check_hostname = False

    consumer = KafkaConsumer(
        *list(WORKER_TOPIC_MAPPING.keys()),
        bootstrap_servers=['kafka:29092'],
        group_id='GROUP1',
        enable_auto_commit=False,
        security_protocol='PLAINTEXT',
        ssl_context=None,
    )

    for msg in consumer:
        print(f"Consumed: {msg.topic}, {msg.partition}, {msg.offset}, {msg.key}, {msg.value}, {msg.timestamp}")
        try:
            WORKER_TOPIC_MAPPING[msg.topic](msg.key.decode(), json.loads(msg.value))
        except Exception as e:
            print(f"Error: {e}")
        consumer.commit()

consume()