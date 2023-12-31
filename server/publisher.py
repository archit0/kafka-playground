from kafka import KafkaProducer
import ssl
import json

context = ssl.SSLContext()
context.verify_mode = ssl.CERT_NONE
context.check_hostname = False

producer = KafkaProducer(
    bootstrap_servers=["kafka:29092"],
)
producer.send("SAMPLE_TOPIC3", value=json.dumps({
    "ar": 1
}).encode(), key="client1".encode())
producer.close()
print("Done")
