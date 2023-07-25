from kafka.admin import KafkaAdminClient, NewTopic

SERVER_URLS = "kafka:9092".split(",")
admin_client = KafkaAdminClient(bootstrap_servers=SERVER_URLS)
admin_client.create_topics(new_topics=[
    NewTopic(name="SAMPLE_TOPIC", num_partitions=2, replication_factor=1)
], validate_only=False)
print("Created topics:", admin_client.list_topics())
