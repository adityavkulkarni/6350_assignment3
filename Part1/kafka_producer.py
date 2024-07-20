from configparser import ConfigParser
from kafka import KafkaProducer


class Kafka:
    def __init__(self):
        self._config = ConfigParser()
        self._config.read(["./config.ini"])
        try:
            bootstrap_servers = self._config.get("KAFKA", "bootstrap_servers")
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                linger_ms=10000)
            print(f"Connected to Kafka@{bootstrap_servers}")
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}")

    def publish(self, text, topic):
        try:
            key_bytes = bytes('post', encoding='utf-8')
            value_bytes = bytes(text, encoding='utf-8')
            self.kafka_producer.send(
                topic=topic,
                key=key_bytes,
                value=value_bytes)
            self.kafka_producer.flush()
        except Exception as e:
            print(f'Failed to publish message: {e}')