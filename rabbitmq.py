from typing import Union
import pika
from pika.credentials import PlainCredentials


class RabbitMQ:
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        schema: Union[None, dict] = None,
    ) -> None:
        credentials = PlainCredentials(user, password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, credentials=credentials)
        )
        self.channel = self.connection.channel()

        if schema is not None:
            self.setup_schema(schema)

    def create_exchange(self, name: str, type: str):
        self.channel.exchange_declare(
            exchange=name, exchange_type=type, durable=True
        )

    def create_queue(self, name: str):
        self.channel.queue_declare(queue=name)

    def bind_queue(self, exchange: str, queue: str, routing_key: str):
        self.channel.queue_bind(
            exchange=exchange, queue=queue, routing_key=routing_key
        )

    def send(self, exchange: str, routing_key: str, msg: str) -> None:
        self.channel.basic_publish(
            exchange=exchange, routing_key=routing_key, body=msg
        )

    def close_connection(self) -> None:
        self.connection.close()

    def setup_schema(self, schema: dict):
        exchange_name = schema["name"]
        exchange_type = schema["type"]
        queues = schema["queues"]

        self.create_exchange(exchange_name, exchange_type)

        for queue in queues:
            queue_name = queue["name"]
            queue_routing_key = queue["routing_key"]

            self.create_queue(queue_name)

            if queue_routing_key:
                self.bind_queue(exchange_name, queue_name, queue_routing_key)
