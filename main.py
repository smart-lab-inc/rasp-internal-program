from typing import List
import serial
from rabbitmq import RabbitMQ
import json
from datetime import datetime
from pika.exceptions import AMQPConnectionError, ChannelClosedByBroker, ConnectionClosed
from decouple import config


ARDUINO_PORT = f"/dev/{config('ARDUINO_PORT')}"
RABBITMQ_HOST = config("RABBITMQ_HOST")
RABBITMQ_USER = config("RABBITMQ_USER")
RABBITMQ_PASSWORD = config("RABBITMQ_PASSWORD")
MONITOR_SERIAL_NUMBER = config("MONITOR_SERIAL_NUMBER")

RABBIT_SCHEMA = {
    "name": "babyWatcher",
    "type": "topic",
    "queues": [
        {"name": "babySensorsData", "routing_key": "new.data"},
    ],
}


def main():
    rabbit = None

    while True:
        try:
            rabbit = RabbitMQ(
                RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASSWORD, schema=RABBIT_SCHEMA
            )

            port = serial.Serial(ARDUINO_PORT, 9600)
            port.flush()

            while True:
                if port.in_waiting > 0:
                    line = port.readline().decode("utf-8").rstrip()
                    data_json = json.loads(line)

                    body = {
                        "monitorId": MONITOR_SERIAL_NUMBER,
                        "body": addTimestamp(data_json),
                    }

                    rabbit.send("babyWatcher", "new.data", json.dumps(body))
        except (AMQPConnectionError, ChannelClosedByBroker, ConnectionClosed) as e:
            print("Error with RabbitMQ connection. Trying to reconnect...")
            continue
        except KeyboardInterrupt:
            print("\nClosing connection...")
            if rabbit:
                rabbit.close_connection()
            break


def addTimestamp(items: List[dict]) -> List[dict]:
    current_datetime = datetime.now()

    for item in items:
        item["timestamp"] = current_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")

    return items


if __name__ == "__main__":
    main()
