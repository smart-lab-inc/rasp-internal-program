import time
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
    last_update_time = time.time()
    is_ready_to_store = False

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

                    data_dict = json.loads(line)
                    data_dict = add_timestamp_property(data_dict)

                    body = {
                        "monitorId": MONITOR_SERIAL_NUMBER,
                        "body": data_dict,
                    }

                    if time.time() - last_update_time >= 60:
                        is_ready_to_store = "true"
                        last_update_time = time.time()
                    else:
                        is_ready_to_store = "false"

                    body["isReadyToStore"] = is_ready_to_store

                    rabbit.send("babyWatcher", "new.data", json.dumps(body))
        except (AMQPConnectionError, ChannelClosedByBroker, ConnectionClosed) as e:
            print("Error with RabbitMQ connection. Trying to reconnect...")
            continue
        except KeyboardInterrupt:
            print("\nClosing connection...")
            if rabbit:
                rabbit.close_connection()
            break


def add_timestamp_property(items: List[dict]) -> List[dict]:
    current_datetime = datetime.now()

    for item in items:
        item["timestamp"] = current_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")

    return items


if __name__ == "__main__":
    main()
