import serial
from rabbitmq import RabbitMQ

ARDUINO_PORT = "/dev/ttyUSB0"
RABBITMQ_URL = "54.243.203.221"
RABBITMQ_USER = "babyUser"
RABBITMQ_PASSWORD = "Piloto123"

RABBIT_SCHEMA = {
    "name": "babyWatcher",
    "type": "topic",
    "queues": [
        {"name": "babySensorsData", "routing_key": "new.data"},
    ],
}


def main():
    rabbit = RabbitMQ(
        RABBITMQ_URL, RABBITMQ_USER, RABBITMQ_PASSWORD, schema=RABBIT_SCHEMA
    )

    port = serial.Serial(ARDUINO_PORT, 9600)
    port.flush()

    while True:
        if port.in_waiting > 0:
            line = port.readline().decode("utf-8").rstrip()

            # TODO: Improve hardcoded data
            rabbit.send("babyWatcher", "new.data", line)


if __name__ == "__main__":
    main()
