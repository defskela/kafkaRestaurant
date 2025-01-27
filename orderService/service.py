import json
import time

from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}

producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Ошибка при доставке сообщения: {err}")
    else:
        print(f"Сообщение доставлено в {msg.topic()} [{msg.partition()}]")


def produce_messages():
    topic = "order"

    for order_id in range(1, 6):
        message = {
            "order_id": order_id,
            "dishes": ["pizza", "salad", "soup"]
        }
        producer.produce(
            topic,
            key=str(order_id),
            value=json.dumps(message),
            callback=delivery_report
        )
        producer.flush()
        print(f"Отправлено сообщение: {message}")
        time.sleep(1)


if __name__ == "__main__":
    produce_messages()
