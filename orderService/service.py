import json

from confluent_kafka import Producer
from flask import Flask, jsonify, request

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}
producer = Producer(conf)

app = Flask(__name__)

orders = {}

NUM_ORDERS = 1


def delivery_report(err, msg):
    if err is not None:
        print(f"Ошибка при доставке сообщения: {err}")
    else:
        print(f"Сообщение доставлено в {msg.topic()} [{msg.partition()}]")


def send_to_kafka(order_id, order_data):
    topic = "order"
    message = {
        "order_id": order_id,
        "dishes": order_data.get("dishes", [])
    }
    producer.produce(
        topic,
        key=str(order_id),
        value=json.dumps(message),
        callback=delivery_report
    )
    producer.flush()


@app.route('/api/orders', methods=['POST'])
def create_order():
    global NUM_ORDERS
    data = request.json

    if not data or 'dishes' not in data:
        return jsonify({"error": "Invalid request. 'dishes' is required"}), 400

    order_id = NUM_ORDERS
    NUM_ORDERS += 1

    orders[order_id] = data

    send_to_kafka(order_id, data)

    return jsonify({"order_id": order_id, "status": "Order created and sent to Kafka"}), 201


@app.route('/api/orders', methods=['GET'])
def get_orders():
    return jsonify(orders), 200


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
