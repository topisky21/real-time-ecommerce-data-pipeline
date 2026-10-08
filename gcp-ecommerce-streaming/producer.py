import json
import random
import time
import uuid
from datetime import datetime, timezone

from google.cloud import pubsub_v1


PROJECT_ID = "project-09a7fa74-88ba-427a-b50"
TOPIC_ID = "orders-topic"

publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


products = [
    {
        "product_id": "P001",
        "product_name": "Laptop",
        "category": "Electronics",
        "price": 899.99
    },
    {
        "product_id": "P002",
        "product_name": "Wireless Headphones",
        "category": "Electronics",
        "price": 129.99
    },
    {
        "product_id": "P003",
        "product_name": "Office Chair",
        "category": "Furniture",
        "price": 249.99
    },
    {
        "product_id": "P004",
        "product_name": "Coffee Machine",
        "category": "Kitchen",
        "price": 179.99
    },
    {
        "product_id": "P005",
        "product_name": "Running Shoes",
        "category": "Sports",
        "price": 99.99
    }
]


def generate_order():

    product = random.choice(products)

    quantity = random.randint(1, 3)

    order = {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"CUST{random.randint(1000, 9999)}",
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": product["price"],
        "total_amount": round(product["price"] * quantity, 2),
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
        "country": random.choice([
            "UK",
            "USA",
            "Canada",
            "Germany",
            "France"
        ])
    }

    return order


def publish_order(order):

    message = json.dumps(order).encode("utf-8")

    future = publisher.publish(
        topic_path,
        message
    )

    message_id = future.result()

    print(
        f"Published order {order['order_id']} "
        f"| Pub/Sub message ID: {message_id}"
    )


def main():

    total_orders = 100

    for i in range(total_orders):

        order = generate_order()

        publish_order(order)

        print(f"Order {i + 1}/{total_orders}")

        # Small delay to simulate real-time traffic
        time.sleep(0.1)


if __name__ == "__main__":
    main()