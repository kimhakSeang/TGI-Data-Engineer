from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer
import json
from datetime import datetime

app = FastAPI()

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "order-topic"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

class Customer(BaseModel):
    name: str
    age: int

class Product(BaseModel):
    name: str
    category: str
    price: float

class Order(BaseModel):
    orderDate: datetime
    customer: Customer
    product: Product
    quantity: int

@app.post("/orders")
async def create_order(order: Order):
    try:
        # Pydantic v2: model_dump instead of dict
        order_dict = order.model_dump()
        order_dict["orderDate"] = order.orderDate.isoformat()

        producer.send(TOPIC_NAME, order_dict)
        producer.flush()
        return {"status": "success", "message": "Order sent to Kafka"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("Order_App:app", host="0.0.0.0", port=8081, reload=True)
