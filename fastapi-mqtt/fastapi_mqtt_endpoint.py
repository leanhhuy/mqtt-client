# https://www.emqx.com/en/blog/comparision-of-python-mqtt-client#fastapi-mqtt-the-fastapi-specialist
# https://fastapi.tiangolo.com/#example

# https://sabuhish.github.io/fastapi-mqtt/example/

# https://github.com/sabuhish/fastapi-mqtt/tree/master/examples

# pip install fastapi
# pip install uvicorn
# pip install fastapi-mqtt

from contextlib import asynccontextmanager

from typing import Any
from fastapi import FastAPI

from gmqtt import Client as MQTTClient
from fastapi_mqtt import FastMQTT, MQTTConfig

mqtt_config = MQTTConfig(
    host="broker.emqx.io",
    port=1883,
    keepalive=60
)

@asynccontextmanager
async def _lifespan(_app: FastAPI):
    await fast_mqtt.mqtt_startup()
    yield
    await fast_mqtt.mqtt_shutdown()

fast_mqtt = FastMQTT(config=mqtt_config)

# Decorator for handling the connect event
@fast_mqtt.on_connect()
def connect(client: MQTTClient, flags:int, rc:int, properties:Any):
    client.subscribe("fastapi-mqtt/test")
    print("Connected: ", client, flags, rc, properties)

# Decorator for handling incoming messages
@fast_mqtt.on_message()    
async def message(client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any):
    print("Received message: ", topic, payload.decode(), qos, properties)


@fast_mqtt.on_disconnect()
def disconnect(client:MQTTClient, packet, exc=None):
    print("Disconnected")

@fast_mqtt.on_subscribe()
def subscribe(client:MQTTClient, mid, qos, properties):
    print("subscribed", client, mid, qos, properties)

app = FastAPI(lifespan=_lifespan)
# app = FastAPI()

@app.get("/")
def read_root():
    return {"FastAPI": "Hello World!"}

# A simple HTTP endpoint to publish a message
@app.post("/publish")
async def publish_message(topic: str, message: str):
    fast_mqtt.publish(topic, message)
    return {"result": "Message published", "topic": topic, "message": message}
