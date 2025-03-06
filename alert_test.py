import paho.mqtt.client as mqtt
import json
import time
import random

MQTT_BROKER = "194.12.158.118"
MQTT_PORT = 1883
MQTT_TOPIC = "plc/s7-1200/temperature"

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Publish high temperature data
for i in range(50):
    try:
        sensor_id_1 = "RTD01"
        sensor_id_2 = "RTD02"
        temperature = round(random.uniform(30.5, 40.5), 2)
        payload = json.dumps({sensor_id_1: temperature, sensor_id_2: temperature, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")})
        client.publish(MQTT_TOPIC, payload, qos=1)
        print(f"Published data to MQTT: {payload}")

        time.sleep(2)
    except KeyboardInterrupt:
        print("User interrupted the program")
        break
    except Exception as e:
        print(f"Cannot publish data to MQTT: {e}")
        break


print("Finished publishing high temperature data")
client.disconnect()


