import snap7
import paho.mqtt.client as mqtt
import json
import time
import schedule
import threading

# PLC info
PLC_IP = "192.168.0.1"
PLC_RACK = 0
PLC_SLOT = 1
DB_NUMBER = 15

# MQTT info
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "plc/s7-1200/temperature"

# Connect to the PLC
def connect_plc():
    client = snap7.client.Client()
    client.connect(PLC_IP, PLC_RACK, PLC_SLOT)
    return client

# Get the data from the PLC
def read_temperature(plc_client, offset):
    data = plc_client.db_read(DB_NUMBER, offset, 382)
    return snap7.util.get_real(data, 0)

# Publish the data to the MQTT broker
def publish_mqtt(sensor_id, temperature):
    # client = mqtt.Client()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    payload = json.dumps({"sensor_id": sensor_id, "temperature": temperature})
    client.publish(MQTT_TOPIC, payload, qos=0)
    client.disconnect()
    print(f"Published data to MQTT: {payload}")

def main():
    plc = connect_plc()
    while True:
        try:
            rtd01 = read_temperature(plc, 6)
            rtd02 = read_temperature(plc, 44)

            time.sleep(10)
            # Publish the data to the MQTT broker
            publish_mqtt("RTD01", rtd01)
            publish_mqtt("RTD02", rtd02)


        except Exception as e:
            print(f"An error occurred: {e}")
            break

if __name__ == "__main__":
    main()


