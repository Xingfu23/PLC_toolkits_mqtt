import snap7
import paho.mqtt.client as mqtt
import json
import time
import schedule
import threading

# Set up threaded operation
def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

# PLC info
PLC_IP = "192.168.0.1"
PLC_RACK = 0
PLC_SLOT = 1
DB_NUMBER = 15

# MQTT info
# MQTT_BROKER = "localhost"
MQTT_BROKER = "194.12.158.118"
MQTT_PORT = 1883
MQTT_TOPIC = "plc/s7-1200/temperature"

# Connect to the PLC
def connect_plc():
    client = snap7.client.Client()
    client.connect(PLC_IP, PLC_RACK, PLC_SLOT)
    return client

# Get the data from the PLC
def read_temperature(plc_client, offset):
    data = plc_client.db_read(DB_NUMBER, offset, 50)
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

    rtd01 = read_temperature(plc, 6)
    rtd02 = read_temperature(plc, 44)

    # Publish the data to the MQTT broker
    publish_mqtt("RTD01", rtd01)
    publish_mqtt("RTD02", rtd02)

if __name__ == "__main__":
    # main()
    schedule.every().minute.at(":00").do(run_threaded, main)
    schedule.every().minute.at(":20").do(run_threaded, main)
    schedule.every().minute.at(":40").do(run_threaded, main)
    while True:
        schedule.run_pending()
        time.sleep(1)


