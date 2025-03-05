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

# Global PLC client and prcessing lock
plc_client = None
plc_lock = threading.Lock()

def init_plc():
    global plc_client
    client = snap7.client.Client()
    client.connect(PLC_IP, PLC_RACK, PLC_SLOT)
    plc_client = client
    print("PLC connection established")

# Get the data from the PLC
def read_temperature(offset):
    with plc_lock:
        data = plc_client.db_read(DB_NUMBER, offset, 4)
    return snap7.util.get_real(data, 0)

# Publish the data to the MQTT broker
def publish_mqtt_batch(data):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        # payload = json.dumps({"sensor_id": sensor_id, "temperature": temperature})
        payload = json.dumps(data)
        client.publish(MQTT_TOPIC, payload, qos=1)
        print(f"Published playload to MQTT: {payload}")
    except Exception as e:
        print(f"Cannot publish data to MQTT: {e}")
    finally:
        client.disconnect()

def schedule_job():
    try:
        rtd01 = read_temperature(6)
        rtd02 = read_temperature(44)
        # Combine the data into a single payload
        data = {
            "RTD01": rtd01,
            "RTD02": rtd02,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        publish_mqtt_batch(data)
    except Exception as e:
        print(f"Error reading PLC data: {e}")

if __name__ == "__main__":
    # Establish connection to the PLC
    init_plc()

    # Schedule the job with 20 seconds interval
    schedule.every().minute.at(":00").do(run_threaded, schedule_job)
    schedule.every().minute.at(":20").do(run_threaded, schedule_job)
    schedule.every().minute.at(":40").do(run_threaded, schedule_job)

    while True:
        schedule.run_pending()
        time.sleep(1)

