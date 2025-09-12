import paho.mqtt.client as mqtt
import psycopg2
import psycopg2.pool
import json
import threading
import queue
import time
import getpass
import pytz
from datetime import datetime

# Database info
DB_HOST = "172.19.16.1"
DB_PORT = "5432"
DB_USER = "TIDC_B205"
DB_NAME = "PLC_COLLECT"
DB_PASSWORD = getpass.getpass("Enter the password for the database: ")

# MQTT connection info
MQTT_BROKER = "172.19.16.1"
MQTT_PORT = 1883
MQTT_TOPIC = "plc/s7-1200/temperature"

# Sensor id information
sensor_id_list = [
    "RTD01",
    "RTD02",
]

# Set up PostgresSQL connection pool
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(
        1, 20, # Min and max connections
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )
    if db_pool:
        print("PostgresSQL connection pool created")
except Exception as e:
    print(f"Cannot create PostgresSQL connection pool: {e}")

# Set up a queue for the database connection
msg_queue = queue.Queue()

def db_worker(worker_id):
    """
    Database write worker thread, retrieves messages from the queue and writes to PostgreSQL.
    """
    taipei_tz = pytz.timezone('Asia/Taipei')
    while True:
        msg_playload = msg_queue.get()
        if msg_playload is None:
            print(f"Worker {worker_id} stopping")
            msg_queue.task_done()
            break # Stop the thread
        try:
            # get a connection from the pool
            conn = db_pool.getconn()
            cursor = conn.cursor()
            data = json.loads(msg_playload)

            time_str = data['timestamp']
            local_time = datetime.fromisoformat(time_str) # fromisoformat() 函式，它能自動解析 ISO 8601 時間格式
            utc_time = local_time.astimezone(pytz.utc)

            insert_sql = "INSERT INTO measurements (sensor_id, value, timestamp) VALUES (%s, %s, %s)"
            # cursor.execute(insert_sql, (data["sensor_id"], data["temperature"]))
            for sensor_id in sensor_id_list:
                cursor.execute(insert_sql, (sensor_id, data[sensor_id], utc_time))
            conn.commit()   
            cursor.close()
            db_pool.putconn(conn)
        except Exception as e:
            print(f"woker {worker_id} error: {e}")
        finally:
            msg_queue.task_done()

num_workers = 4
workers = []
for i in range(num_workers):
    t = threading.Thread(target=db_worker, args=(i+1,), daemon=True)
    t.start()
    workers.append(t)

def on_connect(client, userdata, flags, rc, properties):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Connected with result code {rc}")
    client.subscribe(MQTT_TOPIC, qos=1)

def on_message(client, userdata, msg):
    try:
        msg_queue.put(msg.payload.decode("utf-8"))
        print(f"Received message: {msg.payload}")
    except Exception as e:
        print(f"Error occured: {e}")

def on_disconnect(client, userdata, flags, rc, properties):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Disconnected with result code {rc}")

def on_log(client, userdata, level, buf):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - MQTT Log: {buf}")

def main():
    # Set up the MQTT client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_log = on_log

    # Connect to the MQTT broker
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()


    try:
        while True:
            time.sleep(10)
            print(f"Queue size: {msg_queue.qsize()}")
    except KeyboardInterrupt:
        print("Exiting")
    finally:
        client.loop_stop()
        for _ in range(num_workers):
            msg_queue.put(None)
        for worker in workers:
            worker.join() # Wait for the worker threads to finish
        # msg_queue.put(None) # Stop the worker thread

if __name__ == "__main__":
    main()