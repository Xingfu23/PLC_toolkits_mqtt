import paho.mqtt.client as mqtt
import psycopg2
import json
import getpass

# Postgres SQL connection
# DB_HOST = "localhost"
DB_HOST = "194.12.158.118"
DB_PORT = "5432"
DB_USER = "postgres"
DB_NAME = "plc_data"
DB_PASSWORD = getpass.getpass("Enter the password for the database: ")

# MQTT connection
# MQTT_BROKER = "localhost"
MQTT_BROKER = "194.12.158.118"
MQTT_PORT = 1883
MQTT_TOPIC = "plc/s7-1200/temperature"

# Connect to PostgresSQL
def connect_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            port=DB_PORT,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Cannot connect to database: {e}")
        return None

# When a message is received from the MQTT broker, insert it into the database
def on_message(client, userdata, message):
    try:
        # Analyse the json message
        payload = json.loads(message.payload.decode())
        sensor_id = payload["sensor_id"] # for example: "RTD01"
        temperature = payload["temperature"] # for example: 25.5

        print(f"Received message from MQTT broker: {sensor_id} - {temperature:.2f}")
    
        # Insert the message into the database
        conn = connect_db()
        if conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO temperature_data (sensor_id, temperature) VALUES (%s, %s)", (sensor_id, temperature))
            conn.commit()
            cur.close()
            conn.close()
            print("Inserted into database")
    except Exception as e:
        print(f"Error ouccured: {e}")

def main():
    # Subscribe to the MQTT topic
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(MQTT_TOPIC, qos=0)
    print(f"Subscribed to MQTT topic: {MQTT_TOPIC}")
    client.loop_forever()

if __name__ == "__main__":
    main()
