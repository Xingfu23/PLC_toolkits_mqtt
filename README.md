# PLC Control System Toolkit

This toolkit is designed for the PLC control system.

## Data Flow

- Sensor data is published to the MQTT server (Eclipse Mosquitto).  
- The PostgreSQL database subscribes to the MQTT server and records the incoming data.

## Usage

To run the program, execute the script:

```bash
python plc_to_db.py
```

## TODO

- Upload the Docker Compose file  
- Make the Grafana panel accessible on the NTU network