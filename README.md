# PLC_toolkits_mqtt
 
It is a toolkit for the PLC control system.
For now, the data flow structure is:
1. The data from the sensors is published to the MQTT server (using Eclipse Mosquitto). The script "plc_to_mqtt.py" is responsible for this task.
2. The PostgreSQL database subscribes to an MQTT server and records the data in the database. Please check the script "mqtt_to_db.py."
To execute the programme, please **simultaneously** execute the scripts: "plc_to_mqtt.py" and "mqtt_to_db.py".

TODO: visualisation the data from the database via Grafana and setting an alarm system
