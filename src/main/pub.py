# import argparse
import paho.mqtt.client as mqtt
import json
import json
import time


if __name__ == '__main__':
    broker = "localhost"  # host name, replace with IP address
    client = mqtt.Client("mqtt_test") #create client object
    client.connect(broker) #establishing connection
    topic = 'test_topic1'
    input_file = 'input/example_1.json'

    with open(input_file, 'r') as f:
        input_json = json.load(f)
    ret = client.publish(topic, payload=json.dumps(input_json).encode("utf-8"))
    time.sleep(1)
