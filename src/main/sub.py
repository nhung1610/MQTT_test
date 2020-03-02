import paho.mqtt.client as paho
import datetime
from time import sleep
import logging


class Subscriber:

    def __init__(self, broker, topic):
        logger = logging.getLogger('PythonLogger')
        logger.setLevel(logging.DEBUG)
        # Add the log message handler to the logger
        handler = logging.FileHandler('mqtt_log.json', mode='w', encoding=None, delay=False)
        # Create a Formatter for formatting the log messages

        formatter = logging.Formatter(
            '{"tag":"MQTT", "level": "%(levelno)s", "msg": "%(message)s", "time":"%(asctime)s"}'
            , datefmt='%Y-%m-%dT%H:%M:%S.%fZ'
        )

        # Add the Formatter to the Handler
        handler.setFormatter(formatter)

        if (logger.hasHandlers()):
            logger.handlers.clear()
        # Add the handler to the logger
        logger.addHandler(handler)

        self.logger = logger

        self.client = paho.Client("user")  # create client object
        self.client.enable_logger(logger)

        self.payload = ""
        self.old_mess = self.payload

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("Connection successful")
            print("Connection successful")
        else:
            self.logger.info("Connection error")
            print("Connection error")

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            self.logger.info("Disconnection")
            print("Unexpected disconnection")

    def on_message(self, client, userdata, msg):
        now = datetime.datetime.now()
        self.payload = msg.payload.decode('utf-8')
        print(str(self.payload))
        folder = 'output/'
        filename = "file_%d%02d%02d%02d.jsonl" % (now.year, now.month, now.day, now.hour)
        with open(folder+filename, 'a+') as f:
            f.write(str(self.payload) + '\n')

    def start_receiving(self):
        broker = 'localhost'
        topic = 'test'
        # assign function to callback
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_connect = self.on_connect

        self.logger.info("Connecting to broker host...")
        print("Connecting to broker host", broker)
        self.client.connect(broker)  # connection establishment with broker
        self.logger.info("Subscribing topic...")
        self.client.subscribe(topic)  # subscribe topic test
        self.client.loop_start()  # continously checking for message

        while True:
            if self.payload != self.old_mess:
                yield self.payload
                self.old_mess = self.payload
            sleep(1)
        self.client.loop_stop()
        self.client.disconnect()

if __name__ == '__main__':
    broker = "localhost"  # host name
    topic = 'test_topic1'

    subscriber = Subscriber(broker, topic)

    for msg in subscriber.start_receiving():
        print(msg)



