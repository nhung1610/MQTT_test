import datetime
import logging
import json

class Subscriber:

    def __init__(self, broker, topic, client):
        self.broker = broker
        self.topic = topic
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

        self.client = client
        self.client.enable_logger(logger)

        self.payload = ""
        self.old_mess = self.payload
        self.payload_json = ""

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
        self.payload_json = json.loads(self.payload.encode('utf-8'))
        print(str(self.payload))

    def start_receiving(self):
        # assign function to callback
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_connect = self.on_connect

        self.logger.info("Connecting to broker host...")
        print("Connecting to broker host", self.broker)
        self.client.connect(self.broker)  # connection establishment with broker
        self.logger.info("Subscribing topic...")
        self.client.subscribe(self.topic)  # subscribe topic test
        self.client.loop_start()  # continously checking for message

