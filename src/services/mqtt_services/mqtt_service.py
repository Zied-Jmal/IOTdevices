import os
import sys
import json
import os
import time
import threading
import queue
import paho.mqtt.client as mqtt
import logging
import ssl
from src.modules.mqtt_modules.config_mqtt import ConfigMQTT

logging.basicConfig(level=logging.DEBUG)

# Now you should be able to import the module
# from config_json import ConfigJson


class MQTTService:
    def __init__(self, ConfigMQTT, message_queue):
        self.ConfigMQTT = ConfigMQTT
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.ConfigMQTT.get_value("client_id"),
        )
        self.running = False
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        self.client.on_unsubscribe = self.on_unsubscribe

        self.client.user_data_set([])
        self.messages = []
        self.message_queue = message_queue

    def on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        for reason_code in reason_code_list:
            if reason_code.is_failure:
                print(f"Broker rejected your subscription: {reason_code}")
            else:
                print(f"Broker granted the following QoS: {reason_code.value}")

    def on_unsubscribe(self, client, userdata, mid, reason_code_list, properties):
        if len(reason_code_list) == 0 or not reason_code_list[0].is_failure:
            print("Unsubscribe succeeded (if SUBACK is received in MQTTv3 it success)")
        else:
            print(f"Broker replied with failure: {reason_code_list[0]}")
        client.disconnect()

    def on_message(self, client, userdata, message):
        self.messages.append(message.payload)
        self.message_queue.put((message.topic, message.payload.decode()))
        # print(f">for this topic : {message.topic} ")
        # print(f"-->Received the following message: {message.payload.decode()}")
        if len(self.messages) > 1000:
            self.messages = []

        # self.message_queue.put((message.topic, message.payload.decode()))

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            logging.error(f"Failed to connect: {reason_code}. Retrying...")
        else:
            logging.info(f"Connected with reason code {reason_code}")
            for topic in userdata["topics"]:
                logging.info(f"Subscribing to topic: {topic}")
                client.subscribe(topic)

    def establish_connection(self):
        # self.client.reinitialise(mqtt.CallbackAPIVersion.VERSION2)
        # Set connection parameters
        broker_address = self.ConfigMQTT.get_value("broker_address")
        port = int(self.ConfigMQTT.get_value("port"))
        keepalive = self.ConfigMQTT.get_nested_value("settings", "keep_alive")
        print("broker_address:", broker_address)
        print("port:", port)
        print("keepalive:", keepalive)
        if self.ConfigMQTT.get_nested_value("settings", "auth"):
            print("activate AUTH")
            self.client.username_pw_set(
                self.ConfigMQTT.get_nested_value("settings", "username"),
                self.ConfigMQTT.get_nested_value("settings", "password"),
            )
        if self.ConfigMQTT.get_nested_value("settings", "ssl"):
            print("activate SSL")
            self.client.tls_set(
                ca_certs=self.ConfigMQTT.get_nested_value("settings", "ca_certs"),
                certfile=self.ConfigMQTT.get_nested_value("settings", "certfile"),
                keyfile=self.ConfigMQTT.get_nested_value("settings", "keyfile"),
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLS,
                ciphers=None,
            )

        if self.ConfigMQTT.get_nested_value("settings", "tls_insecure"):
            self.client.tls_insecure_set
        # Connect to the broker
        # try:
        #    self.client.disconnect()
        #    print("disconnect")
        # except Exception as e:
        #    print(f"No previous connection to disconnect: {e}")
        self.client.connect(broker_address, port=port, keepalive=keepalive)
        if not broker_address:
            raise ValueError("Invalid broker address")
        # Start the MQTT client loop

    def start(self):
        self.client.user_data_set({"topics": self.ConfigMQTT.get_value("topics")})
        self.client.loop_start()
        print(f"Received the following messages: {self.messages}")

    def establish_connection_and_start(self):
        self.establish_connection()
        self.start()

    def stop(self):
        logging.info("Stopping MQTT client loop and disconnecting...")
        self.client.loop_stop()
        self.client.disconnect()
