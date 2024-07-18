# ------------------------------------------------------------------------------
# Imports
# ------------------------------------------------------------------------------

import threading
import shutil
from fastapi import Query
import queue
from typing import Literal
from enum import Enum
import logging


# * CUSTOM IMPORTS

# & --------------------------------modules----------------------------------------------
from src.modules.mqtt_modules.config_mqtt import ConfigMQTT
from src.modules.database_modules.database import MongoDBClient

# & --------------------------------services----------------------------------------------
from src.services.database_services.writer_database import DataWriter

from src.services.mqtt_services.mqtt_service import MQTTService

from src.services.database_services.mongo_service_with_many_data_schemas_and_specifics_topics import (
    DataWriterService,
)


# ------------------------------------------------------------------------------
# ManagerMQTT Class Definition
# ------------------------------------------------------------------------------


class ManagerMQTT:
    def __init__(self, config_file):
        self.thread = None
        self.running = False
        self.message_queue = queue.Queue()
        self.instances = {}

        self.client = MongoDBClient.get_client()
        self.config_db = self.client["config_db"]
        self.load_existing_instances()
        self.data_writer = DataWriterService()

        self.saving_thread = threading.Thread(target=self.save_messages)
        self.saving_thread.daemon = (
            True  # Ensure this thread exits when the main program exits
        )
        self.saving_thread.start()

    def load_existing_instances(self):
        collections = self.config_db.list_collection_names()
        for collection in collections:
            self.create_instance(collection)
            print("collection: ", collection)

    def save_messages(self):
        while True:
            topic, message = self.message_queue.get()
            print(f"Queue received topic: {topic}, message: {message}")
            self.data_writer.write_data(topic, message)
            self.message_queue.task_done()

    def create_instance(self, instance_name):
        if instance_name in self.instances:
            raise Exception(f"Instance {instance_name} already exists")

        config_mqtt = ConfigMQTT(self.config_db, instance_name)
        mqtt_instance = MQTTService(config_mqtt, message_queue=self.message_queue)
        self.instances[instance_name] = mqtt_instance

    def delete_instance(self, instance_name):
        if instance_name not in self.instances:
            raise Exception(f"Instance {instance_name} does not exist")
        del self.instances[instance_name]
        self.config_db.drop_collection(instance_name)

    def start_instance(self, instance_name):
        if instance_name not in self.instances:
            raise Exception(f"Instance {instance_name} does not exist")
        instance = self.instances[instance_name]
        if not instance.running:
            instance.thread = threading.Thread(
                target=instance.establish_connection_and_start
            )
            instance.thread.start()
            instance.running = True

    def stop_instance(self, instance_name):
        if instance_name not in self.instances:
            raise Exception(f"Instance {instance_name} does not exist")
        instance = self.instances[instance_name]
        if instance.running:
            instance.client.disconnect()
            instance.client.loop_stop()
            instance.thread.join()
            instance.running = False

    def get_instance(self, instance_name):
        return self.instances.get(instance_name)

    def list_instances(self):
        return list(self.instances.keys())

    def publish_message(self, topic, payload):
        if self.running:
            self.mqtt_instance.client.publish(topic, payload)
        else:
            raise Exception("MQTT client is not running")

    def get_config_value(self, key):
        return self.config_mqtt.get_value(key)

    def set_config_value(self, key, value):
        self.config_mqtt.insert_value(key, value)

    def get_nested_config_value(self, section, key):
        return self.config_mqtt.get_nested_value(section, key)

    def set_nested_config_value(self, section, key, value):
        self.config_mqtt.insert_nested_value(section, key, value=value)

    def set_nested_config_value_jsonTree(self, jesonTree, value):
        self.config_mqtt.insert_nested_value(*jesonTree, value=value)

    def get_messages(self):
        return self.mqtt_instance.messages

    def get_nested_config_value_using_jsonTree(self, instance, jsonTree):
        print(f"jsonTree: {jsonTree}")
        return instance.ConfigMQTT.get_nested_value(*jsonTree)

    def get_nested_config_value_using_jsonTree_HTTP(self, instance, jsonTree):
        try:
            value = self.get_nested_config_value_using_jsonTree(instance, jsonTree)
            return value
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    def set_nested_config_value_using_jsonTree(self, instance, jsonTree, value):
        instance.ConfigMQTT.insert_nested_value(*jsonTree, value=value)

    def set_nested_config_value_using_jsonTree_HTTP(self, instance, jsonTree, value):
        try:
            if value is not None:
                self.set_nested_config_value_using_jsonTree(instance, jsonTree, value)
                return {"message": f"Nested config {jsonTree} set to {value}"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    def update_instance_configuration(self, instance_name, jsonTree, value):
        try:
            instance = self.get_instance(instance_name)
            if not instance:
                raise HTTPException(
                    status_code=404, detail=f"Instance {instance_name} not found"
                )
            self.set_nested_config_value_using_jsonTree_HTTP(
                instance, jsonTree, value=value
            )
            return {
                "message": f"Nested config {jsonTree[0]}.{jsonTree[1]} set to {value} for instance {instance_name}"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
