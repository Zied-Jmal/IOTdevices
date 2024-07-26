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
from fastapi import FastAPI, HTTPException, File, UploadFile


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
    def __init__(self):
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
        self.periodic_threads = {}  # Track periodic threads for each instance

    def load_existing_instances(self):
        collections = self.config_db.list_collection_names()
        for collection in collections:
            autostart = self.config_db[collection].find_one()["autostart"]
            self.create_instance(collection, autostart=autostart)
            # print("collection: ", collection)

    def save_messages(self):
        while True:
            topic, message = self.message_queue.get()
            # print(f"Queue received topic: {topic}, message: {message}")
            self.data_writer.write_data(topic, message)
            self.message_queue.task_done()

    def create_instance(self, instance_name, autostart=False):
        if instance_name in self.instances:
            raise Exception(f"Instance {instance_name} already exists")

        config_mqtt = ConfigMQTT(self.config_db, instance_name)
        mqtt_instance = MQTTService(config_mqtt, message_queue=self.message_queue)
        self.instances[instance_name] = mqtt_instance
        if autostart:
            self.start_instance(instance_name)
            if config_mqtt.get_value("periodic_message").get("active"):
                mqtt_instance.start_periodic_messages()
                self.periodic_threads[instance_name] = mqtt_instance.periodic_thread
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
            instance.stop_periodic_messages()  # Stop periodic messages before stopping instance
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
            jsonTreeFormatter = lambda jsonTree: ".".join(map(str, jsonTree))
                      # Restart periodic messages if the configuration changes
            if jsonTree == ["periodic_message", "active"]:
                if value:
                    instance.start_periodic_messages()
                else:
                    instance.stop_periodic_messages()
            return {
                "message": f"Nested config {jsonTreeFormatter} set to {value} for instance {instance_name}"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        
    def add_topic(self, instance_name: str, topic: str):
        if instance_name not in self.instances:
            raise Exception(f"Instance {instance_name} does not exist")
        
        instance = self.instances[instance_name]
        topics = instance.ConfigMQTT.get_value("topics")
        if topic not in topics:
            topics.append(topic)
            instance.ConfigMQTT.update_value("topics", topics)
            instance.client.subscribe(topic)
            return {"message": f"Topic {topic} added to instance {instance_name}"}
        else:
            raise Exception(f"Topic {topic} already exists in instance {instance_name}")

    def delete_topic(self, instance_name: str, topic: str):
        if instance_name not in self.instances:
            raise Exception(f"Instance {instance_name} does not exist")

        instance = self.instances[instance_name]
        topics = instance.ConfigMQTT.get_value("topics")
        if topic in topics:
            topics.remove(topic)
            instance.ConfigMQTT.update_value("topics", topics)
            instance.client.unsubscribe(topic)
            return {"message": f"Topic {topic} removed from instance {instance_name}"}
        else:
            raise Exception(f"Topic {topic} does not exist in instance {instance_name}")

    def list_topics(self, instance_name: str):
        if instance_name not in self.instances:
            raise Exception(f"Instance {instance_name} does not exist")
        
        instance = self.instances[instance_name]
        topics = instance.ConfigMQTT.get_value("topics")
        return {"instance_name": instance_name, "topics": topics}
    
    def get_all_config_data(self, instance):
        try:
            config_mqtt = instance.ConfigMQTT.get_all_values()
            return config_mqtt  # This should return the full configuration
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
