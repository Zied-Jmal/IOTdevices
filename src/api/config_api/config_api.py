
import os
import sys
from fastapi import FastAPI, HTTPException, File, UploadFile
from pydantic import BaseModel
import threading
import shutil
from fastapi import Query
import queue
from typing import Literal
from enum import Enum
import logging

from src.managers.mqtt_manager.MQTTManager import ManagerMQTT
# ------------------------------------------------------------------------------
# FastAPI Setup
# ------------------------------------------------------------------------------
app = FastAPI()

manager = ManagerMQTT("your_config_file.json")


# ------------------------------------------------------------------------------
# request_body
# ------------------------------------------------------------------------------
class PublishRequest(BaseModel):
    topic: str
    payload: str


class ConfigRequest(BaseModel):
    key: str
    value: str


class NestedConfigRequest(BaseModel):
    section: str
    key: str
    value: str


class QosEnum(int, Enum):
    low = 0
    medium = 1
    high = 2


#! Red (!)
# ? Blue (?)
# * Green (*)
# ^ Yellow (^)
# & Pink (&)
# ~ Purple (~)
# todo Mustard (todo)
# // Grey (//)


@app.post("/settings/{instance_name}/{key}", tags=["get configuration generic"])
async def get_config1(instance_name: str, key: str):
    print("get config")
    jsonTree = ["settings", key]
    try:
        instance = manager.get_instance(instance_name)
        if not instance:
            raise HTTPException(
                status_code=404, detail=f"Instance {instance_name} not found"
            )
        value = manager.get_nested_config_value_using_jsonTree_HTTP(instance, jsonTree)

        response_data = {"instance_name": instance_name, "key": key, "value": value}
        return response_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# &------------------------------------------------------------------------------
# &FastAPI Routes :GET
# &------------------------------------------------------------------------------
# ~ ------------------------------------------------------------------------------
# ~ FastAPI  : get configuration
# ~------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# FastAPI Routes :POST
# ------------------------------------------------------------------------------
@app.post("/start")
async def start_mqtt():
    try:
        manager.start_mqtt()
        return {"message": "MQTT client started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/stop")
async def stop_mqtt():
    try:
        manager.stop_mqtt()
        return {"message": "MQTT client stopped"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/publish")
async def publish_message(request: PublishRequest):
    try:
        manager.publish_message(request.topic, request.payload)
        return {"message": f"Message published to {request.topic}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/config")
async def set_config(request: ConfigRequest):
    try:
        manager.set_config_value(request.key, request.value)
        return {"message": f"Config {request.key} set to {request.value}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/config/nested")
async def set_nested_config(request: NestedConfigRequest):
    try:
        manager.set_nested_config_value(
            request.section, request.key, value=request.value
        )
        return {
            "message": f"Nested config {request.section}.{request.key} set to {request.value}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


UPLOAD_DIR = "Z:/shellyG2/4"


# ------------------------------------------------------------------------------
# MQTT mongo MANAGER
# ------------------------------------------------------------------------------
@app.post("/instances/create/{instance_name}", tags=["MQTT instance"])
async def create_instance(instance_name: str):
    try:
        manager.create_instance(instance_name)
        return {"message": f"Instance {instance_name} created"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/instances/delete/{instance_name}", tags=["MQTT instance"])
async def delete_instance(instance_name: str):
    try:
        manager.delete_instance(instance_name)
        return {"message": f"Instance {instance_name} deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/instances/start/{instance_name}", tags=["MQTT instance"])
async def start_instance(instance_name: str):
    try:
        manager.start_instance(instance_name)
        return {"message": f"Instance {instance_name} started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/instances/stop/{instance_name}", tags=["MQTT instance"])
async def stop_instance(instance_name: str):
    try:
        manager.stop_instance(instance_name)
        return {"message": f"Instance {instance_name} stopped"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/instances/list", tags=["MQTT instances"])
async def list_instances():
    try:
        instances = manager.list_instances()
        return {"instances": instances}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/instances/{instance_name}/config")
async def set_instance_config(instance_name: str, request: ConfigRequest):
    try:
        instance = manager.get_instance(instance_name)
        if not instance:
            raise HTTPException(
                status_code=404, detail=f"Instance {instance_name} not found"
            )
        instance.config_mqtt.insert_value(request.key, request.value)
        return {
            "message": f"Config {request.key} set to {request.value} for instance {instance_name}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/instances/{instance_name}/config/nested")
async def set_instance_nested_config(instance_name: str, request: NestedConfigRequest):
    try:
        instance = manager.get_instance(instance_name)
        if not instance:
            raise HTTPException(
                status_code=404, detail=f"Instance {instance_name} not found"
            )
        instance.config_mqtt.insert_nested_value(
            request.section, request.key, value=request.value
        )
        return {
            "message": f"Nested config {request.section}.{request.key} set to {request.value} for instance {instance_name}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/add/{instance_name}/pipelineConfig")
async def set_instance_nested_config1(instance_name: str, request: NestedConfigRequest):
    try:
        return {"message": f"pipeline configuration for{instance_name} inserted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/add/{instance_name}/schemaConfig")
async def set_instance_nested_config_2(
    instance_name: str, request: NestedConfigRequest
):
    try:
        return {"message": f"schema configuration for {instance_name} inserted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ------------------------------------------------------------------------------
# POST: update instance configuration
# ------------------------------------------------------------------------------


@app.post(
    "/instances/{instance_name}/config/keep_alive",
    tags=["update instance configuration"],
)
async def set_instance_keep_alive_config(
    instance_name: str, value: int = Query(description="keep_alive", alias="keep_alive")
):
    jsonTree = ["settings", "keep_alive"]
    return manager.update_instance_configuration(instance_name, jsonTree, value=value)


@app.post(
    "/instances/{instance_name}/config/qos", tags=["update instance configuration"]
)
async def set_instance_qos_config(
    instance_name: str, value: QosEnum = Query(description="qos", alias="qos")
):
    jsonTree = ["settings", "qos"]
    return manager.update_instance_configuration(instance_name, jsonTree, value=value)


@app.post(
    "/instances/{instance_name}/config/retain", tags=["update instance configuration"]
)
async def set_instance_retain_config(
    instance_name: str, value: bool = Query(description="retain", alias="retain")
):
    jsonTree = ["settings", "retain"]
    return manager.update_instance_configuration(instance_name, jsonTree, value=value)


@app.post(
    "/instances/{instance_name}/config/insecure", tags=["update instance configuration"]
)
async def set_instance_insecure_config(
    instance_name: str, value: bool = Query(description="insecure", alias="insecure")
):
    jsonTree = ["settings", "insecure"]

    return manager.update_instance_configuration(instance_name, jsonTree, value=value)


# Ensure the upload directory exists
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)
