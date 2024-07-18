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
    "/instances/{instance_name}/config/username", tags=["update instance configuration"]
)
async def set_instance_username_config(
    instance_name: str, value: str = Query(description="username", alias="username")
):
    jsonTree = ["settings", "username"]
    return manager.update_instance_configuration(instance_name, jsonTree, value=value)


@app.post(
    "/instances/{instance_name}/config/password", tags=["update instance configuration"]
)
async def set_instance_password_config(
    instance_name: str, value: str = Query(description="password", alias="password")
):
    jsonTree = ["settings", "password"]
    return manager.update_instance_configuration(instance_name, jsonTree, value=value)


@app.post(
    "/instances/{instance_name}/config/ssl", tags=["update instance configuration"]
)
async def set_instance_ssl_config(
    instance_name: str, value: bool = Query(description="ssl", alias="ssl")
):
    jsonTree = ["settings", "ssl"]
    return manager.update_instance_configuration(instance_name, jsonTree, value=value)


@app.post("/instances/{instance_name}/config/upload", tags=["update instance configuration"])
async def upload_files(instance_name: str,
    ca_certs: UploadFile = File(None),
    certfile: UploadFile = File(None),
    keyfile: UploadFile = File(None),
):
    UPLOAD_DIR = f"certs/{instance_name}"
    if not os.path.exists(UPLOAD_DIR):
        os.makedirs(UPLOAD_DIR)
    file_paths = {
        "ca_certs": None,
        "certfile": None,
        "keyfile": None,
    }

    if ca_certs:
        ca_certs_path = os.path.join(UPLOAD_DIR, ca_certs.filename)
        with open(ca_certs_path, "wb") as buffer:
            shutil.copyfileobj(ca_certs.file, buffer)
        file_paths["ca_certs"] = ca_certs_path
        print("file uploaded: ", ca_certs_path)
        value = file_paths["ca_certs"]
        jsonTree = ["settings", "ca_certs"]
        return manager.update_instance_configuration(instance_name, jsonTree, value=value)
    if certfile:
        certfile_path = os.path.join(UPLOAD_DIR, certfile.filename)
        with open(certfile_path, "wb") as buffer:
            shutil.copyfileobj(certfile.file, buffer)
        file_paths["certfile"] = certfile_path

    if keyfile:
        keyfile_path = os.path.join(UPLOAD_DIR, keyfile.filename)
        with open(keyfile_path, "wb") as buffer:
            shutil.copyfileobj(keyfile.file, buffer)
        file_paths["keyfile"] = keyfile_path

    # Here you would update your update configuration with the new file paths
    # For example:
    # config["ca_certs"] = file_paths["ca_certs"]
    # config["certfile"] = file_paths["certfile"]
    # config["keyfile"] = file_paths["keyfile"]

    return {"file_paths": file_paths}


@app.post(
    "/instances/{instance_name}/config/insecure", tags=["update instance configuration"]
)
async def set_instance_insecure_config(
    instance_name: str, value: bool = Query(description="insecure", alias="insecure")
):
    jsonTree = ["settings", "insecure"]

    return manager.update_instance_configuration(instance_name, jsonTree, value=value)


@app.post(
    "/instances/{instance_name}/config/auth", tags=["update instance configuration"]
)
async def set_instance_auth_config(
    instance_name: str, value: bool = Query(description="auth", alias="auth")
):
    jsonTree = ["settings", "auth"]

    return manager.update_instance_configuration(instance_name, jsonTree, value=value)
