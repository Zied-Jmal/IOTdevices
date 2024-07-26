import json
import os
from fastapi import FastAPI, HTTPException, File, UploadFile
import shutil
from fastapi import Query
from enum import Enum
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from fastapi.responses import JSONResponse

from src.managers.mqtt_manager.MQTTManager import ManagerMQTT

# ------------------------------------------------------------------------------
# FastAPI Setup
# ------------------------------------------------------------------------------
app = FastAPI()

manager = ManagerMQTT()


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

class AutostartRequest(BaseModel):
    autostart: bool
    
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


@app.get("/config/{instance_name}/{path_key}", tags=["get configuration generic"])
async def get_config(
    instance_name: str,
    path_key: str,
    sub_key: str = Query(None, description="Optional sub-key for nested configurations")
):
    """
    Get configuration value for a given instance and path key.
    
    - `instance_name`: Name of the instance.
    - `path_key`: The primary key for the configuration.
    - `sub_key`: Optional nested key for more specific configurations.
    """
    print("get config")
    jsonTree = [path_key]
    if sub_key:
        jsonTree.append(sub_key)

    try:
        instance = manager.get_instance(instance_name)
        if not instance:
            raise HTTPException(
                status_code=404, detail=f"Instance {instance_name} not found"
            )
        value = manager.get_nested_config_value_using_jsonTree_HTTP(instance, jsonTree)

        response_data = {"instance_name": instance_name, "path_key": path_key, "sub_key": sub_key, "value": value}
        return response_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




@app.get("/config3/{instance_name}", tags=["get configuration generic"])
async def get_config3(
    instance_name: str,
    path_key: Optional[str] = None,
    sub_key: Optional[str] = Query(None, description="Optional sub-key for nested configurations")
):
    """
    Get configuration value for a given instance and path key.
    
    - `instance_name`: Name of the instance.
    - `path_key`: The primary key for the configuration (optional).
    - `sub_key`: Optional nested key for more specific configurations.
    
    If `path_key` is not provided, returns all configuration data for the instance.
    """
    print("get config")
    jsonTree = []
    if path_key:
        jsonTree.append(path_key)
        if sub_key:
            jsonTree.append(sub_key)

    try:
        instance = manager.get_instance(instance_name)
        if not instance:
            raise HTTPException(
                status_code=404, detail=f"Instance {instance_name} not found"
            )
        
        if jsonTree:
            value = manager.get_nested_config_value_using_jsonTree_HTTP(instance, jsonTree)
        else:
            # Retrieve all configuration data if no path_key is provided
            value = manager.get_all_config_data(instance)
            value["_id"] = str(value["_id"])
        response_data = {"instance_name": instance_name, "path_key": path_key, "sub_key": sub_key, "value": value}
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


@app.post("/instances/autostart/{instance_name}", tags=["MQTT instance"])
async def set_instance_autostart(instance_name: str, value:bool):
    jsonTree = ["autostart"]
    return manager.update_instance_configuration(instance_name, jsonTree, value=value)

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
    "/instances/{instance_name}/config/broker_address",
    tags=["update instance configuration"],
)
async def set_instance_broker_address_config(
    instance_name: str, value: str = Query(description="broker_address", alias="broker_address")
):
    jsonTree = ["broker_address"]
    return manager.update_instance_configuration(instance_name, jsonTree, value=value)

@app.post(
    "/instances/{instance_name}/config/port",
    tags=["update instance configuration"],
)
async def set_instance_port_config(
    instance_name: str, value: int = Query(description="port", alias="port")
):
    jsonTree = ["port"]
    return manager.update_instance_configuration(instance_name, jsonTree, value=value)


@app.post(
    "/instances/{instance_name}/config/client_id",
    tags=["update instance configuration"],
)
async def set_instance_client_id_config(
    instance_name: str, value: int = Query(description="client_id", alias="client_id")
):
    jsonTree = ["client_id"]
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


@app.post(
    "/instances/{instance_name}/config/upload", tags=["update instance configuration"]
)
async def upload_files(
    instance_name: str,
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
        return manager.update_instance_configuration(
            instance_name, jsonTree, value=value
        )
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


# ------------------------------------------------------------------------------
# POST: update schemas configuration
# ------------------------------------------------------------------------------
client = manager.client
db = client["schemas_config"]


# Pydantic models
class SubSchema(BaseModel):
    collection: str
    devices: List[str]
    topics: List[str]
    path_mapping: Dict[str, str]
    data_mapping: Dict[str, str]


class Schema(BaseModel):
    database: str
    sub_schemas: List[SubSchema]


class SchemaUpdate(BaseModel):
    database: Optional[str]
    sub_schemas: Optional[List[SubSchema]]


# CRUD operations
@app.post("/schemas/", response_model=Dict[str, Any],tags=["Schemas"])
def create_schema(schema: Schema):
    if db["schemas"].find_one({"database": schema.database}):
        raise HTTPException(
            status_code=400, detail="Schema with this database already exists"
        )
    result = db["schemas"].insert_one(schema.model_dump())
    print("222222")
    return {"inserted_id": str(result.inserted_id)}


@app.get("/schemas/", response_model=List[Dict[str, Any]],tags=["Schemas"])
def read_schemas():
    schemas = list(db["schemas"].find())
    for schema in schemas:
        schema["_id"] = str(schema["_id"])
    return schemas


@app.get("/schemas/{database}", response_model=Dict[str, Any],tags=["Schemas"])
def read_schema(database: str):
    schema = db["schemas"].find_one({"database": database})
    if schema:
        schema["_id"] = str(schema["_id"])
        return schema
    raise HTTPException(status_code=404, detail="Schema not found")


@app.put("/schemas/{database}", response_model=Dict[str, Any],tags=["Schemas"])
def update_schema(database: str, schema_update: SchemaUpdate):
    update_data = {k: v for k, v in schema_update.model_dump().items() if v is not None}

    if update_data:
        result = db["schemas"].update_one({"database": database}, {"$set": update_data})

        if result.matched_count == 1:
            # Fetch the updated document
            updated_schema = db["schemas"].find_one({"database": database})

            # Convert ObjectId to string
            if updated_schema and "_id" in updated_schema:
                updated_schema["_id"] = str(updated_schema["_id"])

            # Return the updated schema as a JSON response
            return JSONResponse(content=updated_schema)
    raise HTTPException(status_code=404, detail="Schema not found or no changes made")


@app.delete("/schemas/{database}", response_model=Dict[str, Any],tags=["Schemas"])
def delete_schema(database: str):
    result = db["schemas"].delete_one({"database": database})
    if result.deleted_count == 1:
        return {"deleted_database": database}
    raise HTTPException(status_code=404, detail="Schema not found")





# Endpoint to add a topic to an instance
@app.post("/instances/{instance_name}/topics", tags=["MQTT Topics"])
async def add_topic(instance_name: str, topic: str):
    try:
        response = manager.add_topic(instance_name, topic)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to delete a topic from an instance
@app.delete("/instances/{instance_name}/topics", tags=["MQTT Topics"])
async def delete_topic(instance_name: str, topic: str):
    try:
        response = manager.delete_topic(instance_name, topic)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to list topics of an instance
@app.get("/instances/{instance_name}/topics", tags=["MQTT Topics"])
async def list_topics(instance_name: str):
    try:
        response = manager.list_topics(instance_name)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/instances/{instance_name}/config/periodic_message", tags=["update instance configuration"])
async def set_periodic_message_config(
    instance_name: str,
    period: int = Query(description="Time period in seconds"),
    topic: str = Query(description="Topic for periodic message"),
    message: str = Query(description="Message to be sent periodically"),
    active: bool = Query(description="Status to determine if periodic message sending is active")
):
    jsonTree = ["periodic_message"]
    value = {
        "period": period,
        "topic": topic,
        "message": message,
        "active": active
    }
    return manager.update_instance_configuration(instance_name, jsonTree, value=value)
