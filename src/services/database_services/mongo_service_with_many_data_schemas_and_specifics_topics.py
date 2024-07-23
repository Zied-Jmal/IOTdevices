from pymongo.errors import CollectionInvalid
import paho.mqtt.client as mqtt
from datetime import datetime
import json
import re
from src.modules.database_modules.database import (
    MongoDBClient,
)  # Adjust import based on your project structure
import logging

# Suppress debug messages from MongoDB driver
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataWriterService:
    def __init__(self):
        self.client = MongoDBClient.get_client()

        # Load schemas from MongoDB
        self.schemas_cursor = self.client["schemas_config"]["schemas"].find()
        self.schemas = list(
            self.schemas_cursor
        )  # Convert cursor to list for easier access

    def create_time_series_collection(
        self, db_name, collection_name, time_field, meta_field, granularity="seconds"
    ):
        db = self.client[db_name]

        # Check if the collection already exists
        if collection_name in db.list_collection_names():
            logger.info(
                f"Collection '{collection_name}' already exists in database '{db_name}'. Skipping creation."
            )
            return

        try:
            db.create_collection(
                collection_name,
                timeseries={
                    "timeField": time_field,
                    "metaField": meta_field,
                    "granularity": granularity,
                },
            )
            logger.info(
                f"Created time-series collection '{collection_name}' in database '{db_name}'."
            )
        except Exception as e:
            logger.info(
                f"Failed to create collection '{collection_name}' in database '{db_name}': {e}"
            )

    def get_value(self, data, path):
        keys = re.split(r"\.|\[|\]\[|\]", path)
        keys = [key for key in keys if key]  # Remove empty strings
        for key in keys:
            if key.isdigit():
                key = int(key)
            data = data[key]
        return data

    def evaluate_expression(self, data, expression):
        path_expr = self.parse_expression(expression)

        # Evaluate path expression to get data
        return self.get_value(data, path_expr)

    def parse_expression(self, expression):
        return expression.strip()

    def transform(self, data, template):
        if isinstance(template, dict):
            transformed_dict = {}
            for key, value in template.items():
                if isinstance(value, str) and value.startswith("_id"):
                    continue  # Skip ObjectId field
                transformed_dict[key] = self.transform(data, value)
            return transformed_dict
        elif isinstance(template, list):
            return [self.transform(data, item) for item in template]
        elif isinstance(template, str):
            return self.evaluate_expression(data, template)
        else:
            return template
    def save_metadata(self, db_name: str, collection_name: str, metadata: dict):
        try:
            metadata_collection = self.client["metadata"][db_name]
            logger.info(f"metadata collection: {metadata_collection}")
            # Define the query to find an existing document by collection_name
            query = {"collection_name": collection_name}
            logger.info(f"query: {query}")
            # Define the update to apply to the found document
            update = {
                "$set": {
                    "metadata": metadata,
                }
            }
            # Update the document if it exists, otherwise insert a new one
            result = metadata_collection.find_one_and_update(
                query,
                update,
                upsert=True  # Create a new document if none exists
            )
            
            if result.matched_count:
                logger.info(f"Updated metadata for collection '{collection_name}' in {db_name}/metadata.")
            else:
                logger.info(f"Inserted new metadata for collection '{collection_name}' in {db_name}/metadata.")
            
        except Exception as e:
            logger.error(f"Failed to save or update metadata: {e}")
    def write_data(self, topic, message):
        self.schemas_cursor = self.client["schemas_config"]["schemas"].find()
        self.schemas = list(self.schemas_cursor)
        try:
            topic_parts = topic.split("/")
            # database_name = topic_parts[0]
            # device_name = topic_parts[1]

            # # Find the schema for the database_name (formerly database)
            # schema = next(
            #     (s for s in self.schemas if s["database"] == database_name), None
            # )
            # if not schema:
            #     return
            actual_schema = None
            # collection_names = schema["sub_schemas"]
            found_the_device = False
            schemas = self.schemas
            for topic_part in topic_parts:
                for schema in schemas:
                    for sub_schema in schema["sub_schemas"]:
                        if topic_part in sub_schema["devices"]:
                            # logger.info("sub_schema : ",sub_schema)
                            # logger.info(f"""sub_schema["devices"] :{sub_schema["devices"]}""")
                            # logger.info("topic_part",topic_part)
                            device_name = topic_part
                            database_name = topic_part
                            actual_schema = sub_schema
                            found_the_device = True
                            logger.info("OKKKKKKKK")
                            break
                    if found_the_device:
                        break
                if found_the_device:
                    break
            if not found_the_device:
                return
            if not actual_schema:
                return
            collection_name = None
            path_mapping = None
            data_mapping = None
            logger.info("device_name:", device_name)
            logger.info("actual_schema", actual_schema)
            logger.info(f"""this device {device_name} in {actual_schema["devices"]}""")
            for topic_pattern in actual_schema["topics"]:
                logger.info(f"topic asked for {topic_pattern} and topic received {topic}")
                if mqtt.topic_matches_sub(topic_pattern, topic):
                    collection_name = actual_schema["collection"]
                    path_mapping = actual_schema.get("path_mapping", {})
                    data_mapping = actual_schema.get("data_mapping", {})
                    break

            if not collection_name:
                return
            logger.info("collection_name : ", collection_name)
            raw_data = json.loads(message)
            logger.info(f"Raw data: {raw_data}")
            specific_collection_name = ""
            transformed_data = {}
            for key, path_expr in data_mapping.items():
                logger.info(f"Evaluating path expression: {path_expr}")
                logger.info(f"Key: {key}")
                logger.info(f"transformed_data: {transformed_data}")
                logger.info(f"data_mapping: {data_mapping}")
                if key in path_mapping:
                    path_name = self.evaluate_expression(raw_data, path_mapping[key])
                    specific_collection_name = (
                        f"{path_name}"
                    )
                    logger.info(f"Specific collection name: {specific_collection_name}")
                    if (
                        specific_collection_name
                        not in self.client[database_name].list_collection_names()
                    ):
                        self.create_time_series_collection(
                            database_name,
                            specific_collection_name,
                            "timestamp",
                            "metadata",
                        )
                    transformed_data[key] = self.evaluate_expression(
                        raw_data, path_expr
                    )
                elif "NOsubpath" in path_mapping:
                    transformed_data[key] = self.evaluate_expression(
                        raw_data, path_expr
                    )
                elif "subpath" in path_mapping:
                    path_name = self.evaluate_expression(
                        raw_data, path_mapping["subpath"]
                    )
                    specific_collection_name = f"{path_name}"
                    logger.info(f"Specific collection name: {specific_collection_name}")
                    if (
                        specific_collection_name
                        not in self.client[database_name].list_collection_names()
                    ):
                        self.create_time_series_collection(
                            database_name,
                            specific_collection_name,
                            "timestamp",
                            "metadata",
                        )
                    transformed_data[key] = self.evaluate_expression(
                        raw_data, path_expr
                    )
                else:
                    logger.info(f"No path mapping found for '{key}'. Skipping...")

            if specific_collection_name == "":
                specific_collection_name = f"{device_name}"
                logger.info("specific_collection_name:", specific_collection_name)
                self.create_time_series_collection(
                    database_name, specific_collection_name, "timestamp", "metadata"
                )
            logger.info("specific_collection_name")
            metadata = {
                "database": database_name,
                "type": collection_name,
                "collection_name": specific_collection_name,
                "device_name": device_name,
                "path": topic,
            }
            document = {
                "timestamp": datetime.now(),
                "metadata": metadata,
                "data": transformed_data,
            }
            logger.info(f"Metadata: {metadata}")
            logger.info(f"Document to insert: {document}")
            self.client[database_name][specific_collection_name].insert_one(document)
            logger.info(f"Inserted document into {database_name}/{specific_collection_name}: {document}")
            # Save metadata to the metadata database
            logger.info(f"Saving metadata to {database_name}")
            self.save_metadata(db_name=database_name,collection_name=specific_collection_name, metadata=metadata)

        except Exception as e:
            logger.info(f"Error processing message: {e}")
