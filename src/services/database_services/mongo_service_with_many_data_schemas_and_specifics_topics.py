from pymongo.errors import CollectionInvalid
import paho.mqtt.client as mqtt
from datetime import datetime
import json
import re
from src.modules.database_modules.database import (
    MongoDBClient,
)  # Adjust import based on your project structure


class DataWriterService:
    def __init__(self):
        self.client = MongoDBClient.get_client()

        # Load schemas from MongoDB
        self.schemas_cursor = self.client["schemas_config4"]["schemas"].find()
        self.schemas = list(
            self.schemas_cursor
        )  # Convert cursor to list for easier access

    def create_time_series_collection(
        self, db_name, collection_name, time_field, meta_field, granularity="seconds"
    ):
        db = self.client[db_name]

        # Check if the collection already exists
        if collection_name in db.list_collection_names():
            print(
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
            print(
                f"Created time-series collection '{collection_name}' in database '{db_name}'."
            )
        except Exception as e:
            print(
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

    def write_data(self, topic, message):
        self.schemas_cursor = self.client["schemas_config4"]["schemas"].find()
        self.schemas = list(
            self.schemas_cursor
        )  
        try:
            topic_parts = topic.split("/")
            database_name = topic_parts[0]
            device_name = topic_parts[1]

            # Find the schema for the database_name (formerly database)
            schema = next(
                (s for s in self.schemas if s["database"] == database_name), None
            )
            if not schema:
                return

            collection_names = schema["sub_schemas"]
            collection_name = None
            path_mapping = None
            data_mapping = None

            for sub in collection_names:
                if device_name in sub["devices"]:
                    print(f"""{device_name} in {sub["devices"]}""")
                    for topic_pattern in sub["topics"]:
                        if mqtt.topic_matches_sub(topic_pattern, topic):
                            collection_name = sub["collection"]
                            path_mapping = sub.get("path_mapping", {})
                            data_mapping = sub.get("data_mapping", {})
                            break
                    if collection_name:
                        break

            if not collection_name:
                return
            print("collection_name : ",collection_name)
            raw_data = json.loads(message)
            print(f"Raw data: {raw_data}")
            specific_collection_name = ""
            transformed_data = {}
            for key, path_expr in data_mapping.items():
                print(f"Evaluating path expression: {path_expr}")
                print(f"Key: {key}")
                print(f"transformed_data: {transformed_data}")
                print(f"data_mapping: {data_mapping}")
                if key in path_mapping:
                    path_name = self.evaluate_expression(raw_data, path_mapping[key])
                    specific_collection_name = (
                        f"{collection_name}|{device_name}|{path_name}"
                    )
                    print(f"Specific collection name: {specific_collection_name}")
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
                    path_name = self.evaluate_expression(raw_data, path_mapping["subpath"])
                    specific_collection_name = (
                        f"{collection_name}|{device_name}|{path_name}"
                    )
                    print(f"Specific collection name: {specific_collection_name}")
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
                    print(f"No path mapping found for '{key}'. Skipping...")

            if specific_collection_name == "":
                specific_collection_name = f"{collection_name}|{device_name}"
                print("specific_collection_name:", specific_collection_name)
                self.create_time_series_collection(
                    database_name, specific_collection_name, "timestamp", "metadata"
                )
            metadata = {
                "database": database_name,
                "collection": collection_name,
                "device_name": device_name,
                "path": topic,
            }
            document = {
                "timestamp": datetime.utcnow(),
                "metadata": metadata,
                "data": transformed_data,
            }
            print(f"Metadata: {metadata}")
            print(f"Document to insert: {document}")
            self.client[database_name][specific_collection_name].insert_one(document)
            print(
                f"Inserted document into {database_name}/{collection_name}/{device_name}: {document}"
            )

        except Exception as e:
            print(f"Error processing message: {e}")

