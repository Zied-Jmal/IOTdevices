
from pymongo.errors import CollectionInvalid
import paho.mqtt.client as mqtt
from datetime import datetime
import json
from src.modules.database_modules.database import MongoDBClient  # Adjust import based on your project structure

class data_writer_service:
    def __init__(self):
        self.client = MongoDBClient.get_client()

        # Load schemas from MongoDB
        self.schemas_cursor = self.client["schemas_config"]["schemas"].find()
        
        self.schemas = list(self.schemas_cursor)  # Convert cursor to list for easier access
        # Function to create time-series collections in respective databases, if not already created
    def create_time_series_collection(self,db_name, collection_name, time_field, meta_field, granularity='seconds'):
        db = self.client[db_name]
        
        # Check if the collection already exists
        if collection_name in db.list_collection_names():
            print(f"Collection '{collection_name}' already exists in database '{db_name}'. Skipping creation.")
            return
        
        try:
            db.create_collection(collection_name, timeseries={
                'timeField': time_field,
                'metaField': meta_field,
                'granularity': granularity
            })
            print(f"Created time-series collection '{collection_name}' in database '{db_name}'.")
        except CollectionInvalid as e:
            print(f"Failed to create collection '{collection_name}' in database '{db_name}': {e}")

    def write_data(self,topic,message):
        try:
            topic_parts = topic.split("/")
            database_name = topic_parts[0]
            device_name = topic_parts[1]

            # Find the schema for the database_name (formerly database)
            schema = next((s for s in self.schemas if s['database'] == database_name), None)
            if not schema:
                #print(f"Unknown collection for topic: {topic}")
                return

            collection_names = schema['sub_schemas']
            collection_name = None

            for sub in collection_names:
                if device_name in sub['devices']:
                    for topic_pattern in sub['topics']:
                        if mqtt.topic_matches_sub(topic_pattern, topic):
                            collection_name = sub['collection']
                            break
                    if collection_name:
                        break

            if not collection_name:
                #print(f"No matching sub-schema for device: {device_name} with topic: {msg.topic}")
                return

            metadata = {"collection": collection_name,"device_name": device_name }

            document = {
                "timestamp": datetime.utcnow(),
                "metadata": metadata,
                "data": json.loads(message),
            }
            print("database_name:", database_name)
            print("collection_name:", collection_name)
            print("document:", document)
            specific_collection_name=f"{collection_name}|{device_name}"
            print("specific_collection_name:", specific_collection_name)
            self.create_time_series_collection(database_name, specific_collection_name, "timestamp", "metadata")
            self.client[database_name][specific_collection_name].insert_one(document)
            print(f"Inserted document into {database_name}/{collection_name}/{device_name}: {document}")

        except Exception as e:
            print(f"Error processing message: {e}")




