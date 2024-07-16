from src.modules.database_modules.database import MongoDBClient  # Adjust import based on your project structure
import json
from datetime import datetime, timedelta
from pymongo import UpdateOne, ASCENDING
import re

class DataWriter:
    def __init__(self):
        self.client = MongoDBClient.get_client()
        self.db = self.client["my_databaseEM"]
        self.collection_cache = {}
    def get_value(self,data, path):
        keys = re.split(r'\.|\[|\]\[|\]', path)
        keys = [key for key in keys if key]  # Remove empty strings
        for key in keys:
            if key.isdigit():
                key = int(key)
            data = data[key]
        return data
    
    def load_template_from_mongodb(self,collection_name):
        try:
            client = self.client
            db = client.schema_config  # Assuming your database name is 'schema_config'
            collection = db[collection_name]
            template_doc = collection.find_one({})
            if template_doc:
                return template_doc.get('template')
            else:
                raise Exception(f"No template found in collection '{collection_name}'.")
        except Exception as e:
            print(f"Error loading template from MongoDB: {str(e)}")
            return None

# Function to transform data based on the template
    def transform(self,data, template):
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
            return self.get_value(data, template)
        else:
            return template

    def get_collection(self, device_id):
        if device_id not in self.collection_cache:
            collection_name = f"device_{device_id}"
            collection = self.db.get_collection(collection_name)
            # Ensure the collection has an index on the timestamp field
            collection.create_index([("timestamp", ASCENDING)])
            self.collection_cache[device_id] = collection
        return self.collection_cache[device_id]

    def process_message(self, topic, message):
        template=self.load_template_from_mongodb('shellyproem-50')
        print(f"Processing message from topic: {topic}")
        print(f"Message: {message}")

        # Parse MQTT topic to extract device_id and data type
        parts = topic.split("/")
        if len(parts) < 3:
            print(f"Invalid topic format: {topic}")
            return

        device_id = parts[
            1
        ]  # Assuming second part is device_id (e.g., shellyem3-<deviceid>)
        data_category = parts[2]  # emeter or relay
        data_type = "/".join(parts[3:])  # Rest of the topic determines data type

        # Convert message to appropriate data type based on schema
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            print(f"Failed to decode JSON message: {message}")
            return

        # Prepare update structure
        update_data = {}
        # Add timestamp to the update
        update_data["timestamp"] = datetime.utcnow()
        update_data["device_id"] = device_id
        print("payload: ", payload)

        if data_category == "emeter" and len(parts) > 3 and parts[3].isdigit():
            phase = parts[3]
            sub_type = "/".join(parts[4:])
            update_data[f"data.emeter.{phase}.{sub_type}"] = payload
            print("update_data: ", update_data)
            # update_data["data"]=payload
            print(
                f"Updating emeter data for phase {phase} with sub_type {sub_type} with payload: {payload}"
            )
        elif data_category == "relay" and len(parts) > 3:
            relay = parts[3]
            sub_type = "/".join(parts[4:])
            update_data[f"data.relay.{relay}.{sub_type}"] = payload
        
        elif payload["method"] == "NotifyEvent":
            transformed_data = self.transform(payload, template)
            print(f"Unrecognized data category: {data_category}")
            update_data[data_category] = transformed_data
        elif payload["method"] == "NotifyEvent2":
            print(f"Unrecognized data category: {data_category}")
            update_data[data_category] = payload
        # Insert or update document in collection based on device_id (dynamic creation)
        collection = self.get_collection(device_id)
        self.insert_or_update_document(collection, device_id, update_data)

    def insert_or_update_document(self, collection, device_id, update_data):
        # Define the time window as the current minute
        current_time = update_data["timestamp"]
        start_time = current_time.replace(second=0, microsecond=0)
        end_time = start_time + timedelta(minutes=1)

        # Find an existing document within the current minute
        existing_document = collection.find_one(
            {"timestamp": {"$gte": start_time, "$lt": end_time}, "device_id": device_id}
        )

        if existing_document:
            # Update the existing document with the new data
            collection.update_one(
                {"_id": existing_document["_id"]}, {"$set": update_data}
            )
            print(f"Updated existing document: {existing_document['_id']}")
        else:
            # Insert a new document
            collection.insert_one(update_data)
            print(f"Inserted new document for device_id: {device_id}")

    def write_data(self, topic, message):
        self.process_message(topic, message)
