# database.py
import os
from pymongo import MongoClient


class MongoDBClient:
    _client = None

    @staticmethod
    def get_client():
        if MongoDBClient._client is None:
            mongo_url = os.getenv("MONGO_URL", "mongodb://localhost:27018/")
            MongoDBClient._client = MongoClient(mongo_url)
        return MongoDBClient._client

    @staticmethod
    def get_template(collection_name):
        client = MongoDBClient.get_client()
        db = client.schema_config  # Assuming your database name is 'schema_config'
        collection = db[collection_name]
        template_doc = collection.find_one({})
        return template_doc
