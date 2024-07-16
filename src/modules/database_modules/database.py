# database.py
from pymongo import MongoClient


class MongoDBClient:
    _client = None

    @staticmethod
    def get_client():
        if MongoDBClient._client is None:
            MongoDBClient._client = MongoClient("mongodb://localhost:27017/")
        return MongoDBClient._client

    @staticmethod
    def get_template(collection_name):
        client = MongoDBClient.get_client()
        db = client.schema_config  # Assuming your database name is 'schema_config'
        collection = db[collection_name]
        template_doc = collection.find_one({})
        return template_doc
