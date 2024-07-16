# main.py
from fastapi import FastAPI, Depends
from typing import List, Dict
import os
import sys
from database import MongoDBClient
from bson import ObjectId
from fastapi import FastAPI, HTTPException, File, UploadFile
from datetime import datetime


app = FastAPI()

async def get_db():
    client = MongoDBClient.get_client()
    return client


@app.get("/")
async def read_root():
    return {"message": "Welcome to the FastAPI MongoDB application"}


@app.get("/databases")
async def list_databases(db=Depends(get_db)) -> List[str]:
    return db.list_database_names()


@app.get("/databases/{database_name}/collections")
async def list_collections(database_name: str, db=Depends(get_db)) -> List[str]:
    database = db[database_name]
    return database.list_collection_names()


@app.post(
    "/databases/{database_name}/collections/{collection_name}",
    response_model=List[Dict],
)
async def get_collection_data(
    database_name: str, collection_name: str, db=Depends(get_db)
) -> List[Dict]:
    try:
        database = db[database_name]
        collection = database[collection_name]

        documents = []
        cursor = collection.find()
        i = 0
        for doc in cursor:
            i += 1
            print("counter ==>", i)
            print("doc", doc)
            # Convert ObjectId instances to strings
            doc["_id"] = str(doc["_id"])
            # Handle only the first occurrence of _id
            for key, value in doc.items():
                if isinstance(value, datetime):
                    doc[key] = value.isoformat()
            # Convert ObjectId instances to strings for nested _id fields
            if "rpc" in doc and "_id" in doc["rpc"]:
                doc["rpc"]["_id"] = str(doc["rpc"]["_id"])
            documents.append(doc)

        response = documents
        print("response", response)
        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# @app.get("/databases/{database_name}/collections/{collection_name}/id/{id}")
@app.get("/databases/{database_name}/collections/{collection_name}/search")
async def search_collection_data(
    database_name: str,
    collection_name: str,
    field_name: str,
    query: str,
    db=Depends(get_db),
) -> List[Dict]:
    database = db[database_name]
    collection = database[collection_name]
    regex_query = {field_name: {"$regex": f".*{query}.*", "$options": "i"}}
    documents = collection.find(regex_query).to_list(None)
    # Convert ObjectId instances to strings
    for doc in documents:
        doc["_id"] = str(doc["_id"])
    return documents


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
