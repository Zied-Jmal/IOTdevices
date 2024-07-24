from fastapi import FastAPI, Query, HTTPException
from typing import List, Dict, Any
from pydantic import BaseModel
from datetime import datetime, timedelta
import logging
from src.modules.database_modules.database import MongoDBClient
from pymongo import DESCENDING

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# MongoDB connection setup
client = MongoDBClient.get_client()


def get_collection(database: str, collection: str):
    db = client[database]
    return db[collection]


class Metadata(BaseModel):
    database: str
    device_name: str
    path: str
    type: str


class DataPoint(BaseModel):
    timestamp: datetime
    #metadata: Dict[str, Any]
    data: Dict[str, Any]


@app.get("/test_query")
async def test_query(
    database: str = Query(..., description="Name of the database"),
    collection: str = Query(..., description="Name of the collection"),
):
    coll = get_collection(database, collection)
    data = list(coll.find({}, {"timestamp": 1}).limit(5))
    logger.info(f"Test query returned: {data}")
    return data


@app.get("/data", response_model=List[DataPoint])
async def get_data(
    start: datetime = Query(
        ..., description="Start datetime in ISO format (e.g., 2024-07-19T12:00:00)"
    ),
    end: datetime = Query(
        ..., description="End datetime in ISO format (e.g., 2024-07-19T12:00:00)"
    ),
    period: int = Query(None, description="Period in minutes to sum data"),
    database: str = Query(..., description="Name of the database"),
    collection: str = Query(..., description="Name of the collection"),
):
    if start >= end:
        raise HTTPException(
            status_code=400, detail="Start datetime must be before end datetime"
        )

    coll = get_collection(database, collection)
    query = {"timestamp": {"$gte": start, "$lt": end}}
    data = list(coll.find(query))

    if not data:
        logger.info("No data found for the given interval.")
        return []

    if period:
        start_rounded = start.replace(
            minute=(start.minute // period) * period, second=0, microsecond=0
        )
        end_rounded = end.replace(
            minute=(end.minute // period) * period, second=0, microsecond=0
        ) + timedelta(minutes=period)
        period_data = []
        current_start = start_rounded

        while current_start < end_rounded:
            current_end = current_start + timedelta(minutes=period)
            period_sum = {}
            #metadata = None
            for item in data:
                if current_start <= item["timestamp"] < current_end:
                    #if not metadata:
                        #metadata = item["metadata"]
                    for key, value in item["data"].items():
                        if isinstance(value, (int, float)):
                            if key not in period_sum:
                                period_sum[key] = 0
                            period_sum[key] += value
            #if metadata:
            #print(f"period_sum : {period_sum}")
            if period_sum:
                period_data.append(
                    DataPoint(
                        timestamp=current_start, data=period_sum
                    )
                )#, metadata=metadata
            current_start = current_end

        return period_data

    return [
        DataPoint(
            timestamp=item["timestamp"], data=item["data"]
        )
        for item in data
    ]#, metadata=item["metadata"]


@app.get("/all_data", response_model=List[DataPoint])
async def get_all_data(
    database: str = Query(..., description="Name of the database"),
    collection: str = Query(..., description="Name of the collection"),
):
    coll = get_collection(database, collection)
    data = list(coll.find())
    return [
        DataPoint(
            timestamp=item["timestamp"], data=item["data"]
        )
        for item in data
    ]#, metadata=item["metadata"]


@app.get("/last_data", response_model=DataPoint)
async def get_last_data(
    database: str = Query(..., description="Name of the database"),
    collection: str = Query(..., description="Name of the collection"),
):
    coll = get_collection(database, collection)
    data = coll.find_one({}, sort=[("timestamp", DESCENDING)])
    if not data:
        raise HTTPException(status_code=404, detail="No data found")
    return DataPoint(
        timestamp=data["timestamp"], data=data["data"]
    )#metadata=data["metadata"], 


@app.get("/last_data_by_period", response_model=List[DataPoint])
async def get_last_data_by_period(
    period: int = Query(
        ..., description="Period in minutes to sum data, must be a positive integer"
    ),
    database: str = Query(..., description="Name of the database"),
    collection: str = Query(..., description="Name of the collection"),
):
    if period <= 0:
        raise HTTPException(status_code=400, detail="Period must be a positive integer")

    coll = get_collection(database, collection)
    last_data_point = coll.find_one({}, sort=[("timestamp", DESCENDING)])
    if not last_data_point:
        raise HTTPException(status_code=404, detail="No data found")

    end = last_data_point["timestamp"]
    end_rounded = end.replace(
        minute=(end.minute // period) * period, second=0, microsecond=0
    )
    start_rounded = end_rounded - timedelta(minutes=period)

    data = list(coll.find({"timestamp": {"$gte": start_rounded, "$lt": end_rounded}}))

    period_sum = {}
    for item in data:
        for key, value in item["data"].items():
            if isinstance(value, (int, float)):
                
                if key not in period_sum:
                    period_sum[key] = 0
                period_sum[key] += value
                #print(f"period_sum : {period_sum}")

    #print(f"period_sum : {period_sum}")

    return [
        DataPoint(
            timestamp=end_rounded, data=period_sum
        )
    ]#, metadata=last_data_point["metadata"]


@app.get("/databases", response_model=List[str])
async def list_databases():
    databases = client.list_database_names()
    return databases


@app.get("/collections", response_model=List[str])
async def list_collections(
    database: str = Query(..., description="Name of the database")
):
    if database not in client.list_database_names():
        raise HTTPException(status_code=404, detail="Database not found")
    collections = client[database].list_collection_names()
    return collections
