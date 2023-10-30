import os

import pymongo
from dotenv import load_dotenv

load_dotenv()

# MongoDB configuration
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE')
DATABASE_NAME = 'the-data-explorer-db'


def create_dataset(file_dataset):
    """
    Connect to the MongoDB and insert a dataset object.
    """
    try:
        client = pymongo.MongoClient(
            MONGO_HOST,
            username=MONGO_USERNAME,
            password=MONGO_PASSWORD,
            authSource=MONGO_AUTH_SOURCE
        )
        db = client[DATABASE_NAME]
        dataset_collection = db['Dataset']
        inserted_data = dataset_collection.insert_one(file_dataset)
        client.close()
        return inserted_data.inserted_id
    except Exception:
        return None
