import json
import logging
import os
import shutil

import pymongo
from dotenv import load_dotenv

load_dotenv()

# MongoDB configuration
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE')
DATABASE_NAME = 'the-data-explorer-db'

logger = logging.getLogger(__name__)
load_dotenv()



def setup_parsed_loc():
    """
    Set up the location for parsed datasets.
    """
    logger.debug("Setting up parsed location")
    try:
        return os.environ['DATA_EXPLORER_SSR']
    except Exception:
        return "./"

DF_LOC=setup_parsed_loc()


def backup():
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

        # Datasets
        dataset_collection = db['Dataset']
        datasets = dataset_collection.find({"public": True})

        # Making sure the directory exists
        os.makedirs(os.path.dirname("./staging/prepopulate-data/parsed-data-files/"), exist_ok=True)
        os.makedirs(os.path.dirname("./staging/prepopulate-data/sample-data-files/"), exist_ok=True)

        for dataset in datasets:
            parsed_df = f"{DF_LOC}parsed-data-files/{dataset['_id']}.json"
            sample_df = f"{DF_LOC}sample-data-files/{dataset['_id']}.json"
            new_parsed_df = f"./staging/prepopulate-data/parsed-data-files/{dataset['_id']}.json"
            new_sample_df = f"./staging/prepopulate-data/sample-data-files/{dataset['_id']}.json"
            # duplicate the parsed files if they exist
            if os.path.exists(parsed_df):
                shutil.copy(parsed_df, new_parsed_df)
            if os.path.exists(sample_df):
                shutil.copy(sample_df, new_sample_df)
    except Exception as e:
        print(str(e))
        return None

if __name__ == "__main__":
    backup()
