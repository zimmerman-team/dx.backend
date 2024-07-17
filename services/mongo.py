import logging
import os

import pymongo
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# MongoDB configuration
MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE')
DATABASE_NAME = 'the-data-explorer-db'
LOCAL_DEV = "localhost:27017"
DEV = False  # Flag for dev testing with local MongoDB, should be False when committed.
FS_INDEX_DB = "FederatedSearchIndex"


def mongo_client(dev=False):
    client = pymongo.MongoClient(LOCAL_DEV)
    if not dev:
        client = pymongo.MongoClient(
            MONGO_HOST,
            username=MONGO_USERNAME,
            password=MONGO_PASSWORD,
            authSource=MONGO_AUTH_SOURCE
        )
    return client


def mongo_create_external_source(external_source, update=False):
    """
    Connect to the MongoDB and insert an external source object.

    :param external_source: The external source object to insert.
    :param update: A boolean indicating if the object should be updated instead of inserted.
    :return: The inserted object ID or None if the operation failed.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        external_source_collection = db[FS_INDEX_DB]
        if update:
            inserted_data = external_source_collection.update_one(
                {"_id": external_source["_id"]},
                {"$set": external_source}
            )
            client.close()
            return inserted_data.modified_count
        else:
            inserted_data = external_source_collection.insert_one(external_source)
            client.close()
            return inserted_data.inserted_id
    except Exception as e:
        logger.error("Error in create_external_source: " + str(e))
        return None


def mongo_get_all_external_sources():
    """
    Connect to the MongoDB and get all external source objects.

    :return: A list of external source objects.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        external_source_collection = db[FS_INDEX_DB]
        external_sources = list(external_source_collection.find())
        client.close()
        return external_sources
    except Exception as e:
        logger.error("Error in get_all_external_sources: " + str(e))
        return []


def mongo_find_external_sources_by_text(query, limit=None, offset=0, sources=None, sort_by=None):
    """
    Connect to the MongoDB and find external source objects by title or description.

    :param query: The query to search for.
    :param limit: The maximum number of results to return.
    :param offset: The offset to start the search from.
    :param sources: A list of sources to filter by.
    :return: A list of external source objects.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        external_source_collection = db[FS_INDEX_DB]

        # Construct query to include text search and source filtering if sources are provided
        mongo_query = {"$text": {"$search": f"{query}"}}
        if sources:
            mongo_query["source"] = {"$in": sources}

        sort_style = [("score", {"$meta": "textScore"})]
        if sort_by == "updatedDate":
            sort_style = [("dateLastUpdated", -1)]
        if sort_by == "createdDate":
            sort_style = [("datePublished", -1)]
        if sort_by == "name":
            sort_style = [("title", 1)]

        external_sources = list(
            external_source_collection.find(
                mongo_query,
                {"score": {"$meta": "textScore"}}
            )
            .sort(sort_style)
            .skip(offset)
            .limit(limit)
        )
        client.close()
        return external_sources
    except Exception as e:
        logger.error("Error in find_external_sources_by_text: " + str(e))
        return None


def mongo_get_external_source_by_source(sources, limit=None, offset=0, sort_by=None):
    """
    Connect to the MongoDB and get external source objects by source.

    :param limit: The maximum number of results to return.
    :param offset: The offset to start the search from.
    :param sources: A list of sources to filter by.
    :return: A list of external source objects.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        external_source_collection = db[FS_INDEX_DB]

        sort_style = [("title", 1)]
        if sort_by == "name":
            sort_style = [("title", 1)]
        if sort_by == "updatedDate":
            sort_style = [("dateLastUpdated", -1)]
        if sort_by == "createdDate":
            sort_style = [("datePublished", -1)]

        external_sources = list(
            external_source_collection.find(
                {"source": {"$in": sources}}
            )
            # sort on alphabetical order
            .sort(sort_style)
            .skip(offset)
            .limit(limit)
        )
        client.close()
        return external_sources
    except Exception as e:
        logger.error("Error in get_external_source_by_source: " + str(e))
        return None


def mongo_create_text_index_for_external_sources():
    """
    Connect to the MongoDB and create a text index for the external source objects.

    :return: A boolean indicating if the operation was successful.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        external_source_collection = db[FS_INDEX_DB]
        external_source_collection.create_index([
            ("title", pymongo.TEXT),
            ("description", pymongo.TEXT),
            ("resources.title", pymongo.TEXT),
            ("resources.description", pymongo.TEXT)
        ])
        client.close()
        logger.info("Created text index for federated search results.")
        return True
    except Exception as e:
        logger.error("Error in create_text_index_for_external_sources: " + str(e))
        return False


def mongo_remove_data_for_external_sources(source=None):
    """
    Connect to the MongoDB and remove data for the external source objects,
    filtering on the provided source if it is provided. If it is not provided, drop all.

    :param source: The source to filter on.
    :return: A boolean indicating if the operation was successful.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        external_source_collection = db[FS_INDEX_DB]
        if source is None:
            external_source_collection.drop()
        else:
            external_source_collection.delete_many({"source": source})
        client.close()
        logger.info("Removed data for external sources.")
        return True
    except Exception as e:
        logger.error("Error in remove_data_for_external_sources: " + str(e))
        return False
