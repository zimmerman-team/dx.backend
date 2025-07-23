import json
import logging
import os
import bson

import pymongo
from dotenv import load_dotenv

from services.util import setup_parsed_loc

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
PARSED_DATA_COLLECTION = "ParsedDatasetData"
DATASET_COLLECTION = "Dataset"
DF_LOC = setup_parsed_loc()


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


def mongo_migrate_local_parsed_data_files():
    """
    Migrate local parsed files to MongoDB.
    """
    try:
        parsed_df = f"{DF_LOC}parsed-data-files"

        # Ensure the directory exists
        os.makedirs(parsed_df, exist_ok=True)

        # Iterate through all files in the parsed data location
        for filename in os.listdir(parsed_df):
            if filename.endswith('.json'):
                file_path = os.path.join(parsed_df, filename)
                datasetId = filename.split('.')[0]  # Assuming filename is datasetId.json
                if datasetId:
                    with open(file_path, 'r') as file:
                        document = json.load(file)
                        mongo_update_document_by_id(
                            DATASET_COLLECTION,
                            datasetId,
                            {
                                "stats": document.get("stats", []),
                                "dataTypes": document.get("dataTypes", {}),
                                "count": document.get("count", 0),
                                "errors": document.get("errors", []),
                                "sample": document.get("sample", []),
                            }
                        )
                        # mongo_insert_documents(
                        #     PARSED_DATA_COLLECTION,
                        #     [
                        #         {**record, "datasetId": bson.ObjectId(datasetId)}
                        #         for record in document.get("dataset", [])
                        #     ]
                        # )

        return "Migration completed successfully."
    except Exception as e:
        logger.error(f"Error during migration: {str(e)}")
        return "Migration failed."


def mongo_insert_document(collection_name, document):
    """
    Connect to the MongoDB and insert a document into the specified collection.

    :param collection_name: The name of the collection to insert the document into.
    :param document: The document to insert.
    :return: The inserted document ID or None if the operation failed.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        collection = db[collection_name]
        inserted_data = collection.insert_one(document)
        client.close()
        return inserted_data.inserted_id
    except Exception as e:
        logger.error("Error in insert_document: " + str(e))
        return None


def mongo_insert_documents(collection_name, documents):
    """
    Connect to the MongoDB and insert multiple documents into the specified collection.

    :param collection_name: The name of the collection to insert the documents into.
    :param documents: A list of documents to insert.
    :return: The number of inserted documents or None if the operation failed.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        collection = db[collection_name]
        inserted_data = collection.insert_many(documents)
        client.close()
        return len(inserted_data.inserted_ids)
    except Exception as e:
        logger.error("Error in insert_documents: " + str(e))
        return None


def mongo_get_document_by_id(collection_name, document_id):
    """
    Connect to the MongoDB and get a document by its ID from the specified collection.

    :param collection_name: The name of the collection to search in.
    :param document_id: The ID of the document to retrieve.
    :return: The document if found, None otherwise.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        collection = db[collection_name]
        document = collection.find_one({"_id": bson.ObjectId(document_id)})
        client.close()
        return document
    except Exception as e:
        logger.error("Error in get_document_by_id: " + str(e))
        return None


def mongo_get_documents(collection_name, query=None, limit=None, skip=0, sort_by=None):
    """
    Connect to the MongoDB and get documents from the specified collection.

    :param collection_name: The name of the collection to search in.
    :param query: The query to filter documents.
    :param limit: The maximum number of results to return.
    :param skip: The number of documents to skip.
    :param sort_by: The field to sort by.
    :return: A list of documents matching the query.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        collection = db[collection_name]
        mongo_query = query if query else {}
        sort_style = [(sort_by, 1)] if sort_by else None
        documents = list(collection.find(mongo_query).skip(skip).limit(limit).sort(sort_style))
        client.close()
        return documents
    except Exception as e:
        logger.error("Error in get_documents: " + str(e))
        return None


def mongo_delete_documents(collection_name, query):
    """
    Connect to the MongoDB and delete documents from the specified collection based on a query.

    :param collection_name: The name of the collection to search in.
    :param query: The query to filter documents to delete.
    :return: The number of documents deleted.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        collection = db[collection_name]
        result = collection.delete_many(query)
        client.close()
        return result.deleted_count
    except Exception as e:
        logger.error("Error in delete_documents: " + str(e))
        return 0


def mongo_update_document_by_id(collection_name, document_id, update_data):
    """
    Connect to the MongoDB and update a document by its ID in the specified collection.

    :param collection_name: The name of the collection to search in.
    :param document_id: The ID of the document to update.
    :param update_data: The data to update in the document.
    :return: The number of documents updated.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        collection = db[collection_name]
        if collection.find_one({"_id": bson.ObjectId(document_id)}) is None:
            collection.insert_one({"_id": bson.ObjectId(document_id)})
        result = collection.update_one({"_id": bson.ObjectId(document_id)}, {"$set": update_data})
        client.close()
        return result.modified_count
    except Exception as e:
        logger.error("Error in update_document_by_id: " + str(e))
        return 0


def mongo_delete_document_by_id(collection_name, document_id):
    """
    Connect to the MongoDB and delete a document by its ID from the specified collection.

    :param collection_name: The name of the collection to search in.
    :param document_id: The ID of the document to delete.
    :return: The number of documents deleted.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        collection = db[collection_name]
        result = collection.delete_one({"_id": bson.ObjectId(document_id)})
        client.close()
        return result.deleted_count
    except Exception as e:
        logger.error("Error in delete_document_by_id: " + str(e))
        return 0


def mongo_duplicate_dataset_data(dataset_id, new_dataset_id):
    """
    Connect to the MongoDB and duplicate dataset data from one dataset to another.

    :param dataset_id: The ID of the dataset to duplicate from.
    :param new_dataset_id: The ID of the dataset to duplicate to.
    :return: The number of documents duplicated or None if the operation failed.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        collection = db[PARSED_DATA_COLLECTION]

        collection.aggregate(
            [
                {"$match": {"datasetId": bson.ObjectId(dataset_id)}},
                {"$addFields": {"datasetId": bson.ObjectId(new_dataset_id)}},
                {
                    "$unset": "_id"
                },  # This ensures new documents are created instead of replacing
                {"$merge": {"into": PARSED_DATA_COLLECTION}},
            ]
        )
        client.close()
    except Exception as e:
        logger.error("Error in duplicate_dataset_data: " + str(e))
        return None


def mongo_get_dataset_size_in_bytes(dataset_id):
    """
    Connect to the MongoDB and get the size of the dataset.

    :param dataset_id: The ID of the dataset
    :return: The size of the dataset or None if the operation failed.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        size = 0
        dataset_collection = db[DATASET_COLLECTION]
        data_collection = db[PARSED_DATA_COLLECTION]
        dataset = dataset_collection.find_one({"_id": bson.ObjectId(dataset_id)})
        if not dataset:
            return 0
        size += len(bson.BSON.encode(dataset))
        # Get the size of the parsed data documents
        parsed_data_documents = data_collection.find({"datasetId": bson.ObjectId(dataset_id)})
        for doc in parsed_data_documents:
            size += len(bson.BSON.encode(doc))
        # Close the client connection
        client.close()
        return dataset.get("count", 0) if dataset else 0
    except Exception as e:
        logger.error("Error in get_dataset_size: " + str(e))
        return 0


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


def mongo_create_index_for_dataset_data():
    """
    Connect to the MongoDB and create an index for the dataset data collection.

    :return: A boolean indicating if the operation was successful.
    """
    try:
        client = mongo_client(dev=DEV)
        db = client[DATABASE_NAME]
        dataset_data_collection = db[PARSED_DATA_COLLECTION]
        dataset_data_collection.create_index([
            ("datasetId", pymongo.ASCENDING),
        ])
        client.close()
        logger.info("Created index for dataset data collection.")
        return True
    except Exception as e:
        logger.error("Error in create_index_for_dataset_data: " + str(e))
        return False


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
