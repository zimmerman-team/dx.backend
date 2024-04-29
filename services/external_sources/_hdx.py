import copy
import datetime
import logging
import os
import re
import zipfile

import requests
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

from services.external_sources.util import (EXTERNAL_DATASET_FORMAT,
                                            EXTERNAL_DATASET_RESOURCE_FORMAT)
from services.mongo import (mongo_create_external_source,
                            mongo_get_all_external_sources)
from services.preprocess_dataset import preprocess_data

Configuration.create(hdx_site="prod", user_agent="Zimmerman_DX", hdx_read_only=True)

logger = logging.getLogger(__name__)
RE_SUB = r'[^a-zA-Z0-9]'
HDX_SOURCE_NOTICE = "  - This Datasource was retrieved from https://data.humdata.org/."


def hdx_index():
    """
    Indexing function for HDX data.
    Using the HDX API, we search for datasets.
    Then check for updates, if the object is to be updated, pass that as a boolean.

    :return: A string indicating the result of the indexing.
    """
    logger.info("HDX:: Indexing HDX data...")
    # Get existing sources
    existing_external_sources = mongo_get_all_external_sources()
    existing_external_sources = {source["internalRef"]: source for source in existing_external_sources}
    # Get all datasets and process
    res = Dataset.search_in_hdx(fq="isopen:true")
    n_ds = 0
    n_success = 0
    for dataset in res:
        n_ds += 1
        # We use the name as the internal ref, as the id might change.
        internal_ref = dataset.get("name", "")
        update = False
        update_item = None
        if internal_ref in existing_external_sources:
            if existing_external_sources[internal_ref]["dateSourceLastUpdated"] == dataset.get("last_modified", ""):
                continue
            else:
                update = True
                update_item = existing_external_sources[internal_ref]
        try:
            res = _create_external_source_object(dataset, update, update_item)
            if res == "Success":
                n_success += 1
        except Exception as e:
            logger.error(f"HDX:: Failed to index dataset {internal_ref} due to: {e}")
    return f"HDX - Successfully indexed {n_success} out of {n_ds} datasets."


def _create_external_source_object(dataset: Dataset, update=False, update_item=None):
    """
    Core functionality of indexing.
    This function creates the external source object and sends it to MongoDB.
    If update is True, the existing object is updated instead of newly inserted.

    :param dataset: The HDX dataset object.
    :param update: A boolean indicating if the object should be updated instead of inserted.
    :param update_item: The existing object to update.
    :return: A string indicating the result of the operation.
    """
    resources = dataset.get_resources()
    if update:
        external_dataset = copy.deepcopy(update_item)
        external_dataset.pop("score", None)
        external_dataset["subCategories"] = []
        external_dataset["resources"] = []
    else:
        external_dataset = copy.deepcopy(EXTERNAL_DATASET_FORMAT)

    # Prep values
    dsn = dataset.get("name", None)
    if dsn is None:
        logger.info(f"HDX INDEX:: Unable to get name for dataset: {dataset.get('id', 'NO_ID')}")
        return "Unable to process this dataset."

    main_category = ""
    sub_categories = []
    if len(dataset.get("groups", [])) > 0:
        main_category = dataset["groups"][0]["title"]
    if len(dataset.get("groups", [])) > 1:
        sub_categories = [group["title"] for group in dataset["groups"][1:]]

    # Build the external dataset
    external_dataset["title"] = dataset.get("title", "")
    try:
        external_dataset["description"] = dataset.get("notes", "") + HDX_SOURCE_NOTICE
    except Exception:
        external_dataset["description"] = "No description available." + HDX_SOURCE_NOTICE
    external_dataset["source"] = "HDX"
    external_dataset["URI"] = f"https://data.humdata.org/dataset/{dataset.get('name', '')}"
    external_dataset["internalRef"] = dataset.get("name", "")
    external_dataset["mainCategory"] = main_category
    external_dataset["subCategories"] = sub_categories
    external_dataset["datePublished"] = dataset.get("metadata_created", "")
    external_dataset["dateLastUpdated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    external_dataset["dateSourceLastUpdated"] = dataset.get("last_modified", "")

    # Build and attach the resources if they are CSV
    for resource in resources:
        if resource.get("format", "") not in ["csv", "CSV"]:
            continue
        external_resource = copy.deepcopy(EXTERNAL_DATASET_RESOURCE_FORMAT)
        external_resource["title"] = re.sub(RE_SUB, '', resource.get("name", "")[:-4])
        try:
            external_resource["description"] = resource.get("description", "") + HDX_SOURCE_NOTICE
        except Exception:
            external_resource["description"] = "No description available." + HDX_SOURCE_NOTICE
        external_resource["URI"] = resource.get("download_url", "")
        external_resource["internalRef"] = resource.get("id", "")
        external_resource["format"] = resource.get("format", "")
        external_resource["datePublished"] = resource.get("created", "")
        external_resource["dateLastUpdated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        external_resource["dateResourceLastUpdated"] = resource.get("last_modified", "")
        external_dataset["resources"].append(external_resource)

    if len(external_dataset["resources"]) == 0:
        return "No resources attached to this dataset."
    mongo_res = mongo_create_external_source(external_dataset, update=update)
    if mongo_res is not None:
        return "Success"
    return "MongoDB Error"


def hdx_download(external_dataset):
    res = "Sorry, we were unable to download the HDX Dataset, please try again later. Contact the admin if the problem persists."  # NOQA: 501
    logger.debug("HDX:: Downloading hdx dataset")
    try:
        dx_id = external_dataset.get("id", "")
        if dx_id == "":
            dx_id = "0tmp0"

        dataset_title, file_information = external_dataset["name"].split(" - Data file: ")
        desc, filename = file_information.split(" - filename: ")

        # Get the first result where the title is an exact match
        dataset = Dataset.search_in_hdx(query=f'title:"{dataset_title}"', rows=1)[0]
        resources = dataset.get_resources()
        for resource in resources:
            if resource.get("format", "") not in ["csv", "CSV"]:
                continue
            res_name = re.sub(RE_SUB, '', resource.get("name", "")[:-4])
            if res_name == filename and resource.get("description", "") == desc:
                dl_url = resource.get("download_url", "")
                # Download the file
                res = download_file(dl_url, dx_id)
                break
        return res
    except Exception:
        return "Sorry, we were unable to download the HDX Dataset, please try again later. Contact the admin if the problem persists."  # NOQA: 501


def download_file(url, dx_id, destination_folder="./staging"):
    # Ensure the destination folder exists
    os.makedirs(destination_folder, exist_ok=True)

    # Extract the filename from the URL
    filename = url.split('/')[-1]

    # Construct the full path for saving the file
    filepath = os.path.join(destination_folder, filename)

    # Send a GET request to the URL to download the file
    try:
        response = requests.get(url)
        if response.status_code != 200:
            logger.info(f"HDX:: Failed to download file from {url}")
            return "Sorry, we were unable to download the file. Please try again later. Contact the admin if the problem persists."  # NOQA: 501
        # Save the file to the destination folder
        with open(filepath, 'wb') as f:
            f.write(response.content)
            logger.info(f"HDX:: File downloaded successfully: {filepath}")
        # if filepath endswith .zip
        if filepath.endswith(".zip"):
            # Unzip the file
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                zip_ref.extractall(destination_folder)
            logger.info(f"HDX:: File unzipped successfully: {filepath}")
            # Delete the zip file
            os.remove(filepath)
            filepath = filepath[:-4]  # Remove the .zip extension
        # rename ./staging/filepath to ./staging/dx{dx_id}.csv
        dx_name = f"dx{dx_id}.csv"
        os.rename(filepath, f"./staging/{dx_name}")
        try:
            res = preprocess_data(dx_name, create_ssr=True)
        except Exception as e:
            res = "Sorry, we were unable to process the dataset, please try a different dataset. Contact the admin for more information."  # NOQA: 501
            logger.error(f"HDX:: Failed to preprocess data for {dx_name} due to: {e}")
        return res
    except Exception as e:
        logger.error(f"HDX:: Failed to download file from {url}: {e}")
        return "Sorry, we were unable to download or process the file. Please try again later. Contact the admin if the problem persists."  # NOQA: 501
