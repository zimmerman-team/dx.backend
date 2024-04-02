import logging
import os
import re
import zipfile

import requests
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

from services.external_sources.util import EXTERNAL_DATASET_FORMAT
from services.preprocess_dataset import preprocess_data

Configuration.create(hdx_site="prod", user_agent="Zimmerman_DX", hdx_read_only=True)

logger = logging.getLogger(__name__)
RE_SUB = r'[^a-zA-Z0-9]'


def hdx_search(query, owner, limit=5, prev=0):
    """
    Search HDX for a given query.

    :param query: The query to search for
    :param owner: The owner of the dataset
    :param limit: The number of results to look for
    :param prev: The number of results to skip (for iterative searching)
    :return: A list of dicts in the form of EXTERNAL_DATASET_FORMAT
    """
    logger.debug(f"Searching HDX for query: {query}")
    try:
        datasets = Dataset.search_in_hdx(query, rows=limit, start=prev, fq="isopen:true")
        datasets = [dataset for dataset in datasets if 'csv' in dataset.get_filetypes()]
        res = []
        for dataset in datasets:
            resources = dataset.get_resources()
            for resource in resources:
                try:
                    if resource.get("format", "") not in ["csv", "CSV"]:
                        continue
                    res.append(_create_external_source_object(dataset, resource, owner))
                except Exception as e:
                    logging.error(f"Error in external object creation HDX: {str(e)}")
    except Exception as e:
        logger.error(f"Error in hdx_search: {str(e)}")
        res = []
    return res


def _create_external_source_object(meta, resource, owner):
    """
    We fill the EXTERNAL_DATASET_FORMAT with the data from the meta object.
    The meta object is a hdx-python-api search result dataset object.

    The name of the dataset is not enough, as in some cases,
    the name can be the same for multiple files. Therefore we also include the description.

    :param meta: A hdx-python-api search result dataset object
    :param resource: A hdx-python-api resource object
    :param owner: The owner of the dataset
    :return: A dict in the form of EXTERNAL_DATASET_FORMAT
    """
    resource_name = re.sub(RE_SUB, '', resource.get("name", "")[:-4])
    resource_desc = resource.get("description", "")
    dataf = " - Data file: "

    url = ""
    name = meta.get("name", "")
    if name != "":
        url = f"https://data.humdata.org/dataset/{name}"

    category = ""
    if len(meta.get("groups", [])) > 0:
        category = meta["groups"][0]["title"]

    res = EXTERNAL_DATASET_FORMAT.copy()
    res["name"] = meta.get("title", "") + dataf + resource_desc + " - filename: " + resource_name
    res["description"] = meta.get("notes", "") + dataf + resource_desc + " - filename:" + resource_name
    res["source"] = "HDX"
    res["url"] = url
    res["category"] = category
    res["datePublished"] = meta.get("last_modified", "")
    res["owner"] = owner
    res["authId"] = owner
    res["public"] = False
    return res


def hdx_download(external_dataset):
    logger.debug("Downloading hdx dataset")
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
            download_file(dl_url, dx_id)
            break


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
            logger.info(f"Failed to download file from {url}")
            return False
        # Save the file to the destination folder
        with open(filepath, 'wb') as f:
            f.write(response.content)
            logger.info(f"File downloaded successfully: {filepath}")
        # if filepath endswith .zip
        if filepath.endswith(".zip"):
            # Unzip the file
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                zip_ref.extractall(destination_folder)
            logger.info(f"File unzipped successfully: {filepath}")
            # Delete the zip file
            os.remove(filepath)
            filepath = filepath[:-4]  # Remove the .zip extension
        # rename ./staging/filepath to ./staging/dx{dx_id}.csv
        dx_name = f"dx{dx_id}.csv"
        os.rename(filepath, f"./staging/{dx_name}")
        try:
            preprocess_data(dx_name, create_ssr=True)
        except Exception as e:
            logger.error(f"Failed to preprocess data for {dx_name} due to: {e}")

        return True
    except Exception as e:
        logger.error(f"Failed to download file from {url}: {e}")
        return False
