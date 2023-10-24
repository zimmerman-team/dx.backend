import io
import json
import logging
import math
import os
import re
import subprocess

import pandas as pd

from services.external_sources.util import EXTERNAL_DATASET_FORMAT
from services.mongo import create_dataset
from services.preprocess_dataset import preprocess_data

logger = logging.getLogger(__name__)
SEARCH_TERMS = {
    'Sports': 0,
    'Disease': 0,
    'Politics': 0,
    'Science': 0,
    'Technology': 0,
    'Business': 0,
    'Entertainment': 0,
    'Education': 0,
    'Food': 0,
    'Travel': 0
}


def kaggle_search(query, owner, limit=5, prev=0):
    """
    Search kaggle for the given query.
    Get CSV format kaggle metadata, create a list of source objects.

    """
    logger.debug(f"Searching kaggle for query: {query}")
    res = []
    try:
        command = f"kaggle datasets list --file-type csv -s {query} --csv --max-size 5000000"
        if prev % 20 == 0:
            command += f" --page {math.floor(prev/20)+1}"
        output = subprocess.check_output(command, shell=True, text=True)
        # if the first three letters are not 'ref' return an error
        if output[0:3] != "ref":
            logger.error(f"Error in kaggle_search - ref is not found, maybe there is a kaggle update: {output}")
            return "error"

        df = pd.read_csv(io.StringIO(output))
        # for row in df...
        for i in range(len(df)):
            if i < prev:
                continue
            if i >= limit:
                break
            try:
                res.append(_create_external_source_object(df.iloc[i], owner))
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Error in kaggle_search: {str(e)}")
        res = []
    return res


def _create_external_source_object(row, owner):
    res = EXTERNAL_DATASET_FORMAT.copy()
    ref = row["ref"]
    command = f"kaggle datasets metadata {ref} --path ./services/external_sources/staging"
    try:
        output = subprocess.check_output(command, shell=True, text=True)
    except Exception as e:
        logger.error(f"Error in _get_metadata: {str(e)}")
    # if the output does not start with Downloaded metadata to ./staging/, return an error
    if "Downloaded metadata to ./services/external_sources/staging" not in output:
        logger.error(f"Error in _get_metadata - metadata is not found, maybe there is a kaggle update: {output}")
    # read dataset-metadata.json into metadata var
    metadata = json.load(open("./services/external_sources/staging/dataset-metadata.json"))

    if "title" in metadata:
        res["name"] = metadata["title"]
    else:
        res["name"] = row["title"]
    if "subtitle" in metadata:
        res["description"] = metadata["subtitle"]
    elif "description" in metadata:
        res["description"] = metadata["description"]
    res["source"] = "Kaggle"
    res["url"] = f"https://www.kaggle.com/datasets/{ref}"
    if "keywords" in metadata:
        res["category"] = metadata["keywords"][0]
    res["datePublished"] = row["lastUpdated"]
    res["owner"] = owner
    res["authId"] = owner
    res["public"] = False

    # remove the file metadata_json_file
    os.remove("./services/external_sources/staging/dataset-metadata.json")

    logger.info(f"External source object: {res}")
    return res


def kaggle_download(external_dataset):
    # ref = url split on https://www.kaggle.com/datasets/
    ref = external_dataset["url"].split("https://www.kaggle.com/datasets/")[1]
    filenames = _get_filenames(ref)
    _download_files(ref)
    for name in filenames:
        cleaned_name = re.sub(r'[^a-zA-Z0-9]', '', name[:-4])
        file_dataset = external_dataset.copy()
        file_dataset["name"] = file_dataset["name"] + " - " + cleaned_name
        logger.info(file_dataset)
        # we submit the dataset to the mongoDB Dataset collection
        try:
            dx_id = create_dataset(file_dataset)
        except Exception:
            continue
        dx_name = f"dx{dx_id}.csv"
        os.rename(
            f"./staging/{name}",
            f"./staging/{dx_name}"
        )
        try:
            preprocess_data(dx_name, create_ssr=True)
        except Exception:
            continue
        # remove ./staging/{dx.name}
        os.remove(f"./staging/{dx_name}")


def _get_filenames(ref):
    logger.debug(f"Getting filenames for ref: {ref}")
    # get the files for the dataset
    command = f"kaggle datasets files -v {ref}"
    output = subprocess.check_output(command, shell=True, text=True)
    # If the output does not start with name,size,creationDate,return an error
    if "name,size,creationDate" not in output:
        logger.error(f"Error in _get_filenames - files are not found, maybe there is a kaggle update: {output}")
        return "error"
    df = pd.read_csv(io.StringIO(output))
    # make a list of all the items in the first column
    file_names = df["name"].tolist()
    return file_names


def _download_files(ref):
    logger.debug(f"Downloading files for ref: {ref}")
    try:
        # Download the files
        command = f"kaggle datasets download --path ./staging/ {ref} --unzip"
        output = subprocess.check_output(command, shell=True, text=True)
        # if the output does not start with Downloading, return an error
        if "Downloading" not in output:
            return "error"
        return "Success"
    except Exception as e:
        logger.error(f"Error in _download_files: {str(e)}")
        return "error"
