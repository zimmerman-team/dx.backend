import io
import json
import logging
import math
import os
import re
import subprocess

import pandas as pd

from services.external_sources.util import EXTERNAL_DATASET_FORMAT
# previously used from services.mongo import create_dataset
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
RE_SUB = r'[^a-zA-Z0-9]'

# Items returned per page on kaggle is 20
ITEMS_PER_PAGE = 20


def handle_update_log(output: str):
    # if there's an update remove the first line from output and log it
    if "Looks like you're using an outdated API Version" in output:
        logger.info(output.partition('\n')[0])
        return '\n'.join(output.partition('\n')[1:]).strip()
    return output


def kaggle_search(query, owner, limit=5, prev=0):
    """
    Search kaggle for the given query.
    Get CSV format kaggle metadata, create a list of source objects.

    """
    logger.debug(f"Searching kaggle for query: {query}")
    res = []
    try:
        command = f'kaggle datasets list --file-type csv -s "{query}" --csv --max-size 5000000'

        command += f" --page {math.floor(prev/ITEMS_PER_PAGE)+1}"
        output = subprocess.check_output(command, shell=True, text=True)

        # if there's an update remove the first line from output and log it
        output = handle_update_log(output)

        # if the first three letters are not 'ref' return an error
        if output[0:3] != "ref":
            logger.error(f"Error in kaggle_search - ref is not found, maybe there is a kaggle update: {output}")
            return []

        df = pd.read_csv(io.StringIO(output))
        # for row in df...
        for i in range(len(df)):
            """
            This allows the pointer 'index' to continue to the next page
            when prev gets higher than ITEMS_PER_PAGE
            """
            index = i + math.floor(prev/ITEMS_PER_PAGE) * ITEMS_PER_PAGE
            if index < prev:
                continue
            if index >= limit + prev:
                break
            try:
                res += _create_external_source_object(df.iloc[i], owner)
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Error in kaggle_search: {str(e)}")
        res = []
    return res


def _create_external_source_object(row, owner):
    res_l = []
    ref = row["ref"]
    command = f"kaggle datasets metadata {ref} --path ./services/external_sources/staging"
    try:
        output = subprocess.check_output(command, shell=True, text=True)
    except Exception as e:
        logger.error(f"Error in _get_metadata: {str(e)}")

    output = handle_update_log(output)
    # if the output does not start with Downloaded metadata to ./staging/, return an error
    if "Downloaded metadata to ./services/external_sources/staging" not in output:
        logger.error(f"Error in _get_metadata - metadata is not found, maybe there is a kaggle update: {output}")
    # read dataset-metadata.json into metadata var
    metadata = json.load(open("./services/external_sources/staging/dataset-metadata.json"))
    filenames = _get_filenames(ref, pretty=True)
    dataf = " - Data file: "
    for f in filenames:
        res = EXTERNAL_DATASET_FORMAT.copy()
        if "title" in metadata:
            res["name"] = metadata["title"] + dataf + f
        else:
            res["name"] = row["title"] + dataf + f
        if "subtitle" in metadata:
            res["description"] = metadata["subtitle"] + dataf + f
        elif "description" in metadata:
            res["description"] = metadata["description"] + dataf + f
        res["source"] = "Kaggle"
        res["url"] = f"https://www.kaggle.com/datasets/{ref}"
        if "keywords" in metadata:
            res["category"] = metadata["keywords"][0]
        res["datePublished"] = row["lastUpdated"]
        res["owner"] = owner
        res["authId"] = owner
        res["public"] = False
        res_l.append(res)

    # remove the file metadata_json_file
    os.remove("./services/external_sources/staging/dataset-metadata.json")

    logger.info(f"External source object: {res_l}")
    return res_l


def kaggle_download(external_dataset):
    # ref = url split on https://www.kaggle.com/datasets/
    logger.debug("Downloading kaggle dataset")
    ref = external_dataset["url"].split("https://www.kaggle.com/datasets/")[1]
    logger.debug("- ref: " + ref)
    file_choice = re.sub(RE_SUB, '', external_dataset["name"].split(" - Data file: ")[1])
    logger.debug(f"File choice: {file_choice}")
    filenames = _get_filenames(ref)
    if filenames == "Error":
        return "Sorry, we were unable to find matching files for the selected Kaggle dataset, please try a different dataset, or contact the administrator."  # NOQA: 501
    logger.debug(f"File names: {filenames}")
    download_res = _download_files(ref)
    if download_res == "Error":
        return "Sorry, we were unable to download the selected Kaggle dataset, please try a different dataset, or contact the administrator."  # NOQA: 501
    logger.debug("files downloaded")
    for name in filenames:
        logger.debug(f"Checking file: {name}")
        cleaned_name = re.sub(RE_SUB, '', name[:-4])
        if file_choice != cleaned_name:
            # try to remove the file @ ./staging/{name} as it is not relevant
            try:
                logger.debug(f"Removing file: {name}")
                os.remove(f"./staging/{name}")
            except Exception:
                pass
            continue
        file_dataset = external_dataset.copy()
        file_dataset["name"] = file_dataset["name"] + " - " + cleaned_name
        logger.info(file_dataset)
        # we submit the dataset to the mongoDB Dataset collection
        # try:
        #     dx_id = create_dataset(file_dataset)
        # except Exception:
        #     continue
        dx_id = external_dataset['id']
        dx_name = f"dx{dx_id}.csv"
        try:
            os.rename(
                f"./staging/{name}",
                f"./staging/{dx_name}"
            )
        except Exception:
            pass
        try:
            logger.debug(f"start preprocessing for {dx_name}")
            preprocess_res = preprocess_data(dx_name, create_ssr=True)
            if preprocess_res != "Success":
                return "Sorry, we were unable to preprocess the selected Kaggle dataset, please try a different dataset, or contact the administrator."  # NOQA: 501
        except Exception:
            continue
        # remove ./staging/{dx.name}
        os.remove(f"./staging/{dx_name}")
        return "Success"


def _get_filenames(ref, pretty=False):
    logger.debug(f"Getting filenames for ref: {ref}")
    # get the files for the dataset
    command = f"kaggle datasets files -v {ref}"
    output = subprocess.check_output(command, shell=True, text=True)

    output = handle_update_log(output)

    # If the output does not start with name,size,creationDate,return an error
    if "name,size,creationDate" not in output:
        logger.error(f"Error in _get_filenames - files are not found, maybe there is a kaggle update: {output}")
        return "Error"
    df = pd.read_csv(io.StringIO(output))
    # make a list of all the items in the first column
    file_names = df["name"].tolist()
    if pretty:
        # file_names = [f for f in filenames] where f is the filename without the extension, and _ replaced by a space
        file_names = [re.sub(RE_SUB, ' ', f[:-4]) for f in file_names]
    return file_names


def _download_files(ref):
    logger.debug(f"Downloading files for ref: {ref}")
    try:
        # Download the files
        command = f"kaggle datasets download --path ./staging/ {ref} --unzip"
        output = subprocess.check_output(command, shell=True, text=True)
        # if the output does not start with Downloading, return an error
        if "Downloading" not in output:
            return "Error"
        return "Success"
    except Exception as e:
        logger.error(f"Error in _download_files: {str(e)}")
        return "Error"
