import copy
import datetime
import io
import json
import logging
import os
import re
import subprocess

import pandas as pd

from services.external_sources.util import (EXTERNAL_DATASET_FORMAT,
                                            EXTERNAL_DATASET_RESOURCE_FORMAT)
from services.mongo import (mongo_create_external_source,
                            mongo_get_all_external_sources)
from services.preprocess_dataset import preprocess_data

logger = logging.getLogger(__name__)

# Regex to filter text on letters and numbers.
RE_SUB = r'[^a-zA-Z0-9]'
# Items returned per page on kaggle is 20
ITEMS_PER_PAGE = 20
KAGGLE_SOURCE_NOTICE = "  - This Datasource was retrieved from https://www.kaggle.com/."


def handle_update_log(output: str):
    """
    If the Kaggle package has an update, remove the first line from output and log it.

    :param output: The output from the Kaggle CLI command execution.
    :return: The output without the first line if the warning is present.
    """
    try:
        if "Looks like you're using an outdated API Version" in output:
            logger.info(output.partition('\n')[0])
            return '\n'.join(output.partition('\n')[1:]).strip()
    except Exception as e:
        logger.error(f"KAGGLE:: Error in handle_update_log: {str(e)}")
    return output


def kaggle_index():
    """
    Triggering the Kaggle index function, which will index all Kaggle datasets.
    """
    logger.info("KAGGLE INDEX:: Indexing Kaggle data...")
    existing_external_sources = mongo_get_all_external_sources()
    existing_external_sources = {source["internalRef"]: source for source in existing_external_sources}

    n_ds = 0
    n_success = 0
    for i in range(1, 501):  # There can be up to 500 pages of datasets
        n_ds += 1
        res = kaggle_index_page(i, existing_external_sources)
        if res == "break":
            break
        if res == "Success":
            n_success += 1
    logger.info("KAGGLE INDEX:: Kaggle index updated successfully")
    return f"Kaggle - Successfully indexed {n_success} out of {n_ds} dataset pages."


def kaggle_index_page(i, existing_external_sources):
    """
    Subfunction of kaggle index, where i is the page number

    :param i: The page number to index
    :param existing_external_sources: The existing external sources in the database
    """
    try:
        # change back to the original command, the second one is for testing, only a few results
        command = f'kaggle datasets list -v --max-size 5000000 --sort-by votes --file-type csv --license cc -p {i}'
        output = subprocess.check_output(command, shell=True, text=True)
        output = handle_update_log(output)

        if output == "No datasets found":
            logger.info("KAGGLE INDEX:: No datasets found at page {i}, stopping the search")
            return "break"

        # if the first three letters are not 'ref' return an error
        if output[0:3] != "ref":
            logger.error(f"KAGGLE INDEX:: Error in kaggle_index - ref is not found, maybe there is a kaggle update: {output}")  # NOQA: 501
            return []

        df = pd.read_csv(io.StringIO(output))
    except Exception as e:
        logger.error(f"KAGGLE INDEX:: We were unable to update the index the Kaggle datasource, because we could not read the Kaggle API results: {str(e)}")  # NOQA: 501
        return "We were unable to update the index the Kaggle datasource, because we could not read the Kaggle API results."  # NOQA: 501

    for i in range(len(df)):
        row = df.iloc[i]
        update = False
        update_item = None
        if row["ref"] in existing_external_sources:
            # if the source already exists, and the lastUpdated date is the same, skip
            if existing_external_sources[row["ref"]]["dateSourceLastUpdated"] == row["lastUpdated"]:
                continue
            else:
                update = True
                update_item = existing_external_sources[row["ref"]]
        try:
            _create_external_source_object(row, update=update, update_item=update_item)
        except Exception as e:
            logger.error(f"KAGGLE INDEX:: Failed to index dataset {row['ref']} due to: {str(e)}")
    return "Success"


def _create_external_source_object(row, update=False, update_item=None):
    """
    Create the external source object and submit it to MongoDB.

    :param row: The row from the Kaggle dataset list.
    :param update: A boolean indicating if the object should be updated instead of inserted.
    :param update_item: The existing object to update if it exists.
    """
    ref = "REF NOT FOUND"
    try:
        ref = row["ref"]
        command = f"kaggle datasets metadata {ref} --path ./services/external_sources/staging"
        output = subprocess.check_output(command, shell=True, text=True)
    except Exception as e:
        logger.error(f"KAGGLE INDEX: Unable to get the metadata for {ref} from Kaggle, skipping: {str(e)}")
        return f"Unable to get the metadata for {ref} from Kaggle, skipping."
    output = handle_update_log(output)
    # if the output does not start with Downloaded metadata to ./staging/, return an error
    if "Downloaded metadata to ./services/external_sources/staging" not in output:
        logger.error(f"KAGGLE INDEX:: Error in _get_metadata - metadata is not found, maybe there is a kaggle update: {output}")  # NOQA: 501
        return "Unable to download metadata"
    # read dataset-metadata.json into metadata var
    try:
        metadata = json.load(open("./services/external_sources/staging/dataset-metadata.json"))
    except Exception as e:
        logger.error(f"KAGGLE INDEX:: Unable to read the metadata file for {ref}, skipping: {str(e)}")  # NOQA: 501
        return f"Unable to read the metadata file for {ref}, skipping."
    filenames, pretty_filenames = _get_filenames(ref, pretty=True)
    if filenames == "Error":
        logger.error(f"KAGGLE INDEX:: Unable to get the filenames for {ref}, skipping")
        return f"Unable to get the filenames for {ref}, skipping"

    if update:
        external_dataset = copy.deepcopy(update_item)
        external_dataset.pop("score", None)
        external_dataset["subCategories"] = []
        external_dataset["resources"] = []
    else:
        external_dataset = copy.deepcopy(EXTERNAL_DATASET_FORMAT)

    external_dataset["title"] = metadata.get("title", row["title"])
    external_dataset["description"] = metadata.get("subtitle", metadata.get("description", "")) + KAGGLE_SOURCE_NOTICE
    external_dataset["source"] = "Kaggle"
    external_dataset["URI"] = f"https://www.kaggle.com/datasets/{ref}"
    external_dataset["internalRef"] = ref
    # categories
    if "keywords" in metadata:  # Assumes the keywords is a list
        if len(metadata["keywords"]) > 0:
            external_dataset["mainCategory"] = metadata["keywords"][0]
        if len(metadata["keywords"]) > 1:
            external_dataset["subCategories"] = metadata["keywords"][1:]
    external_dataset["datePublished"] = row["lastUpdated"]  # Kaggle only provides the lastUpdated date
    external_dataset["dateLastUpdated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    external_dataset["dateSourceLastUpdated"] = row["lastUpdated"]  # Kaggle only provides the lastUpdated date

    for i in range(len(filenames)):
        file_ref = filenames[i]
        f = pretty_filenames[i]

        external_dataset_resource = EXTERNAL_DATASET_RESOURCE_FORMAT.copy()
        external_dataset_resource["title"] = f
        external_dataset_resource["description"] = f + KAGGLE_SOURCE_NOTICE
        external_dataset_resource["URI"] = f"https://www.kaggle.com/datasets/{ref}"
        external_dataset_resource["internalRef"] = file_ref
        external_dataset_resource["format"] = "csv"
        external_dataset_resource["datePublished"] = row["lastUpdated"]  # Kaggle does not provide a
        external_dataset_resource["dateLastUpdated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        external_dataset_resource["dateResourceLastUpdated"] = row["lastUpdated"]  # Kaggle provides lastUpdated date
        external_dataset["resources"].append(external_dataset_resource)

    # remove the file metadata_json_file
    os.remove("./services/external_sources/staging/dataset-metadata.json")

    mongo_res = mongo_create_external_source(external_dataset, update=update)
    if mongo_res is not None:
        return "Success"
    return "MongoDB Error"


def _get_filenames(ref, pretty=False):
    """
    Function to get the filenames related to a kaggle dataset

    :param ref: The reference to the kaggle dataset
    :param pretty: A boolean indicating if the filenames should be cleaned up
    """
    logger.debug(f"KAGGLE:: Getting filenames for ref: {ref}")
    # get the files for the dataset
    command = f"kaggle datasets files -v {ref}"
    output = subprocess.check_output(command, shell=True, text=True)

    output = handle_update_log(output)

    # If the output does not start with name,size,creationDate,return an error
    if "name,size,creationDate" not in output:
        logger.error(f"KAGGLE:: Error in _get_filenames - files are not found, maybe there is a kaggle update: {output}")  # NOQA: 501
        return "Error"
    df = pd.read_csv(io.StringIO(output))
    # make a list of all the items in the first column
    file_names = df["name"].tolist()
    if pretty:
        return file_names, [re.sub(RE_SUB, ' ', f[:-4]) for f in file_names]
    return file_names


def kaggle_download(external_dataset):
    # ref = url split on https://www.kaggle.com/datasets/
    logger.debug("KAGGLE:: Downloading kaggle dataset")
    ref = external_dataset["url"].split("https://www.kaggle.com/datasets/")[1]
    logger.debug("KAGGLE:: - ref: " + ref)
    file_choice = re.sub(RE_SUB, '', external_dataset["name"].split(" - Data file: ")[1])
    logger.debug(f"KAGGLE:: File choice: {file_choice}")
    filenames = _get_filenames(ref)
    if filenames == "Error":
        return "Sorry, we were unable to find matching files for the selected Kaggle dataset, please try a different dataset, or contact the administrator."  # NOQA: 501
    logger.debug(f"KAGGLE:: File names: {filenames}")
    download_res = _download_files(ref)
    if download_res == "Error":
        return "Sorry, we were unable to download the selected Kaggle dataset, please try a different dataset, or contact the administrator."  # NOQA: 501
    logger.debug("KAGGLE:: files downloaded")
    for name in filenames:
        logger.debug(f"KAGGLE:: Checking file: {name}")
        cleaned_name = re.sub(RE_SUB, '', name[:-4])
        if file_choice != cleaned_name:
            # try to remove the file @ ./staging/{name} as it is not relevant
            try:
                logger.debug(f"KAGGLE:: Removing file: {name}")
                os.remove(f"./staging/{name}")
            except Exception:
                pass
            continue
        file_dataset = external_dataset.copy()
        file_dataset["name"] = file_dataset["name"] + " - " + cleaned_name
        logger.info(f"KAGGLE:: {file_dataset}")
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
            logger.debug(f"KAGGLE:: start preprocessing for {dx_name}")
            preprocess_res = preprocess_data(dx_name, create_ssr=True)
            if preprocess_res != "Success":
                return "Sorry, we were unable to preprocess the selected Kaggle dataset, please try a different dataset, or contact the administrator."  # NOQA: 501
        except Exception:
            continue
        # remove ./staging/{dx.name}
        os.remove(f"./staging/{dx_name}")
        return "Success"


def _download_files(ref):
    logger.debug(f"KAGGLE:: Downloading files for ref: {ref}")
    try:
        # Download the files
        command = f"kaggle datasets download --path ./staging/ {ref} --unzip"
        output = subprocess.check_output(command, shell=True, text=True)
        # if the output does not start with Downloading, return an error
        if "Downloading" not in output:
            return "Error"
        return "Success"
    except Exception as e:
        logger.error(f"KAGGLE:: Error in _download_files: {str(e)}")
        return "Error"
