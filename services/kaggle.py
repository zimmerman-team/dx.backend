import io
import json
import logging
import os
import re
import subprocess

import pandas as pd

from services.preprocess_dataset import preprocess_data
from services.solr import (check_value_in_core, commit_dict_to_solr,
                           post_data_to_solr)
from services.util import remove_files

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


def run_update(num_search_terms=1):
    """
    Run the update process by searching for the search terms and updating the solr core
    """
    logger.debug("Running update")
    search_res = _search(num_search_terms)
    return search_res


def _search(num_search_terms):
    """
    Search n-times for the search terms defined in SEARCH_TERMS.
    Find the next available ref that is not already downloaded.
    Download the metadata and files for the ref.
    Rename the files to the ref name.
    Commit the metadata and data to solr
    """
    status = "Success"
    searches_made = 0
    for _ in range(num_search_terms):
        for term in SEARCH_TERMS:
            if searches_made >= num_search_terms*len(SEARCH_TERMS):
                break

            ref = _get_next_ref(term)
            metadata = _get_metadata(ref)
            file_names = _get_filenames(ref)
            _download_files(ref)
            file_names = _rename_files(file_names, ref)

            # update the metadata and commit to solr
            metadata["files"] = file_names

            commit_dict_to_solr(metadata, 'datasets')
            for name in file_names:
                preprocess_data(name)
                status = post_data_to_solr(name)

                # Remove the processed file
                remove_files(file_names)

                searches_made += 1
                SEARCH_TERMS[term] = SEARCH_TERMS[term] + 1
    return status


def _get_next_ref(term):
    """
    Search for refs that are not yet in the core.
    """
    refdf = None
    while True:
        try:
            ref, refdf = _find_data(term, refdf, SEARCH_TERMS[term])
            if not check_value_in_core('ref', ref, 'datasets'):
                break
            SEARCH_TERMS[term] = SEARCH_TERMS[term] + 1
        except Exception as e:
            logger.error(f"Error in _get_next_ref: {str(e)}")
    return ref


def _find_data(term, refdf, i=0):
    """
    Search kaggle for the given term
    """
    logger.debug(f"Finding data for term: {term}")
    # search for the term
    if type(refdf) == pd.DataFrame:
        ref = refdf["ref"][i]
        return ref, refdf
    command = f"kaggle datasets list --file-type csv -s {term} --csv --max-size 5000000"
    output = subprocess.check_output(command, shell=True, text=True)
    # if the first three letters are not 'ref' return an error
    if output[0:3] != "ref":
        logger.error(f"Error in _find_data - ref is not found, maybe there is a kaggle update: {output}")
        return "error", refdf
    refdf = pd.read_csv(io.StringIO(output))
    # get the first item from the ref column
    ref = refdf["ref"][i]
    return ref, refdf


def _get_metadata(ref):
    """
    Get metadata for a given ref from the kaggle api
    """
    logger.debug(f"Getting kaggle metadata for ref: {ref}")
    command = f"kaggle datasets metadata {ref} --path ./staging/"
    try:
        output = subprocess.check_output(command, shell=True, text=True)
    except Exception as e:
        logger.error(f"Error in _get_metadata: {str(e)}")
        return "error"
    logger.info(f"Output from kaggle metadata: {output}")
    # if the output does not start with Downloaded metadata to ./staging/, return an error
    if "Downloaded metadata to ./staging/" not in output:
        logger.error(f"Error in _get_metadata - metadata is not found, maybe there is a kaggle update: {output}")
        return "error"
    metadata_json_file = "./staging/dataset-metadata.json"
    metadata_json = json.load(open(metadata_json_file))
    metadata = {
        "ref": ref,
        "title": metadata_json["title"],
        "subtitle": metadata_json["subtitle"],
        "description": metadata_json["description"],
        "url": f"https://www.kaggle.com/{metadata_json['id']}",
    }
    # remove the file metadata_json_file
    os.remove(metadata_json_file)
    return metadata


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


def _rename_files(file_names, ref):
    logger.debug(f"Renaming files for ref: {ref}")
    try:
        new_names = []
        for name in file_names:
            new_name = f"{re.sub(r'[^a-zA-Z0-9]', '', ref)}_{re.sub(r'[^a-zA-Z0-9]', '', name)}"
            new_name = new_name[:-3] + '.csv'
            os.rename(
                f"./staging/{name}",
                f"./staging/{new_name}"
            )
        new_names.append(new_name)
        # return the list of renamed files
        return new_names
    except Exception as e:
        logger.error(f"Error in _rename_files: {str(e)}")
        return []
