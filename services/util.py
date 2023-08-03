import logging
import os

import chardet
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


def remove_files(file_names):
    """
    Remove files from the staging directory
    """
    logger.debug(f"Removing files: {file_names}")
    try:
        for name in file_names:
            os.remove(f"./staging/{name}")
        return "Success"
    except Exception:
        return "Error removing files"


def setup_solr_url():
    """
    Setup the solr url based on the environment variables with defaults if it is not set.
    """
    logger.debug("Setting up solr url")
    try:
        os.environ['SOLR_SUBDOMAIN']  # check if the field is set, otherwise except a keyerror
        host = 'dx-solr'  # the docker network name for solr
    except KeyError:
        host = 'localhost'
    try:
        # Try to get the auth credentials, if they are not set, don't use auth
        auth = f"{os.environ['SOLR_ADMIN_USERNAME']}:{os.environ['SOLR_ADMIN_PASSWORD']}@"
    except KeyError:
        auth = ''
    solr_url = f"http://{auth}{host}:8983/solr"
    logger.info(f"Solr url: {solr_url}")
    return solr_url


def setup_ssr_loc():
    """
    Setup the location for the additional dataset json file.
    """
    logger.debug("Setting up ssr location")
    try:
        return os.environ['DATA_EXPLORER_SSR'] + "additionalDatasets.json"
    except Exception:
        return "./additionalDatasets.json"


def setup_parsed_loc():
    """
    Set up the location for parsed datasets.
    """
    logger.debug("Setting up parsed location")
    try:
        return os.environ['DATA_EXPLORER_SSR']
    except Exception:
        return "./"


def detect_encoding(file_path):
    """
    Detect the encoding of a file
    """
    logger.debug(f"Detecting encoding of file: {file_path}")
    try:
        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read())
        return result['encoding']
    except Exception:
        # Default to the most common file encoding
        return 'utf-8'
