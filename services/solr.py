import logging
import subprocess

import pandas as pd
import pysolr
import requests

from services.util import setup_solr_url

logger = logging.getLogger(__name__)
SOLR_URL = setup_solr_url()


def create_solr_core(name):
    """
    Create a Solr core with the given name
    """
    logger.debug(f"Creating solr core: {name}")
    try:
        params = {
            'action': 'CREATE',
            'name': name,
            'configSet': '_default'
        }
        core_admin_url = f'{SOLR_URL}/admin/cores'
        logger.info(f"Creating solr core: {name} at {core_admin_url}")
        requests.get(core_admin_url, params=params)
        return True
    except Exception as e:
        logger.error(f"Error in create_solr_core: {str(e)}")
        return False


def delete_solr_core(name):
    """
    Delete a Solr core with the given name
    """
    logger.debug(f"Deleting solr core: {name}")
    try:
        core_admin_url = f'{SOLR_URL}/admin/cores'
        params = {
            'action': 'UNLOAD',
            'core': name,
            'deleteIndex': 'true',
            'deleteDataDir': 'true',
            'deleteInstanceDir': 'true'
        }
        requests.get(core_admin_url, params=params)
        return True
    except Exception as e:
        logger.error(f"Error in delete_solr_core: {str(e)}")
        return False


def post_data_to_solr(name):
    """
    Post data to solr core with the given name
    """
    logger.debug(f"Posting data to solr: {name}")
    try:
        # Strip .csv from the name
        if not create_solr_core(name[:-4]):
            logger.error(f"Error in post_data_to_solr for {name}")
            return "error creating solr core"
        # post the file
        csv_file_path = f"./staging/{name}"
        command = f"./solr/bin/post -url {SOLR_URL}/{name[:-4]}/update {csv_file_path}"
        output = subprocess.check_output(command, shell=True, text=True)
        if 'error' in output:
            logger.error(f"Error in post_data_to_solr for {name}: {output}")
            return "error posting data to solr"
        return "Success"
    except Exception as e:
        logger.error(f"Error in post_data_to_solr: {str(e)}")
        return "error posting data to solr"


def check_value_in_core(field, value, core):
    """
    Check if a field:value pair exist in a given core
    """
    logger.debug(f"Checking if {field}:{value} is in core: {core}")
    try:
        solr_core = core
        solr = pysolr.Solr(f'{SOLR_URL}/{solr_core}')
        results = solr.search(f'{field}:"{value}"')
        if len(results) > 0:
            return True
        return False
    except Exception as e:
        logger.error(f"Error in check_value_in_core: {str(e)}")
        return False


def commit_dict_to_solr(dict_content, core):
    """
    Submit a dict to Solr
    """
    logger.debug("Committing metadata to solr")
    try:
        solr_core = core
        solr = pysolr.Solr(f'{SOLR_URL}/{solr_core}')
        solr.add([dict_content])
        solr.commit()
        return "Success"
    except Exception as e:
        logger.error(f"Error in commit_dict_to_solr: {str(e)}")
        return "error committing metadata to solr"


def remove_from_core(field, value, core):
    """
    Remove content from a solr core
    """
    logger.debug(f"Removing {field}:{value} from core: {core}")
    try:
        solr_core = core
        solr = pysolr.Solr(f'{SOLR_URL}/{solr_core}')
        solr.delete(q=f'{field}:"{value}"')
        solr.commit()
        return "Success"
    except Exception as e:
        logger.error(f"Error in remove_from_core: {str(e)}")
        return "Error removing from solr"


def retrieve_content_as_df(core, rows, start=0):
    logger.debug(f"Retrieving content as df from core: {core}")
    try:
        dataset_solrcon = pysolr.Solr(f'http://localhost:8983/solr/{core}', timeout=10)
        data = dataset_solrcon.search('*:*', rows=rows, start=start)
        docs_list = [
            {k: ','.join(v) for k, v in item.items() if k not in ['id', '_version_']}
            for item in data.raw_response['response']['docs']
        ]
        df = pd.DataFrame(docs_list)
        return df
    except Exception as e:
        logger.error(f"Error in retrieve_content_as_df: {str(e)}")
