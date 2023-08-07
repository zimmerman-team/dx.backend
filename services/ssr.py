import json
import logging
import os

from services.util import setup_parsed_loc, setup_solr_url, setup_ssr_loc

logger = logging.getLogger(__name__)
SOLR_URL = setup_solr_url()
DS_LOC = setup_ssr_loc()
DF_LOC = setup_parsed_loc()


def add_ssr_data_scraper_entry(name):
    """
    Update SSR with an additional dataset for the SSR parser to process.
    Load the additional datasets json list
    """
    logger.debug("Creating datascraper entry")
    try:
        with open(DS_LOC, 'r') as file:
            additional_datasets = json.load(file)

        # Check if the name is already in the ids of the loaded list
        for dataset in additional_datasets:
            # if the name starts with dx, remove the dx
            sub = name
            if name.startswith('dx'):
                sub = name[2:]
            if dataset['id'] == sub:
                return

        final_name = name
        if name.startswith('dx'):
            final_name = name[2:]
        ds_obj = {
            "id": final_name,
            "datasource": "solr",
            "url": f"{SOLR_URL}/{name}/select?indent=true&q.op=OR&q=*%3A*&useParams=",
            "countUrl": f"{SOLR_URL}/{name}/select?indent=true&q.op=OR&q=*%3A*&useParams=&rows=0"
        }
        additional_datasets.append(ds_obj)

        with open(DS_LOC, 'w') as file:
            json.dump(additional_datasets, file, indent=4)
    except Exception as e:
        logger.error(f"Error in add_ssr_data_scraper_entry: {str(e)}")


def remove_ssr_ref(dataset_id):
    """
    Removing the reference from SSR
    """
    logger.debug("Removing SSR ref")
    try:
        with open(DS_LOC, 'r') as file:
            additional_datasets = json.load(file)

        # Remove the dict with id == file from the list
        for dataset in additional_datasets:
            if dataset['id'] == dataset_id:
                additional_datasets.remove(dataset)
                break

        with open(DS_LOC, 'w') as file:
            json.dump(additional_datasets, file, indent=4)
    except Exception as e:
        logger.error(f"Error in remove_ssr_ref: {str(e)}")


def remove_ssr_parsed_files(ds_name):
    """
    Removing parsed files from the SSR directory
    """
    logger.debug("Removing SSR parsed files")
    try:
        if ds_name.startswith('dx'):
            ds_name = ds_name[2:]
        parsed_df = f"{DF_LOC}parsed-data-files/{ds_name}.json"
        unparsed_df = f"{DF_LOC}data-files/{ds_name}.json"
        # remove the parsed files if they exist
        if os.path.exists(parsed_df):
            os.remove(parsed_df)
        if os.path.exists(unparsed_df):
            os.remove(unparsed_df)
        return "Success"
    except Exception as e:
        logger.error(f"Error in remove_ssr_parsed_files: {str(e)}")
        return "Sorry, something went wrong in our SSR update. Contact the admin for more information."
