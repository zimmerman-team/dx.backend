import json
import logging
import math
import os

import pandas as pd

from services.util import setup_parsed_loc, setup_solr_url, setup_ssr_loc

logger = logging.getLogger(__name__)
SOLR_URL = setup_solr_url()
DS_LOC = setup_ssr_loc()
DF_LOC = setup_parsed_loc()
RAW_DATA_TYPES = {  # Rawgraphs has date, string or number, default to string when using
    "object": "string",
    "datetime64[ns]": {'type': 'date', 'dateFormat': 'YYYY-MM-DD'},
    "int64": "number",
    "float64": "number",
}


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


def create_ssr_parsed_file(df, prefix="", filename=""):
    """
    We want to prepare the data as JSON with the following properties:
    one object
    {
        "dataset": []
        "dataTypes": {
            "column1": "type",
            "column2": "type",
            ...
        }
        "errors": []
    }
    """
    logger.debug("Creating SSR parsed file")
    # if filename starts with dx, remove the dx
    name = filename[2:] if filename.startswith('dx') else filename
    loc = f"{DF_LOC}parsed-data-files/{name}.json"
    copy_loc = f"{DF_LOC}data-files/{name}.json"
    # Remove the prefix if present
    df.columns = df.columns.str.replace(prefix, "")

    # Get the dtypes of the data frame
    data_types = {column: RAW_DATA_TYPES.get(df[column].dtype.name, "string") for column in df.columns}

    # for each column if dtype is datetime64[ns], parse date to only YYYY-MM-DD
    date_columns = df.select_dtypes(include=['datetime64']).columns
    df[date_columns] = df[date_columns].applymap(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else x)

    # Convert data to a dictionary
    data = df.to_dict(orient="records")
    cleaned_data = [{k: v for k, v in e.items() if not isinstance(v, float) or not math.isnan(v)} for e in data]
    # save parsed at loc
    with open(loc, 'w') as f:
        json.dump({
            "dataset": cleaned_data,
            "dataTypes": data_types,
            "errors": []
        }, f, indent=4)

    # save the raw data to the data files
    with open(copy_loc, 'w') as f:
        json.dump(cleaned_data, f, indent=4)
