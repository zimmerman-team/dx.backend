import json
import logging
import math
import os
import shutil

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


def remove_ssr_parsed_files(ds_name):
    """
    Removing parsed files from the SSR directory

    :param ds_name: The name of the dataset
    :return: Success or error message
    """
    logger.debug("Removing SSR parsed files")
    try:
        if ds_name.startswith('dx'):
            ds_name = ds_name[2:]
        parsed_df = f"{DF_LOC}parsed-data-files/{ds_name}.json"
        sample_df = f"{DF_LOC}sample-data-files/{ds_name}.json"
        # remove the parsed files if they exist
        if os.path.exists(parsed_df):
            os.remove(parsed_df)
        if os.path.exists(sample_df):
            os.remove(sample_df)
        return "Success"
    except Exception as e:
        logger.error(f"Error in remove_ssr_parsed_files: {str(e)}")
        return "Sorry, something went wrong in our SSR update. Contact the admin for more information."


def duplicate_ssr_parsed_files(ds_name, new_ds_name):
    """
    Duplicate parsed files from the SSR directory

    :param ds_name: The name of the dataset
    :param new_ds_name: The name of the new dataset
    :return: Success or error message
    """
    logger.debug("Duplicating SSR parsed files")
    try:
        if ds_name.startswith('dx'):
            ds_name = ds_name[2:]
        if new_ds_name.startswith('dx'):
            new_ds_name = new_ds_name[2:]
        parsed_df = f"{DF_LOC}parsed-data-files/{ds_name}.json"
        sample_df = f"{DF_LOC}sample-data-files/{ds_name}.json"
        new_parsed_df = f"{DF_LOC}parsed-data-files/{new_ds_name}.json"
        new_sample_df = f"{DF_LOC}sample-data-files/{new_ds_name}.json"
        # duplicate the parsed files if they exist
        if os.path.exists(parsed_df):
            shutil.copy(parsed_df, new_parsed_df)
        if os.path.exists(sample_df):
            shutil.copy(sample_df, new_sample_df)
        return "Success"
    except Exception as e:
        logger.error(f"Error in duplicate_ssr_parsed_files: {str(e)}")
        return "Sorry, something went wrong in our SSR duplication. Contact the admin for more information."


def get_dataset_stats(df):
    """
    Generate descriptive statistics for each column of the DataFrame.

    This function processes the provided DataFrame and generates statistical summaries for each column.
    Depending on the nature and distribution of data, it categorizes the statistics into 'percentage' for
    common categories, 'bar' for moderate unique values, and 'unique' for columns with many unique values.

    :param df: The DataFrame to be processed
    :return: A list of dictionaries containing the column name, type of statistics, and the data
    """
    try:
        stats = []

        for c in df.columns:
            unique_values = df[c].nunique()

            if unique_values < 4 or (unique_values < len(df) / 1.5 and unique_values > 20):
                data = df[c].value_counts(normalize=True).reset_index()
                data.columns = ["name", "value"]
                data["value"] = data["value"] * 100
                data = data.sort_values(by="value", ascending=False)

                if len(data) > 20:
                    others_value = data.iloc[2:]["value"].sum()
                    others_data = pd.DataFrame([{"name": "Others", "value": others_value}])
                    data = pd.concat([data.iloc[:2], others_data], ignore_index=True)

                stats.append({"name": c, "type": "percentage", "data": data.to_dict(orient="records")})

            elif unique_values < 21:
                data = df[c].value_counts().reset_index()
                data.columns = ["name", "value"]
                data = data.sort_values(by="name")
                stats.append({"name": c, "type": "bar", "data": data.to_dict(orient="records")})

            else:
                stats.append({"name": c, "type": "unique", "data": [{"name": "Unique", "value": unique_values}]})

        return stats
    except Exception as e:
        logger.error("Error in get_dataset_stats: " + str(e))
        return "Error"


def create_ssr_parsed_file(df, prefix="", filename=""):
    """
    We want to prepare the data as JSON with the following properties:
    {
        "dataset": []
        "dataTypes": {
            "column1": "type",
            "column2": "type",
            ...
        }
        "errors": []
        "count": N
        "sample": []  # subset of the data
        "stats": []  # descriptive statistics for each column
    }
    """
    logger.debug("Creating SSR parsed file")
    # if filename starts with dx, remove the dx
    name = filename[2:] if filename.startswith('dx') else filename
    loc = f"{DF_LOC}parsed-data-files/{name}.json"
    sample_loc = f"{DF_LOC}sample-data-files/{name}.json"
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
    stats = get_dataset_stats(df)
    # save parsed at loc
    with open(loc, 'w') as f:
        json.dump({
            "dataset": cleaned_data,
            "dataTypes": data_types,
            "errors": [],
            # Also include the sample data in the parsed file in case it is useful
            "count": len(cleaned_data),
            "sample": cleaned_data[:10],
            "stats": stats
        }, f, indent=4)

    # save the first 10 items to the sample data file
    with open(sample_loc, 'w') as f:
        json.dump({
            "dataset": cleaned_data[:10],
            "dataTypes": data_types,
            "errors": [],
            "count": len(cleaned_data),
            "stats": stats
        }, f, indent=4)


def load_sample_data(dataset_id):
    """
    Read and return the sample data for a given dataset id in the form required by the frontend.

    :param dataset_id: The id of the dataset
    :return: A dictionary containing the sample data
    """
    try:
        logger.debug("Sampling data")
        loc = f"{DF_LOC}sample-data-files/{dataset_id}.json"
        try:
            with open(loc, 'r') as f:
                data = json.load(f)
        except Exception:
            return "Sorry, this dataset is not available. Please contact the admin for more information."
        res = {
            "count": data["count"],
            "dataTypes": data["dataTypes"],
            "sample": data["dataset"][:10],
            "filterOptionGroups": list(data["dataTypes"].keys()),
            "stats": data["stats"]
        }

        return res
    except Exception as e:
        logger.error(f"Error in load_sample_data: {str(e)}")
        return "Sorry, we could not read the data from the provided dataset. Contact the admin for more information."


def load_parsed_data(dataset_id, page: int = 1, page_size: int = 10):
    """
    Read and return the parsed data for a given dataset id in the form required by the frontend.
    Paginated with page and page_size.

    :param dataset_id: The id of the dataset
    :param page: The page number
    :param page_size: The number of items per page
    :return: A dictionary containing the parsed data
    """
    try:
        logger.debug("Loading parsed data, paginated")
        loc = f"{DF_LOC}parsed-data-files/{dataset_id}.json"
        with open(loc, 'r') as f:
            data = json.load(f)

        start = (page - 1) * page_size
        end = start + page_size
        res = {
            "count": data["count"],
            "data": data["dataset"][start:end]
        }

        return res
    except Exception as e:
        logger.error(f"Error in load_parsed_data: {str(e)}")
        return "Sorry, we were unable to retrieve the data for this dataset. Contact the admin for more information."
