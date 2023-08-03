import copy
import logging

import pandas as pd
import pysolr

from services.solr import retrieve_content_as_df

logger = logging.getLogger(__name__)
csv_content_structure = {
    "filename": None,
    "headers": [],
    "sampledRows": []
}

data_structure = {
    "csvContent": [],
    "datasetMetadata": {
        "title": None,
        "subtitle": None,
        "description": None,
        "url": None
    }
}


def dataset_metadata_from_df(df, data):
    """
    create a data structure object with the given dataset metadata
    """
    logger.debug("Retrieving dataset metadata from df")
    try:
        for _, row in df.iterrows():
            content = copy.deepcopy(data_structure)
            content['datasetMetadata']['title'] = row['title']
            content['datasetMetadata']['subtitle'] = row['subtitle']
            content['datasetMetadata']['description'] = row['description']
            content['datasetMetadata']['url'] = row['url']
            content['datasetMetadata']['files'] = row['files']
            data.append(content)
    except Exception as e:
        logger.error(f"Error in dataset_metadata_from_df: {str(e)}")
    return data


def retrieve_sample_for_datasets(data):
    """
    Get a sample for the given dataset
    """
    for obj in data:
        files = obj['datasetMetadata']['files']
        if type(files) != list:
            files = [files]
        for data_file in files:
            data_file = data_file[:-4] if data_file.endswith('.csv') else data_file
            try:
                df = retrieve_content_as_df(data_file, 5)
            except pysolr.SolrError as e:
                logger.error(f"Error in retrieve_sample_for_datasets: {str(e)}")
                return data
            # remove data_file + "__" from each column name
            df.columns = [col.split('__')[1] for col in df.columns]
            content = copy.deepcopy(csv_content_structure)
            content['filename'] = data_file + '.csv'
            content['headers'] = df.columns.tolist()
            # for each row of df, convert to list and append to sampledRows
            for _, row in df.iterrows():
                # replace any nan values with None
                row = row.where(pd.notnull(row), None)
                content['sampledRows'].append(row.tolist())
            obj['csvContent'].append(content)
    return data


def retrieve_dataset_data(start):
    """
    Retrieve dataset data starting at a given integer
    """
    logger.debug(f"Retrieving dataset data for {start}")
    df = retrieve_content_as_df('datasets', 5, start=start)
    data = []
    data = dataset_metadata_from_df(df, data)
    data = retrieve_sample_for_datasets(data)
    return data
