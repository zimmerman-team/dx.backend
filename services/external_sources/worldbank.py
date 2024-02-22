import logging
import os
from datetime import datetime

import wbgapi as wb

from services.external_sources.util import EXTERNAL_DATASET_FORMAT
# previously used from services.mongo import create_dataset
from services.preprocess_dataset import preprocess_data

logger = logging.getLogger(__name__)


def worldbank_search(query, owner, limit=5, prev=0):
    """
    Searching the worldbank.
    We use the wbgapi library to search for the query.
    We return n results, where n is the limit.
    prev can be used to skip, for example for pagination.

    :param query: The query to search for
    :param owner: The owner of the dataset
    :param limit: The number of results to return
    :param prev: The number of results to skip
    :return: A list of dicts in the form of EXTERNAL_DATASET_FORMAT
    """
    logger.debug(f"Searching worldbank for query: {query}")
    res = []
    try:
        if query == "":
            search_meta = wb.search2(q="woman")
        else:
            search_meta = wb.search2(q=query)
        # print the number of results in search_meta
        count = 0
        for meta in search_meta:
            if count < prev:
                count += 1
                continue
            if count >= limit + prev:
                break
            count += 1

            try:
                res.append(_create_external_source_object(meta, owner))
            except Exception as e:
                logger.error(f"Error in external object creation worldbank: {str(e)}")
                pass
    except Exception as e:
        logger.error(f"Error in worldbank_search: {str(e)}")
        res = []
    return res


def _create_external_source_object(meta, owner):
    """
    We fill the EXTERNAL_DATASET_FORMAT with the data from the meta object.
    The meta object is a wbgapi search result object.

    :param meta: A wbgapi search result object
    :param owner: The owner of the dataset
    :return: A dict in the form of EXTERNAL_DATASET_FORMAT
    """
    # id should always be present
    res = EXTERNAL_DATASET_FORMAT.copy()
    meta_id = meta.__dict__["id"]
    # get the value from the first key value pair in meta.__dict__
    meta_description = list(meta.__dict__["metadata"].values())[0]
    if "~" in meta_id:
        # Get the string after the last ~
        meta_id = meta_id.split("~")[-1]
    metadata = wb.series.metadata.get(meta_id).__dict__

    # Create the metadata dict
    res["name"] = metadata["metadata"]["IndicatorName"]
    res["description"] = f'{meta_description} - Dataset description: {metadata["metadata"]["Longdefinition"]}'
    res["source"] = "World Bank"
    # the url is a search url, not a direct url to the dataset, as it is too
    # complex to construct from the metadata.
    res["url"] = f"https://www.worldbank.org/en/search?q={metadata['id']}"
    res["category"] = metadata["metadata"]["Topic"]
    res["datePublished"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    res["owner"] = owner
    res["authId"] = owner
    res["public"] = False
    return res


def worldbank_download(external_dataset):
    url = external_dataset["url"]
    logger.debug(f"Downloading worldbank dataset: {url}")

    meta_id = url.split("https://www.worldbank.org/en/search?q=")[-1]
    df = wb.data.DataFrame(meta_id, mrv=3)
    df.reset_index()
    # transpose, WB data is in form country,year1,year2,year3.
    # transpose to year,country
    df = df.T
    # Reset the index to bring 'economy' back as a column
    df.reset_index(inplace=True)
    # Rename the column to 'Year'
    df.rename(columns={'index': 'Year'}, inplace=True)
    # Melt the dataframe to year,country,value format
    df = df.melt(id_vars=['Year'], var_name='Country', value_name='Value')
    # drop na in value
    df = df.dropna()
    df["Year"] = df["Year"].str[2:]

    # save df as a csv file
    dx_id = external_dataset['id']
    dx_name = f"dx{dx_id}.csv"
    dx_loc = f"./staging/{dx_name}"
    df.to_csv(dx_loc, index=False)
    try:
        preprocess_data(dx_name, create_ssr=True)
    except Exception:
        pass
    os.remove(dx_loc)
