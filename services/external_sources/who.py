import logging
import os
from datetime import datetime

import pandas as pd
import requests

from services.external_sources.util import EXTERNAL_DATASET_FORMAT
# previously used from services.mongo import create_dataset
from services.preprocess_dataset import preprocess_data

logger = logging.getLogger(__name__)


def who_search(query, owner, limit=5, prev=0):
    """
    Searching the who.
    They have an odata api spec'd here:
    https://www.who.int/data/gho/info/gho-odata-api

    We return n results, where n is the limit.
    prev can be used to skip, for example for pagination.

    :param query: The query to search for
    :param owner: The owner of the dataset
    :param limit: The number of results to return
    :param prev: The number of results to skip
    :return: A list of dicts in the form of EXTERNAL_DATASET_FORMAT
    """
    logger.debug(f"Searching who for query: {query}")
    res = []
    try:
        search_meta = _who_search_indicators(query)
        # print the number of results in search_meta
        count = 0
        for meta in search_meta:
            if count < prev:
                continue
            if count >= limit + prev:
                break
            count += 1

            try:
                res.append(_create_external_source_object(meta, owner))
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Error in who_search: {str(e)}")
        res = []
    return res


def _who_search_indicators(query):
    """
    Query the indicators API searching for the query string.
    Search results include the IndicatorCode, IndicatorName, and Language.
    :param query: The query string
    :return: A list of dicts containing the search results
    """
    # get all the indicators from the who api
    who_query_url = f"https://ghoapi.azureedge.net/api/Indicator?$filter=contains(IndicatorName,%20%27{query}%27)"
    res = requests.get(who_query_url).json()["value"]
    return res


def _get_additional_metadata():
    # get all indicators:
    all_indicators_url = "https://apps.who.int/gho/athena/api/GHO/?format=json"
    all_indicators = requests.get(all_indicators_url).json()["dimension"][0]["code"]
    # Iterate through the list of objects
    for obj in all_indicators:
        if isinstance(obj["attr"], list):
            # Check if there is a dictionary with "category" equal to TARGET_CATEGORY
            category_found = False
            for item in obj["attr"]:
                if isinstance(item, dict) and item.get("category") == "CATEGORY":
                    obj["category"] = item.get("value")
                    category_found = True
                    break
            if not category_found:
                obj["category"] = "miscellaneous"
        else:
            obj["category"] = "miscellaneous"
    all_indicators = {i["label"]: i for i in all_indicators}
    return all_indicators


def _create_external_source_object(meta, owner):
    """
    We fill the EXTERNAL_DATASET_FORMAT with the data from the meta object.
    The meta object is a wbgapi search result object.

    :param meta: A wbgapi search result object
    :param owner: The owner of the dataset
    :return: A dict in the form of EXTERNAL_DATASET_FORMAT
    """
    # id should always be present
    code = meta["IndicatorCode"]
    additional_metadata = _get_additional_metadata()
    source_url = additional_metadata[code]["url"]
    if source_url == "":
        source_url = f"https://ghoapi.azureedge.net/api/{code}"

    res = EXTERNAL_DATASET_FORMAT.copy()
    res["name"] = code
    res["description"] = meta["IndicatorName"]
    res["source"] = "WHO"
    res["url"] = source_url
    res["category"] = additional_metadata[code]["category"]
    res["datePublished"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    res["owner"] = owner
    res["authId"] = owner
    res["public"] = False
    return res


def who_download(external_dataset):
    # Download data
    url = f"https://ghoapi.azureedge.net/api/{external_dataset['name']}"
    logger.debug(f"Downloading who dataset: {url}")
    data = requests.get(url).json()["value"]
    df = pd.DataFrame(data)

    # Drop column if empty
    df = df.dropna(axis=1, how='all')
    # Drop excess columns
    if "Id" in df.columns:
        df = df.drop(columns=["Id"])
    if "IndicatorCode" in df.columns:
        df = df.drop(columns=["IndicatorCode"])

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
