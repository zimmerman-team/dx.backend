"""
GOAL:

- Define a format to store our indexed data.
- For every source, do an index of all the relevant search result.
    - Include an update feature, to make sure we don't re-index the same data.
- Provide a function to retrieve indexed data for a given search query.
"""
import concurrent.futures
import logging

from dotenv import load_dotenv

from services.external_sources._hdx import hdx_index
from services.external_sources.kaggle import kaggle_index
from services.external_sources.util import (ALL_SOURCES,
                                            LEGACY_EXTERNAL_DATASET_FORMAT)
from services.external_sources.who import who_index
from services.external_sources.worldbank import worldbank_index
from services.mongo import (mongo_create_text_index_for_external_sources,
                            mongo_find_external_sources_by_text)

logger = logging.getLogger(__name__)
load_dotenv()
DEFAULT_SEARCH_TERM = "World population"
DATA_FILE = " - Data file: "


def external_search_index():
    """
    Trigger the individual index functions for each source.
    Once they are all done, create a text index for the external sources.

    :return: A string indicating the result of the indexing.
    """
    logger.info("Indexing external sources...")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Fire the 4 index functions at the same time
        kaggle_res = executor.submit(kaggle_index)
        hdx_res = executor.submit(hdx_index)
        worldbank_res = executor.submit(worldbank_index)
        who_res = executor.submit(who_index)

        # Wait for all the index functions to finish
        concurrent.futures.wait([kaggle_res, hdx_res, worldbank_res, who_res])
        logger.info(f"Kaggle index result: {kaggle_res.result()}")
        logger.info(f"HDX index result: {hdx_res.result()}")
        logger.info(f"World Bank index result: {worldbank_res.result()}")
        logger.info(f"WHO index result: {who_res.result()}")

    success = mongo_create_text_index_for_external_sources()
    logger.info("Done indexing external sources...")
    if success:
        return "Indexing successful"
    else:
        return "Indexing failed"


def external_search(query, sources=ALL_SOURCES, legacy=False, limit=None, offset=0):
    """
    Given a query, find all results in mongoDB from FederatedSearchIndex.

    :param query: The provided query text.
    :param sources: A list of sources to search through, defaults to all available sources.
    :param legacy: A boolean flag for converting to legacy search results (deprecated).
    :param limit: The maximum number of results to return.
    :param offset: The offset to start the search from.
    :return: A list of external source objects.
    """
    res = mongo_find_external_sources_by_text(query, limit=limit, offset=offset, sources=sources)
    # Remove the 'score' and '_id' from every item in res and filter by source
    res = [
        {k: v for k, v in item.items() if k not in ('score', '_id')}
        for item in res
        if item.get('source') in sources
    ]
    # For legacy requests, convert the results
    if legacy:
        res = _convert_legacy_search_results(res)

    return res


def _convert_legacy_search_results(all_res):
    """
    Convert to the expected format for frontend, and return.

    conversions required:
        title to name
        URI to url
        mainCategory to category

        Create a duplicate for each resource, and combine the names.

    :param all_res: List of all results from the search.
    :return: A converted list of search results.
    """
    all_new_res = []
    for res in all_res:
        one_file = res.get("resources", []) == 1
        for resource in res.get("resources", []):
            new_res = LEGACY_EXTERNAL_DATASET_FORMAT.copy()
            new_res["name"] = res.get("title")
            new_res["description"] = res.get("description")
            if not one_file:
                new_res["name"] += DATA_FILE + resource.get("title")
                new_res["description"] += DATA_FILE + resource.get("title")
            new_res["source"] = res.get("source")
            new_res["url"] = res.get("URI")
            new_res["category"] = res.get("mainCategory")
            new_res["datePublished"] = res.get("datePublished")
            new_res["owner"] = "externalSource"
            new_res["authId"] = "externalSource"
            new_res["public"] = False
            all_new_res.append(new_res)
    return all_new_res
