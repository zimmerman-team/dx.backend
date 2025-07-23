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
from services.external_sources.oecd import oecd_index
from services.external_sources.dw import dw_index
from services.external_sources.tgf import tgf_index
from services.external_sources.util import (ALL_SOURCES,
                                            LEGACY_EXTERNAL_DATASET_FORMAT)
from services.external_sources.who import who_index
from services.external_sources.worldbank import worldbank_index
from services.mongo import (mongo_create_text_index_for_external_sources,
                            mongo_find_external_sources_by_text,
                            mongo_get_external_source_by_source)

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
        # Fire the index functions at the same time
        kaggle_res = executor.submit(kaggle_index)
        hdx_res = executor.submit(hdx_index)
        worldbank_res = executor.submit(worldbank_index)
        who_res = executor.submit(who_index)
        tgf_res = executor.submit(tgf_index)
        oecd_res = executor.submit(oecd_index)
        dw_res = executor.submit(dw_index)

        # Wait for all the index functions to finish
        concurrent.futures.wait([kaggle_res, hdx_res, worldbank_res, who_res, tgf_res, oecd_res, dw_res])
        logger.info(f"Kaggle index result: {kaggle_res.result()}")
        logger.info(f"HDX index result: {hdx_res.result()}")
        logger.info(f"World Bank index result: {worldbank_res.result()}")
        logger.info(f"WHO index result: {who_res.result()}")
        logger.info(f"TGF index result: {tgf_res.result()}")
        logger.info(f"OECD index result: {oecd_res.result()}")
        logger.info(f"DW index result: {dw_res.result()}")
    success = mongo_create_text_index_for_external_sources()
    logger.info("Done indexing external sources...")
    if success:
        return "Indexing successful"
    else:
        return "Indexing failed"


def external_search_force_reindex(source):
    """
    Shorthand function to force reindex a source.

    :param source: The datasource (Kaggle, WHO, WB, HDX, TGF, OECD, DW)
    :return: A string indicating success
    """
    if source == "Kaggle":
        kaggle_index(delete=True)
    if source == "WHO":
        who_index(delete=True)
    if source == "WB":
        worldbank_index(delete=True)
    if source == "HDX":
        hdx_index(delete=True)
    if source == "TGF":
        tgf_index(delete=True)
    if source == "OECD":
        oecd_index(delete=True)
    if source == "DW":
        dw_index(delete=True)
    success = mongo_create_text_index_for_external_sources()
    if success:
        return "Indexing successful"
    else:
        return "Indexing failed"


def external_search(query, sources=ALL_SOURCES, legacy=False, limit=None, offset=0, sort_by=None):
    """
    Given a query, find all results in mongoDB from FederatedSearchIndex.

    :param query: The provided query text.
    :param sources: A list of sources to search through, defaults to all available sources.
    :param legacy: A boolean flag for converting to legacy search results (deprecated).
    :param limit: The maximum number of results to return.
    :param offset: The offset to start the search from.
    :return: A list of external source objects.
    """
    if query == "":
        res = mongo_get_external_source_by_source(sources, limit=limit, offset=offset, sort_by=sort_by)
    else:
        res = mongo_find_external_sources_by_text(query, limit=limit, offset=offset, sources=sources, sort_by=sort_by)
    # Remove the 'score' and '_id' from every item in res and filter by source
    res = [
        {k: v for k, v in item.items() if k not in ('score', '_id')}
        for item in res
        if item.get('source') in sources
    ]
    res = [item for item in res if validate_search_result(item, query)]
    # For legacy requests, convert the results
    if legacy:
        res = _convert_legacy_search_results(res)

    return res


def validate_search_result(item, query):
    """
    Validate the search result against the query.
    This function checks if the query is present in the title or description of the item.

    :param item: The search result item to validate.
    :param query: The query text to check against.
    :return: True if the item is valid, False otherwise.
    """
    # If the query is empty, or contains quotes, we return True to include all results.
    if not query or '"' in query:
        return True
    query = query.lower()
    # split query on spaces and commas
    query = query.split()
    query = [q.strip() for q in query if q.strip()]  # remove empty strings
    logger.info("validating result with query: %s", query)
    title = item.get("title", "").lower()
    description = item.get("description", "").lower()
    logger.info("title: %s, description: %s", title, description)
    for q in query:
        if not q in title and not q in description:
            print("Q not in title or description:", q, title, description)
            return False
    return True


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
            if "QuickCharts" in new_res["name"]:
                continue
            new_res["source"] = res.get("source")
            new_res["url"] = res.get("URI")
            new_res["category"] = res.get("mainCategory")
            new_res["datePublished"] = res.get("datePublished")
            new_res["owner"] = "externalSource"
            new_res["authId"] = "externalSource"
            new_res["public"] = False
            all_new_res.append(new_res)
    return all_new_res
