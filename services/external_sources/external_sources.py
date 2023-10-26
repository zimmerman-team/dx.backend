import logging

from dotenv import load_dotenv

from services.external_sources.kaggle import kaggle_download, kaggle_search
from services.external_sources.util import ALL_SOURCES, list_shuffle_sorted
from services.external_sources.worldbank import (worldbank_download,
                                                 worldbank_search)

logger = logging.getLogger(__name__)
load_dotenv()


def search_external_sources(query, owner, sources=ALL_SOURCES, limit=10):
    """
    The search feature for external sources triggers the implementation for
    each of the external sources, and compiles a list of results from each
    of them.

    A query can be provided, as well as a list of sources to search through,
    for example, only kaggle, or only data.gov and WHO. We can also limit
    the number of results per source.

    :param query: The query to search for
    :param sources: A list of sources to search through
    :param limit: The number of results per source
    :return: A list of results in the form of an ExternalSource object
    """
    try:
        results_list = []
        for source in sources:
            if source == "Kaggle":
                results_list.append(kaggle_search(query, owner))
            if source == "World Bank":
                results_list.append(worldbank_search(query, owner))
        result = list_shuffle_sorted(results_list, limit)
    except Exception as e:
        logger.error(f"Error in external source search: {str(e)}")
        result = []
    return result


def download_external_source(external_dataset):
    """
    This process should receive an external dataset object, and download
    to a staging folder. Then process the dataset. If anything fails, the
    dataset should be removed from the staging folder. This dataset is then
    included in the DX Mongo datasets.

    Then, the dataset is to be processed as a standard DX dataset.

    :param external_dataset: An external dataset object
    :return: A string indicating the result of the download
    """
    try:
        logger.info(f"Downloading external dataset {external_dataset['name']}.")
        source = external_dataset["source"]
        if source == "Kaggle":
            kaggle_download(external_dataset)
        elif source == "World Bank":
            worldbank_download(external_dataset)
        return "Success"
    except Exception as e:
        logger.error(f"Error in external source download: {str(e)}")
        result = "Sorry, something went wrong in our dataset download. Contact the admin for more information."
    return result
