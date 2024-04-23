import logging

from dotenv import load_dotenv

from services.external_sources._hdx import hdx_download
from services.external_sources.index import external_search
from services.external_sources.kaggle import kaggle_download
from services.external_sources.util import ALL_SOURCES
from services.external_sources.who import who_download
from services.external_sources.worldbank import worldbank_download

logger = logging.getLogger(__name__)
load_dotenv()

DEFAULT_SEARCH_TERM = "world population"


def search_external_sources(query, sources=ALL_SOURCES, legacy=False):
    """
    The search feature triggers the search in MongoDB, and returns the appropriate results.

    :param query: The query to search for.
    :param sources: A list of sources to search through.
    :param legacy: A boolean indicating if the search should be returned in the legacy format (deprecated).
    :return: A list of results in the form of an ExternalSource object.
    """
    if query == "":
        query = DEFAULT_SEARCH_TERM
    try:
        result = external_search(query, sources, legacy=legacy)
    except Exception as e:
        logger.error(f"Error in external source search: {str(e)}")
        result = "Sorry, we were unable to search the external sources, please try again with a different search term, or contact the admin for more information."  # noqa
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
    result = "Sorry, we could not find the data source, please contact the admin for more information."
    try:
        logger.info(f"Downloading external dataset {external_dataset['name']}.")
        source = external_dataset["source"]
        if source == "Kaggle":
            result = kaggle_download(external_dataset)
        if source == "World Bank":
            result = worldbank_download(external_dataset)
        elif source == "WHO":
            result = who_download(external_dataset)
        elif source == "HDX":
            result = hdx_download(external_dataset)
    except Exception as e:
        logger.error(f"Error in external source download: {str(e)}")
        result = "Sorry, we were unable to download your selected file. Contact the admin for more information."
    return result
