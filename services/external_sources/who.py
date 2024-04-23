import copy
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime

import pandas as pd
import requests

from services.external_sources.util import (EXTERNAL_DATASET_FORMAT,
                                            EXTERNAL_DATASET_RESOURCE_FORMAT)
from services.mongo import (mongo_create_external_source,
                            mongo_get_all_external_sources,
                            mongo_remove_data_for_external_sources)
from services.preprocess_dataset import preprocess_data

logger = logging.getLogger(__name__)
WB_SOURCE_NOTICE = "  - This Datasource was retrieved from https://apps.who.int/gho/athena/api/GHO."


def who_index(delete=False):
    """
    Trigger the indexing of the WHO data.
    If delete is True, remove all WHO data before indexing.
    We do this, because there is no way to track updated datasets with the WHO API.

    :param delete: A boolean indicating if the WHO data should be removed before indexing.
    """
    logger.info("WHO:: Indexing WHO data...")
    if delete:
        logger.info("WHO:: - Removing old WHO data")
        mongo_remove_data_for_external_sources("WHO")
    existing_external_sources = mongo_get_all_external_sources()
    existing_external_sources = {source["internalRef"]: source for source in existing_external_sources}

    # Get WHO data
    gho_xml_url = "https://apps.who.int/gho/athena/api/GHO"
    response = requests.get(gho_xml_url)
    xml_data = response.content

    # Parse the XML data into an ElementTree
    root = ET.fromstring(xml_data)
    code_elements = root.findall('.//Metadata/Dimension/Code')
    # Iterate over the code elements and create sources
    n_ds = 0
    n_success = 0
    for code in code_elements:
        n_ds += 1
        if code.get('Label') is None:
            continue
        if code.get('Label') in existing_external_sources:
            continue
        try:
            res = _create_external_source_object(code)
            if res == "Success":
                n_success += 1
        except Exception as e:
            logger.error(f"WHO:: Failed to index dataset {code.get('Label')} due to: {e}")
    return f"WHO - Successfully indexed {n_success} out of {n_ds} datasets."


def _create_external_source_object(code):
    """
    Core subroutine to create an external source object from a WHO code element.

    :param code: The code element from the WHO API.
    """
    try:
        internal_ref = code.get('Label')
        try:
            category = code.find('.//Attr[@Category="CATEGORY"]/Value/Display').text
        except Exception:
            category = "Miscellaneous"
        try:
            title = code.find('./Display').text
        except Exception:
            title = "No title available."
        try:
            description = code.find('.//Display').text + WB_SOURCE_NOTICE
        except Exception:
            description = "No description available." + WB_SOURCE_NOTICE
        url = code.get('URL', f"https://ghoapi.azureedge.net/api/{internal_ref}")
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        external_dataset = copy.deepcopy(EXTERNAL_DATASET_FORMAT)
        external_dataset["title"] = title
        external_dataset["description"] = description
        external_dataset["source"] = "WHO"
        external_dataset["URI"] = url
        external_dataset["internalRef"] = internal_ref
        external_dataset["mainCategory"] = category
        external_dataset["subCategories"] = []
        external_dataset["datePublished"] = now
        external_dataset["dateLastUpdated"] = now
        external_dataset["dateSourceLastUpdated"] = now

        external_resource = copy.deepcopy(EXTERNAL_DATASET_RESOURCE_FORMAT)
        external_resource["title"] = title
        external_resource["description"] = description
        external_resource["URI"] = f"https://ghoapi.azureedge.net/api/{internal_ref}"
        external_resource["internalRef"] = internal_ref
        external_resource["format"] = "csv"
        external_resource["datePublished"] = now
        external_resource["dateLastUpdated"] = now
        external_resource["dateResourceLastUpdated"] = now
        external_dataset["resources"].append(external_resource)
        if len(external_dataset["resources"]) == 0:
            return "No resources attached to this dataset."
        mongo_res = mongo_create_external_source(external_dataset, update=False)
        if mongo_res is not None:
            return "Success"
        return "MongoDB Error"
    except Exception as e:
        logger.error(f"WHO:: Error creating external source object: {str(e)}")
        return "Error"


def who_download(external_dataset):
    res = "Success"
    # Download data
    url = f"https://ghoapi.azureedge.net/api/{external_dataset['name']}"
    try:
        logger.debug(f"WHO:: Downloading who dataset: {url}")
        data = requests.get(url).json()["value"]
    except Exception:
        return "Sorry, we were unable to download the WHO Dataset, please try again later. Contact the admin if the problem persists."  # NOQA: 501

    try:
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
            res = preprocess_data(dx_name, create_ssr=True)
        except Exception:
            res = "We were unable to process the dataset, please try a different dataset. Contact the admin for more information."  # NOQA: 501
        os.remove(dx_loc)
    except Exception:
        res = "We were unable to process the dataset, please try a different dataset. Contact the admin for more information."# NOQA: 501
    return res
