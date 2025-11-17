import copy
import logging
import os
from datetime import datetime

import requests
from rb_core_backend.external_sources.model import ExternalSourceModel
from rb_core_backend.external_sources.util import EXTERNAL_DATASET_FORMAT, EXTERNAL_DATASET_RESOURCE_FORMAT
from rb_core_backend.mongo import RBCoreBackendMongo
from rb_core_backend.preprocess_dataset import RBCoreDatasetPreprocessor

logger = logging.getLogger(__name__)
TGF_DEFAULT_URL = "https://data-service.theglobalfund.org/downloads"
TGF_SOURCE_NOTICE = "  - This Datasource was retrieved from https://data-service.theglobalfund.org/downloads."

TGF_DATASETS = {
    "Reported Results": {
        "description": "Global Fund results reported on an annual basis",
        "url": "https://data-service.theglobalfund.org/file_download/gf_reported_results_dataset/CSV",
    },
    "Pledges and Contributions": {
        "description": "Government, private sector, nongovernment and other donor pledges and contributions",
        "url": "https://data-service.theglobalfund.org/file_download/pledges_contributions_dataset/CSV",
    },
    "Country Eligibility": {
        "description": "Country eligibility for funding over time.",
        "url": "https://data-service.theglobalfund.org/file_download/eligibility_dataset/CSV",
    },
    "Allocations": {
        "description": "Allocations amounts for countries by disease",
        "url": "https://data-service.theglobalfund.org/file_download/allocations_dataset/CSV",
    },
    "Grant Agreements": {
        "description": "High-level grant information from across the portfolio",
        "url": "https://data-service.theglobalfund.org/file_download/grant_agreements_dataset/CSV",
    },
    "Grant Agreement Implementation Periods": {
        "description": "High-level implementation period data for all grants across the portfolio",
        "url": "https://data-service.theglobalfund.org/file_download/grant_implementation_periods_dataset/CSV",
    },
    "Grant Agreement Commitments": {
        "description": "Financial commitments for all grant agreements across the portfolio",
        "url": "https://data-service.theglobalfund.org/file_download/grant_commitments_dataset/CSV",
    },
    "Grant Agreement Disbursements": {
        "description": "Disbursement transactions for all grants across the portfolio",
        "url": "https://data-service.theglobalfund.org/file_download/grant_disbursements_dataset/CSV",
    },
    "Grant Agreement Progress Updates": {
        "description": "High-level progress update data for all grants across the portfolio",
        "url": "https://data-service.theglobalfund.org/file_download/grant_progress_updates_dataset/CSV",
    },
    "Grant Agreement Implementation Period Detailed Budgets": {
        "description": "Detailed budgets for each implementation period from the 2017-2019 Allocation Period onwards",
        "url": "https://data-service.theglobalfund.org/file_download/grant_agreement_implementation_period_detailed_budgets_dataset/CSV",  # noqa: 501
    },
    "Grant Agreement Implementation Period Performance Frameworks": {
        "description": "Indicator targets and results for each implementation period of a grant. This data cannot be aggregated",  # noqa: 501
        "url": "https://data-service.theglobalfund.org/file_download/implementationPeriodPerformanceFrameworks/CSV",
    },
}


class DXExternalSourceTGF(ExternalSourceModel):
    """Class representing an TGF external source."""

    def __init__(
        self,
        mongo_client: RBCoreBackendMongo,
        dataset_preprocessor: RBCoreDatasetPreprocessor,
    ) -> None:
        super().__init__(mongo_client, dataset_preprocessor)

    def index(self, delete=False):
        """
        Indexing function for The Global Fund data.
        Using the The Global Fund data source page we hardcode all the sources.
        There is no real way to track updated datasets, so we just index all datasets.

        :param delete: A boolean indicating if the The Global Fund data should be removed before indexing.
        :return: A string indicating the result of the indexing.
        """
        logger.info("TGF:: Indexing The Global Fund data...")
        # Get existing sources
        if delete:
            logger.info("TGF:: - Removing old The Global Fund data")
            self.mongo_client.mongo_remove_data_for_external_sources("TGF")
        existing_external_sources = self.mongo_client.mongo_get_all_external_sources()
        existing_external_sources = {
            source["internalRef"]: source for source in existing_external_sources
        }
        n_ds = 0
        n_success = 0
        for key, values in TGF_DATASETS.items():
            logger.info(f"TGF:: Indexing dataset {key}")
            n_ds += 1
            if key in existing_external_sources:
                # We do not update existing sources, as there is no date provided.
                continue
            try:
                res = self._create_external_source_object(key, values)
                if res == "Success":
                    n_success += 1
            except Exception as e:
                logger.error(f"TGF:: Failed to index dataset {key} due to: {e}")
        return f"World Bank - Successfully indexed {n_success} out of {n_ds} datasets."

    def _create_external_source_object(self, key, values):
        """
        Core subroutine to create an external source object from a World Bank metadata id.

        :param meta_id: The metadata id from the World Bank API.
        :return: A string indicating the result of the creation.
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Build the external dataset
        external_dataset = copy.deepcopy(EXTERNAL_DATASET_FORMAT)
        external_dataset["title"] = key
        external_dataset["description"] = (
            values.get("description", "") + TGF_SOURCE_NOTICE
        )
        external_dataset["source"] = "TGF"
        external_dataset["URI"] = values.get("url", TGF_DEFAULT_URL)
        external_dataset["internalRef"] = key
        external_dataset["mainCategory"] = "Financial"
        external_dataset["subCategories"] = []
        external_dataset["datePublished"] = now
        external_dataset["dateLastUpdated"] = now
        external_dataset["dateSourceLastUpdated"] = now

        external_resource = copy.deepcopy(EXTERNAL_DATASET_RESOURCE_FORMAT)
        external_resource["title"] = key
        external_resource["description"] = (
            values.get("description", "") + TGF_SOURCE_NOTICE
        )
        external_resource["URI"] = values.get("url", TGF_DEFAULT_URL)
        external_resource["internalRef"] = key
        external_resource["format"] = "csv"
        external_resource["datePublished"] = now
        external_resource["dateLastUpdated"] = now
        external_resource["dateResourceLastUpdated"] = now
        external_dataset["resources"].append(external_resource)
        if len(external_dataset["resources"]) == 0:
            return "No resources attached to this dataset."
        logger.info(f"Submitting external dataset to MongoDB {external_dataset}")
        mongo_res = self.mongo_client.mongo_create_external_source(
            external_dataset, update=False
        )
        if mongo_res is not None:
            return "Success"
        return "MongoDB Error"

    def download(self, external_dataset):
        res = "Success"
        url = external_dataset["url"]
        logger.debug(f"TGF:: Downloading TGF dataset: {url}")

        try:
            dx_id = external_dataset["id"]
            dx_name = f"dx{dx_id}.csv"
            dx_loc = f"./staging/{dx_name}"

            # download `url` to `dx_loc`
            with requests.get(url, stream=True) as r:
                r.raise_for_status()  # Check if the request was successful
                with open(dx_loc, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            try:
                res = self.dataset_preprocessor.preprocess_data(
                    dx_name, create_ssr=True
                )
            except Exception:
                return "We were unable to process the dataset, please try a different dataset. Contact the admin for more information."  # noqa
            os.remove(dx_loc)
        except Exception:
            res = "We were unable to download the dataset, please try again later."
        return res
