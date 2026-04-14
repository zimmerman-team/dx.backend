"""Huggingface external source.

With huggingface-hub package.

Creates an indexing and downloading function.

Filter for the following:
* Only selects datasets that are permissive, see PERMISSIVE_LICENSES list.
* modality is "tabular"
* format is "csv"

"""
import copy
import datetime
import logging
import os

import requests
from huggingface_hub import HfApi
from rb_core_backend.external_sources.model import ExternalSourceModel
from rb_core_backend.external_sources.util import EXTERNAL_DATASET_FORMAT, EXTERNAL_DATASET_RESOURCE_FORMAT
from rb_core_backend.mongo import RBCoreBackendMongo
from rb_core_backend.preprocess_dataset import RBCoreDatasetPreprocessor

PERMISSIVE_LICENSES = [
    "apache-2.0", "mit", "bsd-2-clause", "bsd-3-clause", "bsd-3-clause-clear", "bsd",
    "afl-3.0", "artistic-2.0", "bsl-1.0", "wtfpl", "unlicense", "zlib", "isc",
    "postgresql", "ncsa", "ms-pl", "ecl-2.0", "epl-2.0", "epl-1.0",
    "mpl-2.0", "eupl-1.1", "eupl-1.2", "osl-3.0",
    "cdla-permissive-1.0", "cdla-permissive-2.0",
    "cc0-1.0", "cc-by-2.0", "cc-by-2.5", "cc-by-3.0", "cc-by-4.0",
    "odc-by", "etalab-2.0", "ofl-1.1"
]
FORMATS = ["csv", "parquet"]
MODALITIES = ["tabular"]

HF_SOURCE_NOTICE = "  - This Datasource was retrieved from https://huggingface.co/datasets/."
HF_TABULAR_FILTER = "modality:tabular"
STAGING_DIR = "./staging"

logger = logging.getLogger(__name__)


class DXExternalSourceHuggingFace(ExternalSourceModel):
    """Class representing a HuggingFace external source."""

    def __init__(
        self,
        mongo_client: RBCoreBackendMongo,
        dataset_preprocessor: RBCoreDatasetPreprocessor,
    ) -> None:
        super().__init__(mongo_client, dataset_preprocessor)
        self.api = HfApi()

    def index(self, delete=False):
        """
        Indexing function for HuggingFace data.
        Queries the HuggingFace Hub API for tabular CSV datasets with permissive licenses.
        Checks for updates and skips unchanged datasets.

        :return: A string indicating the result of the indexing.
        """
        if delete:
            logger.info("HuggingFace:: - Removing old HuggingFace data")
            self.mongo_client.mongo_remove_data_for_external_sources("HuggingFace")
        logger.info("HuggingFace:: Indexing HuggingFace data...")
        # Get existing sources (filter to HuggingFace only for efficiency)
        existing_external_sources = self.mongo_client.mongo_get_all_external_sources()
        existing_external_sources = {
            source["internalRef"]: source
            for source in existing_external_sources
            if source.get("source") == "HuggingFace"
        }
        # Query HuggingFace for tabular datasets with CSV files
        datasets = self.api.list_datasets(
            filter=HF_TABULAR_FILTER,
            full=True,
        )
        logger.info(f"HuggingFace:: Found {datasets} datasets from HuggingFace API")
        n_ds = 0
        n_success = 0
        for dataset in datasets:
            n_ds += 1
            logger.info(f"HuggingFace:: Processing dataset nr {n_ds}...")
            # Filter by permissive license
            if not self._has_permissive_license(dataset):
                continue
            logger.info(f"HuggingFace:: Processing dataset {dataset.id} with license {dataset.card_data.license}")
            internal_ref = dataset.id
            update = False
            update_item = None
            if internal_ref in existing_external_sources:
                dataset_updated = (
                    dataset.last_modified.strftime("%Y-%m-%d %H:%M:%S") if dataset.last_modified else ""
                )
                if existing_external_sources[internal_ref].get("dateSourceLastUpdated", "") == dataset_updated:
                    continue
                update = True
                update_item = existing_external_sources[internal_ref]
            try:
                res = self._create_external_source_object(dataset, update, update_item)
                if res == "Success":
                    n_success += 1
            except Exception as e:
                logger.error(f"HuggingFace:: Failed to index dataset {internal_ref} due to: {e}")
        return f"HuggingFace - Successfully indexed {n_success} out of {n_ds} datasets."

    def _has_permissive_license(self, dataset) -> bool:
        """Check whether a HuggingFace DatasetInfo has a permissive license."""
        if dataset.card_data is None:
            return False
        license_value = getattr(dataset.card_data, "license", None)
        if license_value is None:
            return False
        # license can be a string or a list of strings
        if isinstance(license_value, list):
            return any(lic in PERMISSIVE_LICENSES for lic in license_value)
        return license_value in PERMISSIVE_LICENSES

    def _create_external_source_object(self, dataset, update=False, update_item=None):
        """
        Core indexing functionality.
        Builds a MongoDB document from HuggingFace DatasetInfo and persists it.

        :param dataset: A HuggingFace DatasetInfo object.
        :param update: Whether to update an existing document instead of inserting.
        :param update_item: The existing MongoDB document to update.
        :return: "Success" or an error string.
        """
        # Find CSV files in the repository
        try:
            repo_files = list(self.api.list_repo_files(dataset.id, repo_type="dataset"))
        except Exception as e:
            logger.error(f"HuggingFace:: Failed to list files for {dataset.id}: {e}")
            return f"Failed to list files: {e}"
        data_files = [f for f in repo_files if f.lower().endswith(tuple(FORMATS))]
        if not data_files:
            return "No CSV or Parquet resources attached to this dataset."

        if update:
            external_dataset = copy.deepcopy(update_item)
            external_dataset.pop("score", None)
            external_dataset["subCategories"] = []
            external_dataset["resources"] = []
        else:
            external_dataset = copy.deepcopy(EXTERNAL_DATASET_FORMAT)

        # Derive a human-readable title from card data if available
        pretty_name = None
        if dataset.card_data is not None:
            pretty_name = getattr(dataset.card_data, "pretty_name", None)
        title = pretty_name or dataset.id

        description = title
        try:
            description = (getattr(dataset.card_data, "description", None) or title) + HF_SOURCE_NOTICE
        except Exception:
            description = title + HF_SOURCE_NOTICE

        date_updated = dataset.last_modified.strftime("%Y-%m-%d %H:%M:%S") if dataset.last_modified else ""
        date_created = dataset.created_at.strftime("%Y-%m-%d %H:%M:%S") if dataset.created_at else ""

        external_dataset["title"] = title
        external_dataset["description"] = description
        external_dataset["source"] = "HuggingFace"
        external_dataset["URI"] = f"https://huggingface.co/datasets/{dataset.id}"
        external_dataset["internalRef"] = dataset.id
        external_dataset["mainCategory"] = "Tabular"
        external_dataset["subCategories"] = []
        external_dataset["datePublished"] = date_created
        external_dataset["dateLastUpdated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        external_dataset["dateSourceLastUpdated"] = date_updated

        # Attach one resource per data file (CSV or Parquet)
        for data_file in data_files:
            ext = data_file.rsplit(".", 1)[-1].lower()
            external_resource = copy.deepcopy(EXTERNAL_DATASET_RESOURCE_FORMAT)
            external_resource["title"] = f"{dataset.id} - {data_file}"
            external_resource["description"] = f"{data_file}{HF_SOURCE_NOTICE}"
            external_resource["URI"] = f"https://huggingface.co/datasets/{dataset.id}/resolve/main/{data_file}"
            external_resource["internalRef"] = f"{dataset.id}/{data_file}"
            external_resource["format"] = ext
            external_resource["datePublished"] = date_created
            external_resource["dateLastUpdated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            external_resource["dateResourceLastUpdated"] = date_updated
            external_dataset["resources"].append(external_resource)

        mongo_res = self.mongo_client.mongo_create_external_source(external_dataset, update=update)
        if mongo_res is not None:
            return "Success"
        return "MongoDB Error"

    def download(self, external_dataset):
        """
        Download a HuggingFace CSV resource and preprocess it.

        Receives a payload shaped like:
          { "url": "https://huggingface.co/datasets/{dataset_id}",
            "name": "{title} - Data file: {dataset_id} - {filename}", ... }

        Extracts dataset_id from url and filename from name, then builds the
        direct resolve URL: .../datasets/{dataset_id}/resolve/main/{filename}

        :param external_dataset: Resource payload from the frontend/rb_core_backend.
        :return: "Success" or an error string.
        """
        error_msg = (
            "Sorry, we were unable to download the HuggingFace Dataset, "
            "please try again later. Contact the admin if the problem persists."
        )
        logger.debug("HuggingFace:: Downloading HuggingFace dataset")
        try:
            dx_id = external_dataset.get("id", "") or "0tmp0"

            # Extract dataset_id from the dataset page URL
            page_url = external_dataset.get("url", "")
            if not page_url:
                logger.error("HuggingFace:: No url found in external_dataset")
                return error_msg
            dataset_id = page_url.replace("https://huggingface.co/datasets/", "").strip("/")

            # Extract filename from name: "{title} - Data file: {dataset_id} - {filename}"
            name = external_dataset.get("name", "")
            separator = f" - Data file: {dataset_id} - "
            if separator not in name:
                logger.error(f"HuggingFace:: Could not parse filename from name: {name!r}")
                return error_msg
            filename = name.split(separator, 1)[1]

            download_url = f"https://huggingface.co/datasets/{dataset_id}/resolve/main/{filename}"
            logger.info(f"HuggingFace:: Downloading {download_url}")

            ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "csv"
            os.makedirs(STAGING_DIR, exist_ok=True)
            dx_name = f"{dx_id}.{ext}"
            filepath = os.path.join(STAGING_DIR, dx_name)

            response = requests.get(download_url, timeout=120)
            if response.status_code != 200:
                logger.error(f"HuggingFace:: Failed to download from {download_url}, status {response.status_code}")
                return error_msg
            with open(filepath, "wb") as f:
                f.write(response.content)
            logger.info(f"HuggingFace:: File downloaded successfully: {filepath}")

            try:
                res = self.dataset_preprocessor.preprocess_data(dx_name, create_ds=True)
            except Exception as e:
                logger.error(f"HuggingFace:: Failed to preprocess {dx_name} due to: {e}")
                res = "Sorry, we were unable to process the dataset, please try a different dataset."
            finally:
                if os.path.exists(filepath):
                    os.remove(filepath)
            return res
        except Exception as e:
            logger.error(f"HuggingFace:: Failed to download file: {str(e)}")
            return error_msg
