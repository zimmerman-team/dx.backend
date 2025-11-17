import copy
import datetime
import logging
import re

import datadotworld as dw
from rb_core_backend.external_sources.model import ExternalSourceModel
from rb_core_backend.external_sources.util import EXTERNAL_DATASET_FORMAT, EXTERNAL_DATASET_RESOURCE_FORMAT
from rb_core_backend.mongo import RBCoreBackendMongo
from rb_core_backend.preprocess_dataset import RBCoreDatasetPreprocessor

logger = logging.getLogger(__name__)
RE_SUB = r"[^a-zA-Z0-9]"


class DXExternalSourceDW(ExternalSourceModel):
    """Class representing a DW external source."""

    def __init__(
        self,
        mongo_client: RBCoreBackendMongo,
        dataset_preprocessor: RBCoreDatasetPreprocessor,
    ) -> None:
        super().__init__(mongo_client, dataset_preprocessor)

    def index(self, delete=False):
        """
        Indexing function for DW data.
        Using the DW API, we search for datasets.
        Then check for updates, if the object is to be updated, pass that as a boolean.

        :return: A string indicating the result of the indexing.
        """
        if delete:
            logger.info("DW:: - Removing old DW data")
            self.mongo_client.mongo_remove_data_for_external_sources("DW")
        logger.info("DW:: Indexing DW data...")
        # Get existing sources
        existing_external_sources = self.mongo_client.mongo_get_all_external_sources()
        existing_external_sources = {
            source["internalRef"]: source for source in existing_external_sources
        }
        # Get all datasets and process
        all_dataset = dw.api_client().fetch_liked_datasets()
        n_ds = 0
        n_success = 0
        for dataset in all_dataset.get("records"):
            if dataset.get("license") not in [
                "Public Domain",
                "CC0",
                "CC-BY",
                "CC-BY-SA",
                "CC-BY-NC",
                "CC-BY-NC-SA",
                "CC-BY-NC-ND",
                "CC-BY-ND",
            ]:  # NOQA: E501
                continue
            n_ds += 1
            # We use the name as the internal ref, as the id might change.
            internal_ref = dataset.get("id")
            update = False
            update_item = None
            if internal_ref in existing_external_sources:
                if existing_external_sources[internal_ref][
                    "dateSourceLastUpdated"
                ] == dataset.get("updated", ""):
                    continue
                else:
                    update = True
                    update_item = existing_external_sources[internal_ref]
            try:
                res = self._create_external_source_object(dataset, update, update_item)
                if res == "Success":
                    n_success += 1
            except Exception as e:
                logger.error(f"DW:: Failed to index dataset {internal_ref} due to: {e}")
        return f"DW - Successfully indexed {n_success} out of {n_ds} datasets."

    def _create_external_source_object(self, dataset, update=False, update_item=None):
        """
        Core functionality of indexing.
        This function creates the external source object and sends it to MongoDB.
        If update is True, the existing object is updated instead of newly inserted.

        :param dataset: The DW dataset object.
        :param update: A boolean indicating if the object should be updated instead of inserted.
        :param update_item: The existing object to update.
        :return: A string indicating the result of the operation.
        """
        if update:
            external_dataset = copy.deepcopy(update_item)
            external_dataset.pop("score", None)
            external_dataset["subCategories"] = []
            external_dataset["resources"] = []
        else:
            external_dataset = copy.deepcopy(EXTERNAL_DATASET_FORMAT)

        # Build the external dataset
        external_dataset["title"] = dataset.get("title", "")
        external_dataset["description"] = (
            f"{dataset.get('description', '')} - [Data World]."
        )
        external_dataset["source"] = "DW"
        external_dataset["URI"] = (
            f"https://data.world/{dataset.get('owner')}/{dataset.get('id')}"
        )
        external_dataset["internalRef"] = dataset.get("id", "")
        external_dataset["mainCategory"] = "Data.World"
        external_dataset["subCategories"] = dataset.get("tags", [])
        external_dataset["datePublished"] = dataset.get("created", "")
        external_dataset["dateLastUpdated"] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        external_dataset["dateSourceLastUpdated"] = dataset.get("updated", "")

        # Build and attach the resources if they are CSV
        for file in dataset.get("files"):
            if not file.get("name").endswith(".csv"):
                continue
            if file.get("size_in_bytes") > 50000000:
                continue
            external_resource = copy.deepcopy(EXTERNAL_DATASET_RESOURCE_FORMAT)
            external_resource["title"] = (
                re.sub(RE_SUB, "", file.get("name", "    ")[:-4])
                + f" - Dataset file name: {file.get('name', '')}"
            )  # NOQA: 501
            external_resource["description"] = (
                f"{file.get('name', '')} - Retrieved from Data.World."
            )
            external_resource["URI"] = (
                f"https://data.world/{dataset.get('owner')}/{dataset.get('id')}"
            )
            external_resource["internalRef"] = (
                f"{dataset.get('id', '')}/{file.get('name')}"
            )
            external_resource["format"] = "csv"
            external_resource["datePublished"] = file.get("created", "")
            external_resource["dateLastUpdated"] = datetime.datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            external_resource["dateResourceLastUpdated"] = file.get("updated", "")
            external_dataset["resources"].append(external_resource)

        if len(external_dataset["resources"]) == 0:
            return "No resources attached to this dataset."
        mongo_res = self.mongo_client.mongo_create_external_source(
            external_dataset, update=update
        )
        if mongo_res is not None:
            return "Success"
        return "MongoDB Error"

    def download(self, external_dataset):
        logger.debug("DW:: Downloading dw dataset")
        try:
            url = external_dataset["URI"]
            file_path = url.split("https://data.world/")[-1]
            name = external_dataset["title"].split(" - Dataset file")[0]
            datasets = dw.load_dataset(file_path)
            df = datasets.dataframes[name]
            try:
                res = self.dataset_preprocessor.preprocess_data(df, create_ssr=True)
            except Exception as e:
                logger.error(f"DW:: Failed to preprocess data for {url} due to: {e}")
                res = "Sorry, we were unable to process the dataset, please try a different dataset. Contact the admin for more information."  # NOQA: 501
        except Exception as e:
            logger.error(f"DW:: Failed to download file: {str(e)}")
            res = "Sorry, we were unable to download the DW Dataset, please try again later. Contact the admin if the problem persists."  # NOQA: 501
        return res
