import copy
import logging
import os
from datetime import datetime

import wbgapi as wb
from rb_core_backend.external_sources.model import ExternalSourceModel
from rb_core_backend.external_sources.util import EXTERNAL_DATASET_FORMAT, EXTERNAL_DATASET_RESOURCE_FORMAT
from rb_core_backend.mongo import RBCoreBackendMongo
from rb_core_backend.preprocess_dataset import RBCoreDatasetPreprocessor

logger = logging.getLogger(__name__)
WB_SOURCE_NOTICE = "  - This Datasource was retrieved from https://data.worldbank.org/."


class DXExternalSourceWB(ExternalSourceModel):
    """Class representing an WB external source."""

    def __init__(
        self,
        mongo_client: RBCoreBackendMongo,
        dataset_preprocessor: RBCoreDatasetPreprocessor,
    ) -> None:
        super().__init__(mongo_client, dataset_preprocessor)

    def index(self, delete=False):
        """
        Indexing function for World Bank data.
        Using the World Bank API, we search for datasets.
        There is no real way to track updated datasets, so we just index all datasets.

        :param delete: A boolean indicating if the World Bank data should be removed before indexing.
        :return: A string indicating the result of the indexing.
        """
        logger.info("WB:: Indexing World Bank data...")
        # Get existing sources
        if delete:
            logger.info("WB:: - Removing old World Bank data")
            self.mongo_client.mongo_remove_data_for_external_sources("World Bank")
        existing_external_sources = self.mongo_client.mongo_get_all_external_sources()
        existing_external_sources = {
            source["internalRef"]: source for source in existing_external_sources
        }
        # Get all datasets and process
        search_meta = wb.series.list()
        n_ds = 0
        n_success = 0
        for meta in search_meta:
            n_ds += 1
            meta_id = meta.get("id", None)
            if meta_id is None:
                continue
            if meta_id in existing_external_sources:
                # We do not update existing sources, as there is no date provided.
                continue
            try:
                res = self._create_external_source_object(meta_id)
                if res == "Success":
                    n_success += 1
            except Exception as e:
                logger.error(f"WB:: Failed to index dataset {meta_id} due to: {e}")
        return f"World Bank - Successfully indexed {n_success} out of {n_ds} datasets."

    def _create_external_source_object(self, meta_id):
        """
        Core subroutine to create an external source object from a World Bank metadata id.

        :param meta_id: The metadata id from the World Bank API.
        :return: A string indicating the result of the creation.
        """
        dataset = wb.series.metadata.get(meta_id).__dict__["metadata"]

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Build the external dataset
        external_dataset = copy.deepcopy(EXTERNAL_DATASET_FORMAT)
        external_dataset["title"] = dataset.get("IndicatorName", "")
        external_dataset["description"] = (
            dataset.get("Longdefinition", "") + WB_SOURCE_NOTICE
        )
        external_dataset["source"] = "World Bank"
        external_dataset["URI"] = f"https://data.worldbank.org/indicator/{meta_id}"
        external_dataset["internalRef"] = meta_id
        external_dataset["mainCategory"] = dataset.get("Topic", "")
        external_dataset["subCategories"] = []
        external_dataset["datePublished"] = now
        external_dataset["dateLastUpdated"] = now
        external_dataset["dateSourceLastUpdated"] = now

        external_resource = copy.deepcopy(EXTERNAL_DATASET_RESOURCE_FORMAT)
        external_resource["title"] = dataset.get("IndicatorName", "")
        external_resource["description"] = (
            dataset.get("Longdefinition", "") + WB_SOURCE_NOTICE
        )
        external_resource["URI"] = (
            f"https://api.worldbank.org/v2/en/indicator/{meta_id}?downloadformat=csv"
        )
        external_resource["internalRef"] = meta_id
        external_resource["format"] = "csv"
        external_resource["datePublished"] = now
        external_resource["dateLastUpdated"] = now
        external_resource["dateResourceLastUpdated"] = now
        external_dataset["resources"].append(external_resource)
        if len(external_dataset["resources"]) == 0:
            return "No resources attached to this dataset."
        mongo_res = self.mongo_client.mongo_create_external_source(
            external_dataset, update=False
        )
        if mongo_res is not None:
            return "Success"
        return "MongoDB Error"

    def download(self, external_dataset):
        res = "Success"
        url = external_dataset["url"]
        logger.debug(f"WB:: Downloading worldbank dataset: {url}")

        try:
            meta_id = url.split("https://data.worldbank.org/indicator/")[-1]
        except Exception:
            return "The dataset source was malformed, please try a different dataset."
        try:
            df = wb.data.DataFrame(meta_id, mrv=3)
            df.reset_index()
            # transpose, WB data is in form country,year1,year2,year3.
            # transpose to year,country
            df = df.T
            # Reset the index to bring 'economy' back as a column
            df.reset_index(inplace=True)
            # Rename the column to 'Year'
            df.rename(columns={"index": "Year"}, inplace=True)
            # Melt the dataframe to year,country,value format
            df = df.melt(id_vars=["Year"], var_name="Country", value_name="Value")
            # drop na in value
            df = df.dropna()
            df["Year"] = df["Year"].str[2:]

            # save df as a csv file
            dx_id = external_dataset["id"]
            dx_name = f"dx{dx_id}.csv"
            dx_loc = f"./staging/{dx_name}"
            df.to_csv(dx_loc, index=False)
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
