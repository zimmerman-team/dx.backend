import copy
import datetime
import logging
from urllib.parse import parse_qs, unquote, urlparse

import pandas as pd
from rb_core_backend.external_sources.model import ExternalSourceModel
from rb_core_backend.external_sources.util import EXTERNAL_DATASET_FORMAT, EXTERNAL_DATASET_RESOURCE_FORMAT
from rb_core_backend.mongo import RBCoreBackendMongo
from rb_core_backend.preprocess_dataset import RBCoreDatasetPreprocessor

logger = logging.getLogger(__name__)
RE_SUB = r"[^a-zA-Z0-9]"
OECD_COLS = [
    "OECD.Stat Dataset code",
    "OECD.Stat Dataset name (EN)",
    "OECD.Stat Dataset name (FR)",
    "OECD.Stat url",
    "OECD Data Explorer dataset name (EN)",
    "OECD Data Explorer dataset name (FR)",
    "OECD Data Explorer url",
]


class DXExternalSourceOECD(ExternalSourceModel):
    """Class representing an OECD external source."""

    def __init__(
        self,
        mongo_client: RBCoreBackendMongo,
        dataset_preprocessor: RBCoreDatasetPreprocessor,
    ) -> None:
        super().__init__(mongo_client, dataset_preprocessor)

    def index(self, delete=True):
        """
        Indexing function for OECD data.
        Using the OECD API, we search for datasets.
        Then check for updates, if the object is to be updated, pass that as a boolean.

        :return: A string indicating the result of the indexing.
        """
        if delete:
            logger.info("OECD:: - Removing old OECD data")
            self.mongo_client.mongo_remove_data_for_external_sources("OECD")
        logger.info("OECD:: Indexing OECD data...")
        # Get datasets
        url = "https://gitlab.com/sis-cc/topologies/oecd-migration/-/raw/main/OECDDatasetsCorrespondence.xlsx"
        df = pd.read_excel(
            url, header=5
        )  # Drop first 12 rows, as the datasets start at 13
        df = df.iloc[:, :-1]  # Drop the last column, as they are unused references
        n_ds = 0
        n_success = 0
        # for row in df.iterrows():
        for i in range(len(df)):
            row = df.iloc[i]
            n_ds += 1
            # We use the name as the internal ref, as the id might change.
            internal_ref = row[OECD_COLS[0]]
            try:
                res = self._create_external_source_object(row)
                if res == "Success":
                    n_success += 1
            except Exception as e:
                logger.error(
                    f"OECD:: Failed to index dataset {internal_ref} due to: {e}"
                )
        return f"OECD - Successfully indexed {n_success} out of {n_ds} datasets."

    def _create_external_source_object(self, dataset):
        """
        Core functionality of indexing.
        This function creates the external source object and sends it to MongoDB.

        :param dataset: The OECD dataset object.
        :return: A string indicating the result of the operation.
        """
        external_dataset = copy.deepcopy(EXTERNAL_DATASET_FORMAT)
        _current_year = datetime.datetime.now().year
        _title = dataset[OECD_COLS[1]]
        _desc = f"{dataset[OECD_COLS[4]]} - [OECD ({_current_year}), {OECD_COLS[1]}, {OECD_COLS[6]}]."
        # Build the external dataset
        external_dataset["title"] = _title
        external_dataset["description"] = _desc
        external_dataset["source"] = "OECD"
        external_dataset["URI"] = dataset[OECD_COLS[6]]
        external_dataset["internalRef"] = dataset[OECD_COLS[0]]
        external_dataset["mainCategory"] = "OECD"
        external_dataset["subCategories"] = []
        external_dataset["datePublished"] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        external_dataset["dateLastUpdated"] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        external_dataset["dateSourceLastUpdated"] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # Build and attach the resources if they are CSV
        _input = dataset[OECD_COLS[6]]
        if "df[ag]=" not in _input or "df[id]=" not in _input:
            return "Invalid link"
        _end = _input.split("df[ag]=")[1]
        _id = _input.split("df[id]=")[1].split("&")[0]
        resource_url = f"https://sdmx.oecd.org/public/rest/data/{_end},{_id},/all?dimensionAtObservation=AllDimensions&format=csvfilewithlabels"  # NOQA: 501

        resource_url = f"https://sdmx.oecd.org/public/rest/data/{_end},{_id}/all?dimensionAtObservation=AllDimensions&format=csvfilewithlabels"  # NOQA: 501
        external_resource = copy.deepcopy(EXTERNAL_DATASET_RESOURCE_FORMAT)
        external_resource["title"] = _title
        external_resource["description"] = _desc
        external_resource["URI"] = resource_url
        external_resource["internalRef"] = dataset[OECD_COLS[0]]
        external_resource["format"] = "csv"
        external_resource["datePublished"] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        external_resource["dateLastUpdated"] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        external_resource["dateResourceLastUpdated"] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        external_dataset["resources"].append(external_resource)

        if len(external_dataset["resources"]) == 0:
            return "No resources attached to this dataset."
        mongo_res = self.mongo_client.mongo_create_external_source(external_dataset)
        if mongo_res is not None:
            return "Success"
        return "MongoDB Error"

    def download(self, external_dataset):
        logger.debug("OECD:: Downloading oecd dataset")
        try:
            url = self._convert_oecd_url(external_dataset["url"])
            df = pd.read_csv(url)
            try:
                res = self.dataset_preprocessor.preprocess_data(df, create_ssr=True)
            except Exception as e:
                logger.error(f"OECD:: Failed to preprocess data for {url} due to: {e}")
                res = "Sorry, we were unable to process the dataset, please try a different dataset. Contact the admin for more information."  # NOQA: 501
        except Exception as e:
            logger.error(f"OECD:: Failed to download file: {str(e)}")
            res = "Sorry, we were unable to download the OECD Dataset, please try again later. Contact the admin if the problem persists."  # NOQA: 501
        return res

    @staticmethod
    def _get_end_and_id_from_url(_input):
        try:
            _end = _input.split("df[ag]=")[1]
            _id = _input.split("df[id]=")[1].split("&")[0]
        except IndexError:
            _end = None
            _id = None
        if not _end or not _id:
            try:
                _end = _input.split("df%5bag%5d=")[1].split("&")[0]
                _id = _input.split("df%5bid%5d=")[1].split("&")[0]
            except IndexError:
                _end = None
                _id = None
        if not _end or not _id:
            try:
                _end = _input.split("dataflow[datasourceId]=")[1].split("&")[0]
                _id = _input.split("dataflow[agencyId]=")[1].split("&")[0]
            except IndexError:
                _end = None
                _id = None
        return _end, _id

    @staticmethod
    def _convert_oecd_url(original_url):
        # Parse the URL and extract the query string parameters
        parsed_url = urlparse(original_url)
        query_params = parse_qs(parsed_url.query)

        # Extract required parts
        df_ag = query_params.get("df[ag]", [None])[0]
        df_id = query_params.get("df[id]", [None])[0]

        # Decode and sanitize
        if df_ag is None or df_id is None:
            df_ag = query_params.get("df%5bag%5d", [None])[0]
            df_id = query_params.get("df%5bid%5d", [None])[0]
        if df_ag is None or df_id is None:
            df_ag = query_params.get("dataflow[agencyId]", [None])[0]
            df_id = query_params.get("dataflow[dataflowId]", [None])[0]
        if df_ag is None or df_id is None:
            raise ValueError(
                "Required parameters 'df[ag]' and 'df[id]' not found in the URL"
            )
        df_id = unquote(df_id)
        df_ag = unquote(df_ag)

        # Construct new SDMX API URL
        new_url = (
            f"https://sdmx.oecd.org/public/rest/data/"
            f"{df_ag},{df_id}"
            f"?startPeriod=2020&endPeriod=2025&dimensionAtObservation=AllDimensions&format=csvfilewithlabels"
        )

        return new_url
