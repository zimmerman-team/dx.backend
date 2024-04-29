# A global that contains the names of all implemented external sources.
ALL_SOURCES = ["Kaggle", "World Bank", "WHO", "HDX"]
# A global representation of the format of any external dataset.

# Definition of the external dataset
EXTERNAL_DATASET_FORMAT = {
   "title": "",  # External source dataset name
   "description": "",  # External source dataset description
   "source": "",  # External source name (WB, WHO, Kaggle, HDX)
   "URI": "",  # URL to read more about the dataset
   "internalRef": "",  # Internal reference to the dataset from the datasource
   "mainCategory": "",  # Category in which to place the dataset
   "subCategories": [],  # list of subcategories of the dataset
   "datePublished": "",  # Date the dataset was published
   "dateLastUpdated": "",  # Date this object was last updated
   "dateSourceLastUpdated": "",  # Date the dataset was last updated
   "resources": [],  # List of resources, in case there are multiple.
   "connectedDataset": [],  # Connected dataset objects, can be collections in the future
   "owner": "externalSource",
   "authId": "externalSource",
   "public": False
}

# Definition of the external dataset resource
EXTERNAL_DATASET_RESOURCE_FORMAT = {
    "title": "",  # Resource name
    "description": "",  # Resource description
    "URI": "",  # URL to download the resource
    "internalRef": "",  # Internal reference to the resource from the datasource
    "format": "",  # Format of the resource
    "datePublished": "",  # Date the resource was published
    "dateLastUpdated": "",  # Date the resource was last updated in DX
    "dateResourceLastUpdated": "",  # Date the resource was last updated in its source.
    "owner": "externalSource",
    "authId": "externalSource",
    "public": False
}

# Legacy format for external datasets (deprecated)
LEGACY_EXTERNAL_DATASET_FORMAT = {
   "name": "",
   "description": "",
   "source": "",
   "url": "",
   "category": "",
   "datePublished": "",
   "owner": "externalSource",
   "authId": "externalSource",
   "public": False
}


def list_shuffle_sorted(results_list, limit):
    """
    Results_list is a list of lists. Convert it to a single list,
    where we take the n-th item of each sublist.
    [[1,5,9,13,15], [2,6,10,14], [3,7,11], [4,8,12]] will be
    converted to [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]

    :param results_list: A list of lists
    :param limit: The maximum number of results per source
    """
    try:
        results = []
        for i in range(limit):
            for result in results_list:
                if len(result) > i:
                    results.append(result[i])
        return results
    except Exception:
        return []
