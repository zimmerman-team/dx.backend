# A global that contains the names of all implemented external sources.
ALL_SOURCES = ["Kaggle", "World Bank"]
# A global representation of the format of any external dataset.
EXTERNAL_DATASET_FORMAT = {
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
