import json
import logging
import random
import time

import pandas as pd
import requests

from services.preprocess_dataset import preprocess_data
from services.ssr import create_ssr_parsed_file


def external_iati_search(query, selected_fields):
    logging.debug(f"IATI:: Searching for IATI data with query: {query}, and selected fields: {selected_fields}")
    cols, fl = _read_iati_source_json(selected_fields)

    ai_api = "http://ai-api-dev:5000/prompts/iati-cloud-external-search"
    data = {'prompt': query}
    headers = {'Authorization': 'vMIpghXkqnwbi0GaRjdOu2YR8bUqIDp1'}
    try:
        response = requests.post(ai_api, data=data, headers=headers)
        if response.status_code == 200:
            url = response.json()['result']
            url += f"&fl={','.join(fl)}&rows=1000"
        else:
            print(f"Failed with status code: {response.status_code} {response.text}")
            return 500, "Failed to retrieve the IATI query, try a different query. If this happens again, contact the administrator"  # NOQA: 501
    except Exception:
        return 500, "Failing to retrieve the IATI query, try a different query. If this happens again, contact the administrator"  # NOQA: 501

    ds_id = _object_id()
    status, data = _fetch_iati(url)
    if status == "OK":
        status, _ = _parse_iati(data, ds_id, fl, cols)
    if status == "OK":
        status = preprocess_data(f"dx{ds_id}.csv", create_ssr=True)
    code = 200 if status == "Success" else 500
    return code, status + f" for the query: {url} - datasetId: {ds_id}"


def iati_direct_data(iati_url):
    logging.debug(f"IATI:: Fetching IATI data from {iati_url}")
    cols, fl = _read_iati_source_json([""])
    status, data = _fetch_iati(iati_url)
    if status == "OK":
        status, df = _parse_iati(data, None, fl, cols, save=False)
    if status == "OK":
        # convert pandas dataframe df to a json object
        data_dict = create_ssr_parsed_file(df, save=False)
        # save the json object to a file "test.json"
        return 200, data_dict


def _fetch_iati(url):
    logging.debug(f"IATI:: Fetch IATI data from {url}")
    try:
        response = requests.get(url)
    except Exception:
        print("Error fetching data from the API")
        return "Error fetching data from the API", None
    try:
        data = response.json()['response']['docs']
        return "OK", data
    except Exception:
        print("Error parsing data from the API")
        return "Error parsing data from the API", None


def _parse_iati(data, ds_id, fl, cols, save=True):
    logging.debug("IATI:: Parsing IATI data to CSV")
    try:
        rows = []
        for activity in data:
            row = []
            for field in fl:
                value = activity.get(field, pd.NA)
                if isinstance(value, list):
                    value = "; ".join(value)
                row.append(value)
            rows.append(row)

        df = pd.DataFrame(rows, columns=cols)
        df = df.dropna(axis=1, how="all")
        if save:
            df.to_csv(f"./staging/dx{ds_id}.csv", index=False)
            return "OK", None
        else:
            return "OK", df
    except Exception:
        return "Error parsing IATI data", None


def _hex_value(value):
    return hex(int(value))[2:]  # Remove the '0x' prefix from hex representation


def _object_id():
    logging.debug("IATI:: Generating an objectID")
    # Get the current time in seconds and convert it to hex
    timestamp_hex = _hex_value(time.time())
    # Generate 16 random hex characters
    random_hex = ''.join(_hex_value(random.random() * 16) for _ in range(16))
    return timestamp_hex + random_hex


def _read_iati_source_json(field_ids):
    with open("./iati_source.json", "r") as f:
        data = json.load(f)
        cols = []
        fl = []
        if field_ids == [""]:
            for d in data:
                if d["default"]:
                    cols.append(d["name"])
                    fl.append(d["iati-tag"])
        else:
            for d in data:
                if d["id"] in field_ids:
                    cols.append(d["name"])
                    fl.append(d["iati-tag"])
        return cols, fl
