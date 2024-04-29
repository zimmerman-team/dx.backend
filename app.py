# Imports
import logging

from dotenv import load_dotenv
from flask import Flask, request

from services.external_sources.external_sources import (
    download_external_source, search_external_sources)
from services.external_sources.index import external_search_index
from services.preprocess_dataset import preprocess_data
from services.ssr import (duplicate_ssr_parsed_files, load_parsed_data,
                          load_sample_data, remove_ssr_parsed_files)
from services.util import remove_files
from util.api import json_return
from util.configure_logging import confirm_logger
from services.mongo import mongo_create_text_index_for_external_sources
# Load the environment variables
load_dotenv()
# Set up the flask app
app = Flask(__name__)

# Setup and confirm the logger
confirm_logger()
# Ensure we always have a text index for FederatedSearchIndex
mongo_create_text_index_for_external_sources()


"""
DX Processing
"""


@app.route('/health-check', methods=['GET'])
def health_check():
    return json_return(500, 'OK')


@app.route('/upload-file/<string:ds_name>', methods=['GET', 'POST'])
def process_dataset(ds_name):
    logging.debug(f"route: /upload-file/<string:ds_name> - Processing dataset {ds_name}")
    try:
        # Preprocess
        preprocess_res = preprocess_data(ds_name, create_ssr=True)
        if preprocess_res != "Success":
            return json_return(500, preprocess_res)
        # Create a solr core and post the dataset
        # res = post_data_to_solr(ds_name)  # TODO: Disabled solr until data processing required
        # Remove the processed file
        remove_res = remove_files([ds_name])
        code = 200 if remove_res == "Success" else 500
        res = json_return(code, remove_res)
    except Exception as e:
        logging.error(f"Error in route: /upload-file/<string:ds_name> - {str(e)}")
        res = json_return(500, "Sorry, something went wrong in our dataset processing. Contact the admin for more information.")  # noqa: E501
    return res


@app.route('/duplicate-dataset/<string:ds_name>/<string:new_ds_name>', methods=['GET', 'POST'])
def duplicate_dataset(ds_name, new_ds_name):
    logging.debug(f"route: /duplicate-dataset/<string:ds_name>/<string:new_ds_name> - Duplicating dataset {ds_name} to {new_ds_name}")  # noqa: E501
    try:
        res = duplicate_ssr_parsed_files(ds_name, new_ds_name)
    except Exception as e:
        logging.error(f"Error in route: /duplicate-dataset/<string:ds_name>/<string:new_ds_name> - {str(e)}")
        res = "Sorry, something went wrong in our dataset duplication. Contact the admin for more information."
    code = 200 if res == "Success" else 500
    return json_return(code, res)


@app.route('/duplicate-datasets', methods=['GET', 'POST'])
def duplicate_datasets():
    """
    Duplicate a list of datasets

    body: A list of datasets to be duplicated in the format:
    [
        {
            "ds_name": "dataset1",
            "new_ds_name": "new_dataset1"
        },
        {
            "ds_name": "dataset2",
            "new_ds_name": "new_dataset2"
        }
    ]
    """
    data = request.get_json()
    logging.debug(f"route: /duplicate-datasets - Duplicating dataset {len(data)} datasets")  # noqa: E501
    try:
        errors = []
        for ds in data:
            res = duplicate_ssr_parsed_files(ds['ds_name'], ds['new_ds_name'])
            if res != "Success":
                errors.append(ds['ds_name'])

        if len(errors) > 0:
            res = f"Sorry, something went wrong in our dataset duplication for {len(errors)} dataset(s). Contact the admin for more information."  # noqa: E501
    except Exception as e:
        logging.error(f"Error in route: /duplicate-datasets - {str(e)}")
        res = "Sorry, something went wrong in our dataset duplication. Contact the admin for more information."
    code = 200 if res == "Success" else 500
    return json_return(code, res)


@app.route('/upload-file/<string:ds_name>/<string:table>', methods=['GET', 'POST'])
def process_dataset_sqlite(ds_name, table):
    logging.debug(f"route: /upload-file/<string:ds_name>/<string:table> - Processing dataset {ds_name} with table {table}")  # noqa: E501
    try:
        # Preprocess
        preprocess_data(ds_name, create_ssr=True, table=table)
        remove_files([ds_name])
        res = "Success"
    except Exception as e:
        logging.error(f"Error in route: /upload-file/<string:ds_name>/<string:table> - {str(e)}")
        res = "Sorry, something went wrong in our sqlite dataset processing. Contact the admin for more information."
    code = 200 if res == "Success" else 500
    return json_return(code, res)


@app.route('/upload-file/<string:ds_name>/<string:username>/<string:password>/<string:host>/<string:port>/<string:database>/<string:table>', methods=['GET', 'POST'])  # noqa: E501
def process_dataset_sql(ds_name, username, password, host, port, database, table):
    logging.debug(f"route: /upload-file/<string:ds_name>/<string:table> - Processing dataset {ds_name} with table {table} @ {host}:{port}/{database}")  # noqa: E501
    try:
        # Preprocess
        db = {
            'username': username,
            'password': password,
            'host': host,
            'port': port,
            'database': database,
            'table': table
        }
        res = preprocess_data(ds_name, create_ssr=True, db=db)
    except Exception as e:
        logging.error(f"Error in route: /upload-file/<string:ds_name>/<string:table> - {str(e)}")
        res = "Sorry, something went wrong in our sql dataset processing. Contact the admin for more information."
    code = 200 if res == "Success" else 500
    return json_return(code, res)


@app.route('/upload-file/<string:ds_name>/<string:api_url>/<string:json_root>/<string:xml_root>', methods=['GET', 'POST'])  # noqa: E501
def process_dataset_api(ds_name, api_url, json_root, xml_root):
    logging.debug(f"route: /upload-file/<string:ds_name>/<string:api_url> - Processing dataset {ds_name} with api_url: {api_url}, json_root: {json_root}, xml_root: {xml_root}")  # noqa: E501
    try:
        # Preprocess
        api = {
            'api_url': api_url,
            'json_root': json_root,
            'xml_root': xml_root,
        }
        preprocess_data(ds_name, create_ssr=True, api=api)
        res = "Success"
    except Exception as e:
        logging.error(f"Error in route: /upload-file/<string:ds_name>/<string:table> - {str(e)}")
        res = "Sorry, something went wrong in our api dataset processing. Contact the admin for more information."
    return res


@app.route('/delete-dataset/<string:ds_name>', methods=['GET', 'POST'])
def delete_dataset(ds_name):
    """
    Delete the dataset's content from the SSR data folders

    :param ds_name: The name of the dataset to be deleted
    :return: A string indicating the result of the deletion
    """
    logging.debug(f"route: /delete-dataset/<string:ds_name> - Deleting dataset {ds_name}")
    try:
        # Remove the dataset from the datasets list in solr
        # this is in case the dataset was created through the update feature
        # remove_from_core('ref', ds_name, 'datasets')  # TODO: Disabled solr until data processing required

        # Delete the solr core belonging to the dataset
        # delete_solr_core(ds_name)  # TODO: Disabled solr until data processing required

        # Remove the dataset from SSR
        res = remove_ssr_parsed_files(ds_name)
    except Exception as e:
        logging.error(f"Error in route: /delete-dataset/<string:ds_name> - {str(e)}")
        res = "Sorry, something went wrong in our dataset deletion. Contact the admin for more information."
    code = 200 if res == "Success" else 500
    return json_return(code, res)


@app.route('/sample-data/<string:ds_name>', methods=['GET', 'POST'])
def sample_data(ds_name):
    """
    Return sample data for a given dataset id
    """
    logging.debug(f"route: /sample-data/<string:ds_name> - Sampling dataset {ds_name}")
    try:
        res = load_sample_data(ds_name)
    except Exception as e:
        logging.error(f"Error in route: /sample-data/<string:ds_name> - {str(e)}")
        res = "Sorry, something went wrong in our dataset sampling. Contact the admin for more information."
    code = 200 if not isinstance(res, str) else 500
    return json_return(code, res)


@app.route('/dataset/<string:ds_name>', methods=['GET', 'POST'])
def get_dataset(ds_name):
    """
    Return the dataset for a given dataset id
    """
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', 10))

    logging.debug(f"route: /dataset/<string:ds_name> - Getting dataset {ds_name}")
    try:
        res = load_parsed_data(ds_name, page, page_size)
    except Exception as e:
        logging.error(f"Error in route: /dataset/<string:ds_name> - {str(e)}")
        res = "Sorry, something went wrong in our dataset retrieval. Contact the admin for more information."
    return res


"""
External data sources
"""


# Index
@app.route('/external-sources/index', methods=['GET'])
def external_sources_index():
    logging.debug("route: /external-sources/index - Indexing external sources")
    try:
        res = external_search_index()
    except Exception as e:
        logging.error(f"Error in route: /external-sources/index - {str(e)}")
        res = "Sorry, something went wrong in our external source indexing. Contact the admin for more information."
    code = 200 if res == "Indexing successful" else 500
    return json_return(code, res)


# Search
@app.route('/external-sources/search', methods=['POST'])
def external_source_search():
    data = request.get_json()
    query = data.get('query')
    logging.debug(f"route: /external-sources/search/<string:query> - Searching external sources for {query}")
    try:
        res = search_external_sources(query, legacy=True)
    except Exception as e:
        logging.error(f"Error in route: /external-sources/search/<string:query> - {str(e)}")
        res = "Sorry, something went wrong in our external source search. Contact the admin for more information."
    code = 200 if not isinstance(res, str) else 500
    return json_return(code, res)


# Search for a limited number of results.
@app.route('/external-sources/search-limited', methods=['POST'])
def external_source_search_limited():
    data = request.get_json()
    query = data.get('query', '')
    source = data.get('source', '')
    limit = data.get('limit', 10)
    offset = data.get('offset', 0)
    logging.debug(f"route: /external-sources/search-limited/<string:query> - Searching external sources for {query}")
    try:
        res = search_external_sources(query, source.split(','), legacy=True, limit=limit, offset=offset)
    except Exception as e:
        logging.error(f"Error in route: /external-sources/search-limited/<string:query> - {str(e)}")
        res = "Sorry, something went wrong in our external source search. Contact the admin for more information."
    code = 200 if not isinstance(res, str) else 500
    return json_return(code, res)


# Download and process
@app.route('/external-sources/download', methods=['POST'])
def external_source_download():
    data = request.get_json()
    external_source = data.get('externalSource')
    logging.debug(f"route: /external-sources/search/<string:query> - Searching external sources for {external_source}")
    try:
        res = download_external_source(external_source)
    except Exception as e:
        logging.error(f"Error in route: /external-sources/search/<string:query> - {str(e)}")
        res = "Sorry, we were unable to download your selected file. Contact the admin for more information."
    code = 200 if res == "Success" else 500
    return json_return(code, res)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=105, debug=True)
