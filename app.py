# Imports
import logging

from dotenv import load_dotenv
from flask import Flask, request

from services.external_sources.external_sources import (
    download_external_source, search_external_sources)
from services.preprocess_dataset import preprocess_data
from services.ssr import load_sample_data, remove_ssr_parsed_files, duplicate_ssr_parsed_files
from services.util import remove_files
from util.configure_logging import confirm_logger

# Load the environment variables
load_dotenv()
# Set up the flask app
app = Flask(__name__)

# Setup and confirm the logger
confirm_logger()


"""
DX Processing
"""


@app.route('/upload-file/<string:ds_name>', methods=['GET', 'POST'])
def process_dataset(ds_name):
    logging.debug(f"route: /upload-file/<string:ds_name> - Processing dataset {ds_name}")
    try:
        # Preprocess
        preprocess_data(ds_name, create_ssr=True)
        # Create a solr core and post the dataset
        # res = post_data_to_solr(ds_name)  # TODO: Disabled solr until data processing required
        # Remove the processed file
        remove_files([ds_name])
        res = "Success"
    except Exception as e:
        logging.error(f"Error in route: /upload-file/<string:ds_name> - {str(e)}")
        res = "Sorry, something went wrong in our dataset processing. Contact the admin for more information."
    return res

@app.route('/duplicate-dataset/<string:ds_name>/<string:new_ds_name>', methods=['GET', 'POST'])
def duplicate_dataset(ds_name, new_ds_name):
    logging.debug(f"route: /duplicate-dataset/<string:ds_name> - Duplicating dataset {ds_name} to {new_ds_name}")
    try:
        # Preprocess
        duplicate_ssr_parsed_files(ds_name, new_ds_name)
        res = "Success"
    except Exception as e:
        logging.error(f"Error in route: /duplicate-dataset/<string:ds_name> - {str(e)}")
        res = "Sorry, something went wrong in our dataset duplication. Contact the admin for more information."
    return res


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
    return res


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
        preprocess_data(ds_name, create_ssr=True, db=db)
        res = "Success"
    except Exception as e:
        logging.error(f"Error in route: /upload-file/<string:ds_name>/<string:table> - {str(e)}")
        res = "Sorry, something went wrong in our sql dataset processing. Contact the admin for more information."
    return res


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
        remove_ssr_parsed_files(ds_name)
        res = "Success"
    except Exception as e:
        logging.error(f"Error in route: /delete-dataset/<string:ds_name> - {str(e)}")
        res = "Sorry, something went wrong in our dataset deletion. Contact the admin for more information."
    return res


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
    return res


"""
External data sources
"""


# Search
@app.route('/external-sources/search', methods=['POST'])
def external_source_search():
    data = request.get_json()
    owner = data.get('owner')
    query = data.get('query')
    logging.debug(f"route: /external-sources/search/<string:query> - Searching external sources for {query}")
    try:
        res = search_external_sources(query, owner)
    except Exception as e:
        logging.error(f"Error in route: /external-sources/search/<string:query> - {str(e)}")
        res = "Sorry, something went wrong in our external source search. Contact the admin for more information."
    return res


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
        res = "Sorry, something went wrong in our external source download. Contact the admin for more information."
    return res


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=105, debug=True)
