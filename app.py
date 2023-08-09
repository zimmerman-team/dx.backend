# Imports
import logging

from dotenv import load_dotenv
from flask import Flask

from services.datatok import retrieve_dataset_data
from services.kaggle import run_update
from services.preprocess_dataset import preprocess_data
from services.solr import create_solr_core, delete_solr_core, remove_from_core
from services.ssr import (load_sample_data, remove_ssr_parsed_files,
                          remove_ssr_ref)
from services.util import remove_files
from util.configure_logging import confirm_logger

# Load the environment variables
load_dotenv()
# Set up the flask app
app = Flask(__name__)

# Setup and confirm the logger
confirm_logger()


"""
Dataset retrieval
"""


@app.route('/update/', methods=['GET', 'POST'])
def update():
    logging.debug("route: /update/ - Running update")
    try:
        res = run_update()
    except Exception as e:
        logging.error(f"Error in route: /update/ - {str(e)}")
        res = "Sorry, something went wrong in our update. Contact the admin for more information."
    return res


@app.route('/update/<int:num>', methods=['GET', 'POST'])
def update_max(num):
    logging.debug(f"route: /update/<int:num> - Running update for {num}")
    try:
        res = run_update(num)
    except Exception as e:
        logging.error(f"Error in route: /update/<int:num> - {str(e)}")
        res = "Sorry, something went wrong in our update. Contact the admin for more information."
    return res


@app.route('/createDatasetsCore/', methods=['GET', 'POST'])
def create_core():
    logging.debug("route: /createDatasetsCore/ - Creating core")
    _error = "Sorry, something went wrong in our dataset core creation. Contact the admin for more information."
    try:
        created = create_solr_core('datasets')
        res = "Success" if created else _error
    except Exception as e:
        logging.error(f"Error in route: /createDatasetsCore/ - {str(e)}")
        res = _error
    return res


@app.route('/clearDatasetsCore/', methods=['GET', 'POST'])
def clear_core():
    logging.debug("route: /clearDatasetsCore/ - Clearing core")
    _error = "Sorry, something went wrong in our dataset core reset. Contact the admin for more information."
    try:
        deleted = delete_solr_core('datasets')
        res = "Success" if deleted else _error
        res = create_core()
    except Exception as e:
        logging.error(f"Error in route: /clearDatasetsCore/ - {str(e)}")
        res = _error
    return res


"""
DataTok data retrieval
"""


@app.route('/get-data/<int:num>', methods=['GET', 'POST'])
def get_data(num):
    logging.debug(f"route: /get-data/<int:num> - Retrieving data for {num}")
    _error = "Sorry, something went wrong in our data retrieval. Contact the admin for more information."
    try:
        # Retrieve
        data = retrieve_dataset_data(num)
        return data
    except Exception as e:
        res = _error
        logging.error(f"Error in route: /get-data/<int:num> - {str(e)}")
        res = []
    return res


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
        # res = post_data_to_solr(ds_name)
        # Remove the processed file
        remove_files([ds_name])
        # Add the dataset to SSR
        # add_ssr_data_scraper_entry(ds_name[:-4])  # TODO: review obsolete
        res = "Success"
    except Exception as e:
        logging.error(f"Error in route: /upload-file/<string:ds_name> - {str(e)}")
        res = "Sorry, something went wrong in our dataset processing. Contact the admin for more information."
    return res


@app.route('/delete-dataset/<string:ds_name>', methods=['GET', 'POST'])
def delete_dataset(ds_name):
    logging.debug(f"route: /delete-dataset/<string:ds_name> - Deleting dataset {ds_name}")
    try:
        # Remove the dataset from the datasets list in solr
        # this is in case the dataset was created through the update feature
        remove_from_core('ref', ds_name, 'datasets')

        # Delete the solr core belonging to the dataset
        delete_solr_core(ds_name)

        # Remove the dataset from SSR
        remove_ssr_ref(ds_name)
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=105, debug=True)
