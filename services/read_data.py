"""
This file reads in a data file, not necessarily determined already.
It will be used to read in data from the staging directory, and then parse it into a dataframe.

First, we determine the file's encoding, which we use for reading the content.
Second, we determine the file's extension, which we use to determine how to parse the file.
Third, we parse the file into a dataframe, and return it.

For the several types of data files, we use a switch statement to select the correct parsing method.
"""
import json
import logging
import mimetypes
import os
import sqlite3
import xml.etree.ElementTree as ET

import chardet
import pandas as pd
import requests

import magic
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def read_data(file_path, optional_args={}):
    logger.info(f"Reading data from file: {file_path}")
    try:
        if file_path in ["MySQL", "PostgreSQL", "MsSQL", "Oracle"]:
            file_extension = file_path
            file_encoding = file_path
        else:
            file_encoding = detect_file_encoding(file_path)
            file_extension = detect_file_extension(file_path)
            if file_extension == "error":
                return "error", "error"

        res = read_path_as_df(file_path, file_encoding, file_extension, optional_args)
        if isinstance(res, tuple):
            df, message = res
        else:
            df = res
            message = "Successfully read dataframe."
        logger.debug(f"Dataset read finished with message: {message}")
        logger.debug(f"Dataframe:\n{df.head()}")
        return df, message
    except Exception as e:
        logger.error(f"Error reading data: {e}")
        return "error", "error"


def detect_file_encoding(file_path):
    """
    Detect the encoding of a file
    """
    logger.debug(f"Detecting encoding of file: {file_path}")
    try:
        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read())
        logger.debug(f"Detected encoding: {result['encoding']}")
        return result['encoding']
    except Exception as e:
        logger.error(f"Error detecting file encoding: {e}")
        # Default to the most common file encoding
        return 'utf-8'


def detect_file_extension(file_path):
    """
    Detect the file type of a file. First, on any extension in the filename (used primarily for .h5 or .spss etc)
    if that does not work, use the mimetypes library,
    and if that does not work, use the magic library.
    """
    try:
        if bool(os.path.splitext(file_path)[1]):
            t = os.path.splitext(file_path)[1]
        else:
            t = mimetypes.guess_type(file_path)[0]
            if t is None:
                t = magic.from_file(file_path, mime=True)
            t = _mimetype_to_extension(t)
        logger.debug(f"Detected file extension: {t}")
        return t
    except Exception as e:
        logger.error(f"Error detecting file extension: {e}")
        return "error"


def _mimetype_to_extension(mimetype):
    mimetype_extension_dict = {
        "text/csv": ".csv",
        "application/excel": ".xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/vnd.ms-excel.sheet.macroEnabled.12": ".xlsm",
        "application/vnd.ms-excel.sheet.binary.macroEnabled.12": ".xlsb",
        "application/vnd.oasis.opendocument.formula": ".odf",
        "application/vnd.oasis.opendocument.spreadsheet": ".ods",
        "application/vnd.oasis.opendocument.text": ".odt",
        "application/json": ".json",
        "application/xml": ".xml",
        "application/x-stata-data": ".dta",
        "application/x-hdf": ".h5"
    }
    try:
        return mimetype_extension_dict[mimetype]
    except KeyError:
        return "error"


def read_path_as_df(file_path, file_encoding, file_extension, optional_args={}):
    read_functions = {
        ".csv": lambda: pd.read_csv(file_path, encoding=file_encoding, low_memory=False, **optional_args),
        ".tsv": lambda: pd.read_csv(file_path, encoding=file_encoding, sep='\t', low_memory=False, **optional_args),
        ".json": lambda: _read_json(file_path, **optional_args),
        ".xml": lambda: _read_xml(file_path, encoding=file_encoding, **optional_args),
        ".xls": lambda: pd.read_excel(file_path, engine="xlrd", **optional_args),
        ".xlsx": lambda: pd.read_excel(file_path, engine="openpyxl", **optional_args),
        ".xlsm": lambda: pd.read_excel(file_path, engine="openpyxl", **optional_args),
        ".xlsb": lambda: pd.read_excel(file_path, engine="pyxlsb", **optional_args),
        ".ods": lambda: pd.read_excel(file_path, engine="odf", **optional_args),  # excluding .odt and .odf for now
        ".dta": lambda: pd.read_stata(file_path, **optional_args),
        ".h5": lambda: pd.read_hdf(file_path, **optional_args),
        ".orc": lambda: pd.read_orc(file_path, **optional_args),  # pyarrow
        ".feather": lambda: pd.read_feather(file_path, **optional_args),  # pyarrow
        ".parquet": lambda: pd.read_parquet(file_path, **optional_args),  # pyarrow
        ".sav": lambda: pd.read_spss(file_path, **optional_args),  # pip install pyreadstat
        ".sas7bdat": lambda: pd.read_sas(file_path, **optional_args),
        ".sas": lambda: pd.read_sas(file_path, **optional_args),
        ".sqlite": lambda: _read_sqlite(file_path, **optional_args),
        "MySQL": lambda: _read_sql(file_path, **optional_args),  # pip install pymysql sqlalchemy
        "PostgreSQL": lambda: _read_sql(file_path, **optional_args),  # pip install psycopg sqlalchemy
        "MsSQL": lambda: _read_sql(file_path, **optional_args),  # pip install pymssql sqlalchemy
        "Oracle": lambda: _read_sql(file_path, **optional_args),  # pip install oracledb sqlalchemy
        # Introduce more from https://docs.sqlalchemy.org/en/13/dialects/index.html
    }

    logger.debug(f"Reading file {file_path} with extension {file_extension} and encoding {file_encoding}")
    if file_extension in read_functions:
        try:
            return read_functions[file_extension]()
        except Exception as e:
            logger.error(f"Error reading file: {e}")
            return "error"
    else:
        raise ValueError("Unsupported file extension")


def _read_json(file_path, **optional_args):
    """
    Read a json file into a dataframe, but normalize it first.
    Achieved by json loading the json file, then applying pandas.json_normalize to it.

    Unfortunately, nested arrays are not supported, here is why:
    Flattening nested arrays in a JSON file into a 2D table, such as a DataFrame, can also pose similar challenges and risks as flattening nested XML structures. Here's an explanation of why flattening nested arrays in JSON might be considered dangerous:
     - Loss of Hierarchy: JSON is designed to represent structured and nested data using arrays and objects. Flattening nested arrays into a table can lead to the loss of the original hierarchical structure. Elements that were intended to be nested might lose their meaningful relationships and context.
     - Data Duplication: Flattening can cause data duplication as values from higher levels of the hierarchy are repeated across multiple rows. This duplication can inflate the dataset's size and potentially lead to inconsistencies.
     - Misinterpretation of Relationships: JSON arrays can represent lists of related data. Flattening can break these relationships, making it challenging to understand which elements were originally related.
     - Loss of Metadata: Just like XML, JSON objects can contain metadata and attributes associated with their elements. Flattening might not properly capture this metadata, leading to information loss.
     - Data Integrity Issues: When nested arrays are flattened, it might become difficult to maintain data integrity, especially during updates or modifications. Original relationships could be challenging to reconstruct.
     - Loss of Context: The original context and structure of the data can be lost, making it harder to understand the meaning behind the data points.
     - Complex Nested Structures: JSON arrays can represent complex hierarchical structures. Flattening these structures can result in overly wide tables, making the data difficult to work with and analyze effectively.

    In cases where the nested arrays in a JSON file hold meaningful relationships and context, it's generally recommended to work with the JSON data while preserving its hierarchical structure. If you do need to work with tabular data, carefully consider whether flattening is appropriate for your specific use case and whether any crucial information will be compromised in the process. Using tools that can handle hierarchical JSON data can provide better insights and more accurate representations of the original data structure.

    Your file may still get processed by the data parser, but your arrays will likely be strings with the array's literal contents.
    """
    with open(file_path, 'r') as f:
        data = json.load(f)
    return pd.json_normalize(data, **optional_args)


def _is_shallow_xml(file_path):
    """
    Function to determine the shallowness of an XML file, i.e. whether it contains nested data or not.
    The nested 'limit' is two levels, as this allows us to create a 2d table from the data.
    """
    try:
        with open(file_path, 'r') as xml_file:
            xml_content = xml_file.read()
            root = ET.fromstring(xml_content)
            for element in root:
                for item in element:
                    if len(item) > 0:
                        return False
            return True
    except ET.ParseError as e:
        logger.error(f"Error parsing XML: {e}")
        return False


def _read_xml(file_path, encoding, **optional_args):
    """
    Read a json file into a dataframe, but normalize it first.
    Achieved by json loading the xml file, then applying pandas.json_normalize to it.

    Unfortunately, nested XML data is not supported. Here is why:
    Flattening nested XML structures into a 2D table (like a DataFrame) can lead to data loss and misinterpretation, especially when the XML structure is hierarchical and contains complex relationships between elements. XML is designed to represent hierarchical data, and attempting to flatten it into a tabular structure might not capture the full context and relationships present in the original XML.
    When you flatten nested XML into a 2D table, you might encounter the following challenges:
     - Data Loss: Flattening nested XML can lead to the loss of hierarchical relationships, resulting in a loss of context and meaning. Nested data structures might contain information that is crucial for understanding the data.
     - Duplicated Data: Flattening can lead to duplication of data, as elements from higher levels of the hierarchy are replicated across multiple rows, potentially leading to redundancy and increased storage requirements.
     - Misrepresentation: Complex relationships between elements might not be accurately represented in a tabular format, leading to misinterpretation of the data.
     - Loss of Metadata: XML often includes attributes and metadata associated with elements, which might not be properly represented in a flattened table.
     - Data Integrity: Maintaining data integrity, especially when performing updates or modifications, can become challenging when the original hierarchical structure is lost.
     - Loss of Context: Hierarchical relationships can provide important context to the data. Flattening can make it harder to understand the original structure and relationships.

    In cases where the XML structure is inherently hierarchical and contains nested elements with meaningful relationships, it's generally advisable to work with the XML data as is, using tools that allow you to navigate and query the hierarchical structure. If you do need to work with the data in a tabular format, consider carefully whether flattening is appropriate for your specific use case and whether any information or context will be lost in the process.
    """
    message = "Successfully read XML file."
    if not _is_shallow_xml(file_path):
        message = "Nested XML data is not supported. Please create a shallow XML file of the nested data if you wish to use it."
    return pd.read_xml(file_path, encoding=encoding, **optional_args), message


def _read_sqlite(file_path, **optional_args):
    conn = sqlite3.connect(file_path)
    if 'table' in optional_args:
        df = pd.read_sql_query(f"SELECT * FROM {optional_args['table']}", conn)
    else:
        return None, "No table specified"
    return df


def _read_sql(file_path, **optional_args):
    try:
        username = optional_args["username"]
        password = optional_args["password"]
        host = optional_args["host"]
        port = optional_args["port"]
        database = optional_args["database"]
    except KeyError:
        return None, "Missing SQL credentials"

    try:
        query = optional_args["query"]
    except KeyError:
        try:
            table = optional_args["table"]
            query = f"SELECT * FROM {table}"
        except KeyError:
            return None, "Missing SQL query or table"

    if file_path == "MySQL":
        engine_type = "mysql+pymysql"
    elif file_path == "PostgreSQL":
        engine_type = "postgresql+psycopg"
    elif file_path == "MsSQL":
        engine_type = "mssql+pymssql"
    elif file_path == "Oracle":
        engine_type = "oracle+oracledb"
    else:
        return None, "Invalid database type"
    engine = f"{engine_type}://{username}:{password}@{host}:{port}/{database}"
    df = pd.read_sql(query, con=create_engine(engine))
    return df, "Successfully read SQL table."


"""
Description for the user:

Please provide us with the URL which provides access to the data in your API.
This can be in JSON, XML or CSV format.
We assume you have provided us with CSV if you do not specify.
We do not store the URL anywhere in our system, so any authentication credentials you may pass, will not be stored by us.

If you provide us with an URL that provides the data in CSV, we assume the data is to be used as-is.

If you chose to provide us with JSON or XML, please provide us with a "base field", which is the field that contains your data.
For example if the flat data is in value or result.docs.
If the data is in the root of the JSON or XML, please leave this field with exclusively a period(.) as specifier.

Note with JSON and XML that the following still applies:
Flattening nested XML structures into a 2D table (like a DataFrame) can lead to data loss and misinterpretation, especially when the XML structure is hierarchical and contains complex relationships between elements. XML is designed to represent hierarchical data, and attempting to flatten it into a tabular structure might not capture the full context and relationships present in the original XML.
Flattening nested arrays in a JSON file into a 2D table, such as a DataFrame, can also pose similar challenges and risks as flattening nested XML structures.
"""


def read_data_from_api(url, additional_args={}):
    """
    Download data from an API
    :param URL: The URL to download the data from
    :param additional_args: Additional arguments relating to the API
    :return: A pandas DataFrame with the data
    """
    logger.debug(f"download_api_data:: Starting download of API data with:\n{url}\n{additional_args}")
    try:
        # check if json_root is provided
        json_root = additional_args.get('json_root', None)

        xml_root = additional_args.get('xml_root', None)
        if json_root:
            logger.debug(f"json_root provided: {json_root}")
            df = download_and_prep_json(url, json_root)
        elif xml_root:
            logger.debug(f"xml_root provided: {xml_root}")
            df = download_and_prep_xml(url, xml_root)
        else:
            logger.debug("No json_root or xml_root provided, assuming csv data")
            df = download_and_prep_csv(url)
        return df, "success"
    except Exception as e:
        logger.error(f"Error in download_api_data: {str(e)}")
        return "error", "error"


def download_and_prep_json(url, json_root):
    # Download the data
    data = requests.get(url).json()
    data = _recursive_dict_root(data, json_root)
    df = pd.DataFrame(data)
    logger.debug(f"\n{df.head()}")
    return df


def download_and_prep_xml(url, xml_root):
    # convert xml_root to an XPATH
    if xml_root == ".":
        df = pd.read_xml(url)
    else:
        xpath = "/"+xml_root.replace(".", "/")
        df = pd.read_xml(url, xpath=xpath)
    logger.debug(f"\n{df.head()}")
    return df


def download_and_prep_csv(url):
    df = pd.read_csv(url)
    logger.debug(f"\n{df.head()}")
    return df


def _recursive_dict_root(data, roots):
    if roots == "." or roots == "":
        return data
    else:
        root = roots.split(".")[0]
        return _recursive_dict_root(data[root], ".".join(roots.split(".")[1:]))
