import base64
import datetime
import logging
import re

import numpy as np
import pandas as pd

from services.read_data import read_data, read_data_from_api
from services.ssr import create_ssr_parsed_file

logger = logging.getLogger(__name__)

DATE_FORMATS = [
    "%d %b %Y",  # d MMM yyyy
    "%d %b %Y %H:%M:%S",  # d MMM yyyy HH:mm:ss
    "%d %b %Y %H:%M:%S %z",  # d MMM yyyy HH:mm:ss Z
    "%d %B %Y",  # d MMMM yyyy
    "%d %B %Y %H:%M:%S %Z",  # d MMMM yyyy HH:mm:ss z
    "%d-%m-%y",  # d-M-yy
    "%d-%m-%y %H:%M",  # d-M-yy H:mm
    "%d.%m.%y %H:%M",  # d.M.yy H:mm
    "%d/%m/%Y",  # d/M/yyyy
    "%d/%m/%Y %I:%M %p",  # d/M/yyyy h:mm a
    "%d/%m/%Y %I:%M:%S %p",  # d/M/yyyy h:mm:ss a
    "%d/%m/%Y %H:%M",  # d/M/yyyy HH:mm
    "%d/%m/%Y %H:%M:%S",  # d/M/yyyy HH:mm:ss
    "%d/%m/%y",  # d/MM/yy
    "%d/%m/%y %H:%M",  # d/MM/yy H:mm
    "%d/%b/%Y %H:%M:%S %z",  # d/MMM/yyyy H:mm:ss Z
    "%d %B %Y",  # dd MMMM yyyy
    "%d %B %Y %H:%M:%S %Z",  # dd MMMM yyyy HH:mm:ss z
    "%d-%b-%y %I.%M.%S.%f %p",  # dd-MMM-yy hh.mm.ss.nnnnnnnnn a
    "%d-%b-%Y",  # dd-MMM-yyyy
    "%d-%b-%Y %H:%M:%S",  # dd-MMM-yyyy HH:mm:ss
    "%d/%m/%y",  # dd/MM/yy
    "%d/%m/%y %H:%M",  # dd/MM/yy HH:mm
    "%d/%m/%y %H:%M:%S",  # dd/MM/yy HH:mm:ss
    "%d/%m/%Y",  # dd/MM/yyyy
    "%d/%m/%Y %I:%M %p",  # dd/MM/yyyy h:mm a
    "%d/%m/%Y %I:%M:%S %p",  # dd/MM/yyyy h:mm:ss a
    "%d/%m/%Y %H:%M",  # dd/MM/yyyy HH:mm
    "%d/%m/%Y %H:%M:%S",  # dd/MM/yyyy HH:mm:ss
    "%d/%b/%y %I:%M %p",  # dd/MMM/yy h:mm a
    "%a %b %d %H:%M:%S %Z %Y",  # EEE MMM dd HH:mm:ss z yyyy
    "%a, %d %b %Y %H:%M:%S %z",  # EEE, d MMM yyyy HH:mm:ss Z
    "%A %d %B %Y",  # EEEE d MMMM yyyy
    "%A %d %B %Y %H h %M %Z",  # EEEE d MMMM yyyy H' h 'mm z
    "%A %d %B %Y %H h %M %Z",  # EEEE d MMMM yyyy HH' h 'mm z
    "%A, %d %B %Y",  # EEEE, d MMMM yyyy
    "%A, %d %B %Y %H:%M:%S o'clock %Z",  # EEEE, d MMMM yyyy HH:mm:ss 'o''clock' z
    "%A, %B %d, %Y",  # EEEE, MMMM d, yyyy
    "%A, %B %d, %Y %I:%M:%S %p %Z",  # EEEE, MMMM d, yyyy h:mm:ss a z
    "%m-%d-%y",  # M-d-yy
    "%m-%d-%y %I:%M %p",  # M-d-yy h:mm a
    "%m-%d-%y %I:%M:%S %p",  # M-d-yy h:mm:ss a
    "%m-%d-%y %H:%M",  # M-d-yy HH:mm
    "%m-%d-%y %H:%M:%S",  # M-d-yy HH:mm:ss
    "%m-%d-%Y",  # M-d-yyyy
    "%m-%d-%Y %I:%M %p",  # M-d-yyyy h:mm a
    "%m-%d-%Y %I:%M:%S %p",  # M-d-yyyy h:mm:ss a
    "%m-%d-%Y %H:%M",  # M-d-yyyy HH:mm
    "%m-%d-%Y %H:%M:%S",  # M-d-yyyy HH:mm:ss
    "%m/%d/%y",  # M/d/yy
    "%m/%d/%y %I:%M %p",  # M/d/yy h:mm a
    "%m/%d/%y %H:%M:%S",  # M/d/yy H:mm:ss (HH:mm:ss, MM)
    "%m/%d/%y %H:%M",  # M/d/yy HH:mm
    "%m/%d/%Y",  # M/d/yyyy
    "%m/%d/%Y %I:%M %p",  # M/d/yyyy h:mm a
    "%m/%d/%Y %I:%M:%S %p",  # M/d/yyyy h:mm:ss a
    "%m/%d/%Y %H:%M",  # M/d/yyyy HH:mm
    "%m/%d/%Y %H:%M:%S",  # M/d/yyyy HH:mm:ss
    "%m-%d-%y",  # MM-dd-yy
    "%m-%d-%y %I:%M %p",  # MM-dd-yy h:mm a
    "%m-%d-%y %I:%M:%S %p",  # MM-dd-yy h:mm:ss a
    "%m-%d-%y %H:%M",  # MM-dd-yy HH:mm
    "%m-%d-%y %H:%M:%S",  # MM-dd-yy HH:mm:ss
    "%m-%d-%Y",  # MM-dd-yyyy
    "%m-%d-%Y %I:%M %p",  # MM-dd-yyyy h:mm a
    "%m-%d-%Y %I:%M:%S %p",  # MM-dd-yyyy h:mm:ss a
    "%m-%d-%Y %H:%M",  # MM-dd-yyyy HH:mm
    "%m-%d-%Y %H:%M:%S",  # MM-dd-yyyy HH:mm:ss
    "%m/%d/%y",  # MM/dd/yy
    "%m/%d/%y %I:%M %p",  # MM/dd/yy h:mm a
    "%m/%d/%y %I:%M:%S %p",  # MM/dd/yy h:mm:ss a
    "%m/%d/%y %H:%M",  # MM/dd/yy HH:mm
    "%m/%d/%Y",  # MM/dd/yyyy
    "%m/%d/%Y %I:%M %p",  # MM/dd/yyyy h:mm a
    "%m/%d/%Y %I:%M:%S %p",  # MM/dd/yyyy h:mm:ss a
    "%m/%d/%Y %H:%M",  # MM/dd/yyyy HH:mm
    "%m/%d/%Y %H:%M:%S",  # MM/dd/yyyy HH:mm:ss
    "%b %d %Y",  # MMM d yyyy
    "%b %d, %Y",  # MMM d, yyyy
    "%b %d, %Y %I:%M:%S %p",  # MMM d, yyyy h:mm:ss a
    "%b.%d.%Y",  # MMM.dd.yyyy
    "%B %d %Y",  # MMMM d yyyy
    "%B %d, %Y",  # MMMM d, yyyy
    "%B %d, %Y %I:%M:%S %p %Z",  # MMMM d, yyyy h:mm:ss z a
    "%y-%m-%d",  # yy-MM-dd
    "%Y-'W'%W-%w",  # YYYY-'W'w-c
    "%Y-%j%z",  # yyyy-DDDXXX
    "%Y-%m-%d %I:%M %p",  # yyyy-M-d h:mm a
    "%Y-%m-%d %I:%M:%S %p",  # yyyy-M-d h:mm:ss a
    "%Y-%m-%d %H:%M",  # yyyy-M-d HH:mm
    "%Y-%m-%d %H:%M:%S",  # yyyy-M-d HH:mm:ss
    "%Y-%m-%d",  # yyyy-MM-dd
    "%Y-%m-%d %G",  # yyyy-MM-dd G
    "%Y-%m-%d %I:%M %p",  # yyyy-MM-dd h:mm a
    "%Y-%m-%d %I:%M:%S %p",  # yyyy-MM-dd h:mm:ss a
    "%Y-%m-%d %H:%M:%S",  # yyyy-MM-dd HH:mm:ss
    "%Y-%m-%d %H:%M:%S,%f",  # yyyy-MM-dd HH:mm:ss,SSS
    "%Y-%m-%d %H:%M:%S,%f[%Z]",  # yyyy-MM-dd HH:mm:ss,SSS'['VV']'
    "%Y-%m-%d %H:%M:%S,%fZ",  # yyyy-MM-dd HH:mm:ss,SSS'Z'
    "%Y-%m-%d %H:%M:%S,%f%z",  # yyyy-MM-dd HH:mm:ss,SSSXXX
    "%Y-%m-%d %H:%M:%S,%f%z[%Z]",  # yyyy-MM-dd HH:mm:ss,SSSXXX'['VV']'
    "%Y-%m-%d %H:%M:%S.%f",  # yyyy-MM-dd HH:mm:ss.S
    "%Y-%m-%d %H:%M:%S.%f",  # yyyy-MM-dd HH:mm:ss.SSS
    "%Y-%m-%d %H:%M:%S.%f[%Z]",  # yyyy-MM-dd HH:mm:ss.SSS'['VV']'
    "%Y-%m-%d %H:%M:%S.%fZ",  # yyyy-MM-dd HH:mm:ss.SSS'Z'
    "%Y-%m-%d %H:%M:%S.%f%z",  # yyyy-MM-dd HH:mm:ss.SSSXXX
    "%Y-%m-%d %H:%M:%S.%f%z[%Z]",  # yyyy-MM-dd HH:mm:ss.SSSXXX'['VV']'
    "%Y-%m-%d %H:%M:%S%z[%Z]",  # yyyy-MM-dd HH:mm:ssXXX'['VV']'
    "%Y-%m-%d %H:%M:%S%z",  # yyyy-MM-dd HH:mm:ssZ (ss'Z', ssX, ssXXX)
    "%Y-%m-%dT%H:%M:%S",  # yyyy-MM-dd'T'HH:mm:ss
    "%Y-%m-%dT%H:%M:%S,%f",  # yyyy-MM-dd'T'HH:mm:ss,SSS
    "%Y-%m-%dT%H:%M:%S,%f[%Z]",  # yyyy-MM-dd'T'HH:mm:ss,SSS'['VV']'
    "%Y-%m-%dT%H:%M:%S,%fZ",  # yyyy-MM-dd'T'HH:mm:ss,SSS'Z'
    "%Y-%m-%dT%H:%M:%S,%f%z",  # yyyy-MM-dd'T'HH:mm:ss,SSSXXX
    "%Y-%m-%dT%H:%M:%S,%f%z[%Z]",  # yyyy-MM-dd'T'HH:mm:ss,SSSXXX'['VV']'
    "%Y-%m-%dT%H:%M:%S.%f",  # yyyy-MM-dd'T'HH:mm:ss.SSS
    "%Y-%m-%dT%H:%M:%S.%f[%Z]",  # yyyy-MM-dd'T'HH:mm:ss.SSS'['VV']'
    "%Y-%m-%dT%H:%M:%S.%fZ",  # yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
    "%Y-%m-%dT%H:%M:%S.%f%z",  # yyyy-MM-dd'T'HH:mm:ss.SSSXXX
    "%Y-%m-%dT%H:%M:%S.%f%z[%Z]",  # yyyy-MM-dd'T'HH:mm:ss.SSSXXX'['VV']'
    "%Y-%m-%dT%H:%M:%S%z[%Z]",  # yyyy-MM-dd'T'HH:mm:ssXXX'['VV']'
    "%Y-%m-%dT%H:%M:%S%z",  # yyyy-MM-dd'T'HH:mm:ssZ (ss'Z', ssX, ssXXX)
    "%Y-%m-%d%z",  # yyyy-MM-ddXXX
    "%Y'W'%W%w",  # YYYY'W'wc
    "%Y/%m/%d",  # yyyy/M/d
    # "%Y%m%d",  # yyyyMMdd This is disabled, because it leads to false positives on numbers
    "%Y%m%d%z",  # yyyyMMddZ
    "%Y-W%V",  # Week: 2024-W34
    "%G-W%V-%u",  # Week with weekday: 2024-W34-5
    "%Y-%j"  # Ordinal date: 2024-236
]


def check_and_convert_dates(df):
    """
    Replacement function for dates. We want to be able to catch all dates.
    Dates were sourced from
      https://help.talend.com/en-us/data-preparation-user-guide/8.0/list-of-date-and-date-time-formats
    And
      https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior

    :param df: dataframe to check
    :return: dataframe with the dates converted to a standard format
    """
    for header in df.columns.tolist():
        # get the first non na value in the column, alphabetically sorted in reverse (for american dates).
        try:
            first_value = df[header].dropna().sort_values(ascending=False).iloc[0]
        except IndexError:
            continue
        # get the presumed date format
        presumed_dateformat = None
        has_time = False
        for date_format in DATE_FORMATS:
            # check if my_date is in the date_format
            try:
                # value is not used, just checking for a ValueError
                datetime.datetime.strptime(str(first_value), date_format)
                presumed_dateformat = date_format
                has_time = "H" in date_format or "I" in date_format
                break
            except ValueError:
                continue
        # Convert the column to a date if the presumed date format is found.
        # Convert to YYYY-MM-DD if there is no time, otherwise convert to ISO8601
        if presumed_dateformat is None:
            continue
        if has_time:
            try:
                df[header] = df[header].apply(
                    lambda x, fmt=presumed_dateformat: datetime.datetime.strptime(str(x), fmt).isoformat())
                # if all the time values end up as 00:00:00, remove the time
                if df[header].apply(lambda x: x.endswith("00:00:00")).all():
                    df[header] = df[header].apply(lambda x: x[:10])
            except ValueError:
                continue
        else:
            df[header] = df[header].apply(
                lambda x, fmt=presumed_dateformat: datetime.datetime.strptime(str(x), fmt).strftime("%Y-%m-%d"))
    return df


# DEPRECATED
def is_datetime_column(column):
    """
    Check whether or not a column is majority datetime objects.
    Considering 75% to be majority

    :param column: column to check
    :return: True if the column is majority datetime objects, False otherwise
    """
    try:
        non_numeric_values = column.apply(lambda x: not isinstance(x, (int, float)) or isinstance(x, bool))
        datetime_values = pd.to_datetime(column[non_numeric_values], errors='coerce', format=DATE_FORMATS[0])
        valid_datetime_count = datetime_values.count()
        total_values = len(column)
        return valid_datetime_count / total_values > 0.75
    except Exception as e:
        logger.error(f"Error in is_datetime_column: {str(e)}")
        return False


# DEPRECATED
def apply_date(string):
    """
    Convert a string to a datetime object, return a pandas df na if not possible

    :param string: string to convert
    :return: datetime object if possible, pandas df na otherwise
    """
    for date_format in DATE_FORMATS:
        try:
            date_input = datetime.datetime.strptime(str(string), date_format)
            return date_input
        except ValueError:
            continue

    # return a df na
    return np.nan


def has_string(string):
    """
    Check if a string can be converted to a float in any way, if not, it is a string

    :param string: string to check
    :return: True if the string cannot be converted to a float, False otherwise
    """
    try:
        float(string)
        return False
    except Exception:
        try:
            float(string.replace(",", ""))
            return False
        except Exception:
            return True


def detect_first_row_is_number(df, header):
    """
    Check if the first row in a column is a number

    :param df: dataframe to check
    :param header: header to check
    :return: True if the first row is a number, False otherwise
    """
    try:
        first_string_index = df.loc[df[header].apply(lambda x: has_string(x))].index[0]
        return first_string_index == 0
    except Exception as e:
        logger.error(f"Error in detect_first_row_is_number: {str(e)}")
        return False


def swap_first_row(df, headers):
    """
    Swap the first row with the first row that has a string in all of the given headers.

    :param df: dataframe to swap
    :param headers: headers to check
    :return: dataframe with the first row swapped with the first row that has a string in all of the given headers
    """
    try:
        df, header_indices = get_header_indices(df, headers)
        # Find the first row that has strings in each of the given headers
        common_set = set(header_indices[next(iter(header_indices))])
        # Find the intersection with the remaining sets
        for lst in header_indices.values():
            common_set = common_set.intersection(lst)
        first_common_number = next(iter(common_set), None)
        # If there is no common row with strings, return the df as is
        if first_common_number is None:
            return df
        # swap the first row with the first common row index
        df.iloc[0], df.iloc[first_common_number] = df.iloc[first_common_number], df.iloc[0]
        return df
    except Exception as e:
        logger.error(f"Error in swap_first_row: {str(e)}")
        return df


def get_header_indices(df, headers):
    """
    identifying rows in the DataFrame where string values are present for each header,
    and then performing specific operations to clean and handle those string values.

    :param df: dataframe to check
    :param headers: headers to check
    :return: dataframe with the first row swapped with the first row that has a string in all of the given headers
    """
    header_indices = {}
    try:
        for header in headers:
            stringls = df.loc[df[header].apply(lambda x: has_string(x))].index.tolist()
            # if the length of stringls is less than 5% of the number of rows in the dataframe, skip
            if len(stringls) < (df.count()[header]) * 0.05:
                for i in stringls:
                    # replace anything other than 0-9 or . with '' in the column at row i
                    try:
                        df.loc[i, header] = re.sub(r'[^0-9.]', '', df.loc[i, header])
                    except TypeError:
                        df.loc[i, header] = np.nan
                    # check if df.loc[i, header] is empty
                    if df.loc[i, header] == '':
                        df.loc[i, header] = np.nan
                continue
            header_indices[header] = stringls
    except Exception as e:
        logger.error(f"Error in get_header_indices: {str(e)}")
    return df, header_indices


def has_comma_readable_number(x):
    """
    Check if a string can be converted to a float, also if there is a comma separating the thousands

    :param x: string to check
    :return: True if the string can be converted to a float, False otherwise
    """
    try:
        float(x)
        return False
    except Exception:
        try:
            float(x.replace(",", ""))
            return True
        except Exception:
            return False


def replace_comma_readable_number(x):
    """
    Replace the comma in a string with nothing

    :param x: string to replace
    :return: string with comma replaced with nothing
    """
    try:
        if ',' in x:
            if '.' in x:
                return x.replace(",", "")
            else:
                return x.replace(",", ".")
        return x
    except Exception:
        return x


def numerify(x):
    """
    Replace anything other than 0-9 or . with '' in the column

    :param x: string to replace
    :return: string with anything other than 0-9 or . replaced with ''
    """
    try:
        if pd.isna(x):
            return 0
        x = re.sub(r'[^0-9.]', '', x)
        x = 0 if x == '' else x
        return x
    except Exception:
        return 0


def fillna_on_dtype(df):
    """
    Fill NaN values in a dataset based on the data type of the column.

    :param df: dataframe to fill
    :return: dataframe with NaN values filled
    """
    fill_values = {
        'object': '',
        'int64': 0,
        'float64': 0.0,
        'datetime64[ns]': datetime.datetime(1970, 1, 1, 0, 0, 0),
    }

    # Iterate over each column and fill the NaN values based on the data type
    for column in df.columns:
        dtype = df[column].dtype
        fill_value = fill_values.get(str(dtype), np.nan)
        df[column].fillna(fill_value, inplace=True)
    return df


def is_number(s: str):
    try:
        float(s)
        return True
    except ValueError:
        return False


def is_percentage_column(column: pd.Series):
    """
    This check is done to prevent percentage columns from being parsed as strings.
    Check whether or not a column is majority percentage strings.
    Considering 75% to be majority

    :param column: column to check
    :return: True if the column is majority percentage strings, False otherwise
    """
    try:
        percentage_strings = column.apply(lambda x: isinstance(x, str) and x.endswith('%') and is_number(x[:-1]))
        valid_items = column[percentage_strings]
        valid_items_count = valid_items.count()
        total_values = len(column)
        return valid_items_count / total_values > 0.75
    except Exception as e:
        logger.error(f"Error in preprocessing data - <is_percentage_column()>: {str(e)}")
        return False


def convert_percentage_value(x: str):
    """
    Convert a percentage string to a float value

    :param x: percentage string to convert
    :return: float value of the percentage string or NAN
    """
    try:
        return float(x[:-1])
    except ValueError:
        return pd.NA


def preprocess_data_df(df):
    """
    Subfunction to preprocess the data in a dataframe.
    Here we check if a column is a date, a string, or a number.
    We also confirm the type of the content of each column.

    :param df: dataframe to preprocess
    :return: dataframe with the data preprocessed
    """
    columns_with_strings_starting_with_numbers = []
    df = check_and_convert_dates(df)
    for header in df.columns.tolist():
        # if there are 75% or more dates in the column, convert the column to a date
        """
        Deprecated date conversion 26/08/2024, remove after testing cycle
        has_datetime_values = is_datetime_column(df[header])
        if has_datetime_values:
            # Apply the date functions to a new column, replace the old column with the
            # converted, to update the dtype of the column
            df[header + "_converted"] = df[header].apply(lambda x: apply_date(x))
            df.drop(columns=[header], inplace=True)
            df.rename(columns={header + "_converted": header}, inplace=True)
            continue
        """
        has_percentage_values = is_percentage_column(df[header])
        if has_percentage_values:
            df[header + ",%"] = df[header].apply(lambda x: convert_percentage_value(x))
            df.drop(columns=[header], inplace=True)
            continue

        has_string_values = df[header].apply(lambda x: has_string(x)).any()
        if has_string_values:
            if df[header].apply(lambda x: has_string(x)).sum() < df.count()[header] * 0.05:
                # replace anything other than 0-9 or . with np.nan in the column
                df.loc[:, header] = df[header].apply(lambda x: numerify(x))
                # check if df.loc[:, header] is empty
            elif not detect_first_row_is_number(df, header):
                columns_with_strings_starting_with_numbers.append(header)
                continue
            else:
                continue

        has_comma_readable_numbers = df[header].apply(lambda x: has_comma_readable_number(x)).any()
        if has_comma_readable_numbers:
            df.loc[:, header] = df[header].apply(lambda x: replace_comma_readable_number(x))
        # if there are 50% or more numbers in the column, convert the column to a number
        if df[header].apply(lambda x: pd.to_numeric(x, errors='ignore')).count() >= (df.count()[header]) * 0.5:
            df[header] = pd.to_numeric(df[header], errors='coerce')

    return df, columns_with_strings_starting_with_numbers


def _find_almost_empty_row_index(df, threshold=0.9):
    # Calculate the percentage of NaN values in each row
    nan_percentage = df.isnull().mean(axis=1)
    # Find rows where the percentage of NaN values exceeds the threshold
    almost_empty_rows = df[nan_percentage >= threshold]
    # If there are no almost empty rows, return NaN
    if almost_empty_rows.empty:
        return pd.NA
    # Return the index of the first almost empty row
    return almost_empty_rows.index.min()


def _remove_rows_after_empty(df):
    # Find the index of the first completely empty row
    # empty_row_index = df[df.isnull().all(axis=1)].index.min()
    empty_row_index = _find_almost_empty_row_index(df)
    if not pd.isnull(empty_row_index):
        # Drop rows after the empty row
        df = df.iloc[:empty_row_index]
    return df


def strip_metadata(df):
    try:
        df = _remove_rows_after_empty(df)  # Drop bottom comments or metadata.
        # Drop rows and columns with all NaN values
        df = df.dropna(axis=0, how='all').dropna(axis=1, how='all')
        # Find the indices of the first and last non-NaN values in each row
        start_indices = df.apply(lambda x: x.first_valid_index(), axis=1)
        end_indices = df.apply(lambda x: x.last_valid_index(), axis=1)
        # Determine the minimum and maximum non-NaN indices across all rows
        min_start_index = start_indices.min()
        max_end_index = end_indices.max()
        # Convert column names to numeric indices
        min_start_index = df.columns.get_loc(min_start_index)
        max_end_index = df.columns.get_loc(max_end_index)
        # Slice the dataframe to keep only the table data
        df = df.iloc[:, min_start_index:max_end_index + 1]
    except Exception as e:
        logger.error(f"Error in strip_metadata: {str(e)}")
    return df


def preprocess_data(name, create_ssr=False, table=None, db=None, api=None):
    """
    Process trigger to preprocess a dataset.
    The headers are updated to include the dataset name and only have a-zA-Z0-9 values.
    We make sure each column is the correct dtype and has no NaN values.
    Lastly we make sure the first row contains the correct dtype in each column, to enforce solr indexing.
    This process saves the preprocessed dataset by overwriting the provided file.

    :param name: name of the dataset to preprocess
    :param create_ssr: boolean to indicate if we should create an SSR entry for the dataset
    :return: success message if the dataset was preprocessed successfully, error message otherwise
    """
    logger.debug(f"Preprocessing data for {name}")
    file_path = f"./staging/{name}" if not db else name
    res = "Success"
    try:
        extension_length = len(name.split('.')[-1]) + 1  # including the .
    except Exception:
        extension_length = 0
    logger.debug(f"---- Extension length: {extension_length}")
    try:
        logger.debug("-- Preprocessing content")
        df, message = _read_data(file_path, table, db, api)
        logger.debug(f"---- Reading data result: {message}")
        if "Success" not in message:
            return message
        # try to strip metadata from input files.
        df = strip_metadata(df)
        # drop any row that is 90-100% empty
        df.dropna(thresh=df.shape[1] * 0.1, inplace=True)
        # drop any column that is 95-100% empty
        df.dropna(axis=1, thresh=df.shape[0] * 0.05, inplace=True)

        # make all column names in df a string.
        # Clean the headers to only a-z, A-Z, 0-9
        df.columns = df.columns.astype(str)
        df.columns = df.columns.str.replace(r'[^a-zA-Z0-9%]', '', regex=True, flags=re.IGNORECASE)
        # prefix the headers with the column name
        df_prefix = name[:-extension_length] + '__'
        df.columns = [df_prefix + str(col) for col in df.columns]
        # for each column check: is it a date, is it a string, is it a number,
        # make sure we start our string columns with a string
        df, columns_with_strings_starting_with_numbers = preprocess_data_df(df)
        # ensure the first row starts with strings for all columns that contain
        # strings but start with numbers
        if columns_with_strings_starting_with_numbers != []:
            df = swap_first_row(df, columns_with_strings_starting_with_numbers)

        # prep by cleaning the dataframe from any NA values
        df = fillna_on_dtype(df)

        logger.debug(f"Dataframe:\n{df.head()}")
        logger.debug(f"Done preprocessing data for {name}")
        if create_ssr:
            create_ssr_parsed_file(df, df_prefix, name[:-extension_length])
    except Exception as e:
        logger.error(f"Error in preprocess_data: {str(e)}")
        res = "Sorry, something went wrong in our dataset processing. Contact the admin for more information."
    return res


def _read_data(file_path, table, db, api):
    if table:
        res = read_data(file_path, {'table': table})
    elif db:
        res = read_data(file_path, db)
    elif api:
        # replace _ with / in api_url
        url = base64.b64decode(api['api_url'].replace("_", "/"))
        additional_args = {}
        if api.get('json_root', None) != "none":
            additional_args['json_root'] = api['json_root']
        if api.get('xml_root', None) != "none":
            additional_args['xml_root'] = api['xml_root']

        res = read_data_from_api(url, additional_args)
    else:
        res = read_data(file_path)

    if isinstance(res, tuple):
        df, message = res
    else:
        df = res
        message = "Sorry, we were unable to parse your data into a dataframe. Contact the admin for more information."

    if type(df) is not pd.DataFrame:
        logger.error(f"Error in preprocess_data: {message}")
    return df, message
