"""
This program determines the datatype, datetime format, and fill value of parameters read in from a CSV data file. There are two files created. One is a log file, parameters_overview.txt, and the other is an output file, parameters_summary.json. The output file is an array of JSON objects of the form

{
    "source": <full path of file>,
    "filename": <base filename>,
    "columns": [
        {<column name>: {"type": <data type>, "format": <datetime format>, "fill_value": <fill value>}},
        ...
    ]
}

where type is always given, but format and fill_value only appear if these values exist for a parameter column.

The CSV data files are read in as strings to a pandas dataframe to analyze values to prevent pandas from typecasting. This is to prevent a fill value or NaN value from turning an integer column into a float.

The program uses two reference files. Constant values of BCO-DMO datetime variables names are stored in the file bcodmo_datetime_parameters.txt and possible datetime formats to check a column value by are stored in the file possilbe_datetime_formats.txt.

Each data file parameter names are checked if they are a datetime parameter name stored in bcodmo_datetime_parameters.txt. If they are, the program will try to infer what the datetime format of the parameter values are. If the csv data file has a corresponding parameters information file, it is used to determine the official BCO-DMO parameter names of a parameter in order to check if it is in the file bcodmo_datetime_parameters.txt.

To infer if a parameter value is a fill value, a list of possible BCO-DMO data fill values defined in a list is checked. The program also infers if an alternate fill value is a possibility. The alternate fill values are indicated in the log file called parameters_overview.txt, but are not included in the output file parameters_summary.json.

Possible data types: string, integer, float, datetime, date, time, and null. Null occurs when a column is filled with NaN values.

TODO: Determine data type of a column with only fill values.

Data type inference order using all column values:

A data type of a parameter column is inferred after removing NaN values and possible fill values from consideration. If a column is entirely a NaN value or entirely a fill value, the data type is null.

The inference is determined by the following: if there is a string value in the column, the whole column is a string. If the parameter name is a BCO-DMO datetime name and it has a datetime format, the data type is one of date, time, or datetime. If there is no string value in a column and it is not a datetime column, if there is a float value, the whole column is a float. If there are no string values, datetimes, or floats, a column type is integer.

If a parameter has an official datetime type name, it is checked as a date, time, or datetime type.

Datetime format inference

Some datetime formats are not included.

%m%d%y is not included because it is difficult to distinguish unless two digit year
is greater than 31 so that it can't be a month or day. And can't distinguish if the
format could be %y%m%d. If assumed only format possible is %m%d%y and it's not a time
datatype (which is currently only determined from the format and not the official time names),
one could check if the first two digits or the second two digits is a day
to determine positioning of the day and month values


"""

# Following steps from to determine datetime format
#
# https://stackoverflow.com/questions/53892450/get-the-format-in-dateutil-parse
#

# TODO
# The program is slower in a Docker environment. To speed up, try the following

# to speed up, see https://stackoverflow.com/questions/64370739/multiprocessing-pool-much-slower-than-manually-instantiating-multiple-processes

# https://stackoverflow.com/questions/47989418/multiprocessing-python-program-inside-docker


# --------------

# TODO
# Set list of definite time variable names and then check them just for possible
# time formats and do the same for dates only and datetimes. Then could avoid possibility of %m%d%y (if included) from being perceived at %H%M%S.


#
# TODO
#
# Make a list of variables in bcodmo datetime formats that ended up being a string. This means a format was unaccounted for
#
# If a datetime format has all NaNs, what is the output?
#

# TODO
# if whole column is a 'fill' value, determine if it is a string, integer or float fill

# TODO
# modify so that NaN counts as a fill value
# and if a column is entirely nan, set data type to None


import os
from pathlib import Path
import re
import pandas as pd
import json
import math
from datetime import datetime
import time
import string
import multiprocessing
import errno

from get_fill_values import *

# import chardet
# from chardet import detect
# import codecs

# Set this to True if want to use program with just a subset of rows in files
TESTING = False
NUMBER_TESTING_ROWS = 5

# Set names of folders and files used
top_data_folder = f"../data"

parameters_overview_file = "../logs/parameters_overview.txt"
parameters_summary_file = "../output/parameters_summary.json"
log_encodings_not_utf8_file = "../logs/log_encodings_not_utf8.txt"
log_no_results_file = "../logs/log_no_results_returned_files.txt"

# Read in possible datetime formats globally so can access in mulitple program sections
possible_formats_file = "possible_datetime_formats.txt"
df_formats = pd.read_fwf(possible_formats_file)
datetime_formats_to_match = df_formats["datetime_formats"].tolist()

# These are names of parameters to look for
# that will have a datetime format inferred for
bcodmo_datetime_parameters_file = "bcodmo_datetime_parameters.txt"
with open(bcodmo_datetime_parameters_file, "r") as f:
    bcodmo_datetime_parameters = f.read().splitlines()

bcodmo_datetime_parameters = [val.lower() for val in bcodmo_datetime_parameters]


def get_dataset_id(csv_file: str) -> str | None:
    """
    Get the dataset id from the csv file to use for finding
    data folders

    Args:
        csv_file (str): CSV data filename

    Returns:
        str | None: dataset_id
    """
    match = re.search(r"/(\d+)/dataURL/.*\.csv$", csv_file)

    if match:
        dataset_id = match.group(1)
    else:
        dataset_id = None

    return dataset_id


def get_is_name_in_bcodmo_datetime_vars(
    col_name: str, parameter_official_names: dict
) -> bool:
    """
    Find if the official parameter name is in the list of BCO-DMO datetime names
    to check for a datetime format

    Args:
        col_name (str): file parameter name
        parameter_official_names (dict): dict of file parameter names to official names

    Returns:
        bool: True if parameter name is a datetime type to infer a format for
    """
    # bcodmo_datetime_parameters = get_bcodmo_datetime_parameters()

    # Get the official name of parameter in the data file
    try:
        parameter_official_name = parameter_official_names[col_name]
    except:
        parameter_official_name = None

    try:
        if parameter_official_name is None:
            name_in_bcodmo_datetimes = col_name.lower() in bcodmo_datetime_parameters
        else:
            name_in_bcodmo_datetimes = (
                col_name.lower() in bcodmo_datetime_parameters
                or parameter_official_name.lower() in bcodmo_datetime_parameters
            )
    except (KeyError, AttributeError):
        # Don't use lower() in case column name is not a string
        name_in_bcodmo_datetimes = col_name in bcodmo_datetime_parameters

    return name_in_bcodmo_datetimes


def get_parameters_info_filename(csv_file: str) -> str | None:
    """_summary_

    Returns:
        str | None: parameters_file
    """

    dataset_id = get_dataset_id(csv_file)

    if dataset_id is not None:
        parameters_folder = f"{top_data_folder}/{dataset_id}/parameters"
        parameters_file = f"{parameters_folder}/{dataset_id}_parameters.json"

        path = Path(parameters_file)
        file_exists = path.is_file()

        if not file_exists:
            parameters_file = None

    else:
        print(
            f"Couldn't get dataset id from filename so can't get parameters info file"
        )
        parameters_file = None

    return parameters_file


def get_official_name(param_name: str, parameter_official_names: dict) -> str | None:
    try:
        parameter_official_name = parameter_official_names[param_name]
    except:
        parameter_official_name = None

    return parameter_official_name


def save_parameters_overview(csv_file: str, final_results: dict):
    # Get one line of parameter values to see sample values
    values = {}
    fill_values = {}
    alt_fill_values = {}
    final_parameters_datetime_formats = {}
    final_parameters_datatypes = {}

    if final_results is not None:
        for col_name, val in final_results.items():
            fill_values[col_name] = final_results[col_name]["fill_value"]
            alt_fill_values[col_name] = final_results[col_name]["alt_fill_value"]

            final_parameters_datetime_formats[col_name] = final_results[col_name][
                "final_format"
            ]
            final_parameters_datatypes[col_name] = final_results[col_name][
                "final_datatype"
            ]

            col_values = val["col_values"]
            values[col_name] = col_values[0]

        final_parameters_datetime_formats = {
            key: value
            for key, value in final_parameters_datetime_formats.items()
            if value
        }

        fill_values = {key: val for key, val in fill_values.items() if val}

        alt_fill_values = {key: val for key, val in alt_fill_values.items() if val}

    with open(parameters_overview_file, "a") as f:
        f.write(f"**********************\n")
        f.write(f"file: {csv_file}\n")
        f.write(f"**********************\n")
        f.write(f"Sample values\n")
        f.write(json.dumps(values, indent=4))
        f.write(f"\nFill values\n")
        f.write(json.dumps(fill_values, indent=4))
        f.write(f"\nAlternate fill values\n")
        f.write(json.dumps(alt_fill_values, indent=4))
        f.write(f"\nFinal datetime formats\n")
        f.write(json.dumps(final_parameters_datetime_formats, indent=4))
        f.write(f"\nFinal parameter data types\n")
        f.write(json.dumps(final_parameters_datatypes, indent=4))
        f.write(f"\n\n")


def write_parameters_final_results(csv_file: str, final_results: dict):
    """_summary_

    Returns:
        _type_: _description_
    """

    filename = Path(csv_file).name

    parameter_names_from_file = final_results.keys()

    summary_obj = {}

    summary_obj["source"] = csv_file
    summary_obj["filename"] = filename

    columns = []

    for parameter_col_name in parameter_names_from_file:
        param_obj = {}

        try:
            final_datatype = final_results[parameter_col_name]["final_datatype"]
        except:
            print(f"Error reading in {parameter_col_name} in file {csv_file}")
            continue

        # Formats
        try:
            final_format = final_results[parameter_col_name]["final_format"]

        except KeyError:
            print(
                f"Error reading in datetime formats for {parameter_col_name} in file {csv_file}"
            )
            continue

        # Fill values
        try:
            fill_value = final_results[parameter_col_name]
            final_fill_value = fill_value["fill_value"]

        except KeyError:
            print(
                f"Error reading in fill values for {parameter_col_name} in file {csv_file}"
            )
            continue

        # Parameter Object composed of datatype, format and fill value
        param_obj[parameter_col_name] = {}

        param_obj[parameter_col_name]["type"] = final_datatype

        if final_format is not None:
            param_obj[parameter_col_name]["format"] = final_format

        if final_fill_value is not None:
            param_obj[parameter_col_name]["fill_value"] = final_fill_value

        columns.append(param_obj)

    summary_obj["columns"] = columns

    json_object = json.dumps(summary_obj, indent=4)

    final_str = json_object + "\n,"

    with open(parameters_summary_file, "a") as f:
        f.write(final_str)


def get_parameter_final_datatype(unique_datatypes: list) -> str | None:
    """_summary_

    Returns:
        str: final_datatype
    """

    # If there is a datatype along with NaN values, don't inlcude the
    # NaN values when determining the data type
    final_datatype = None

    if "isnan" in unique_datatypes:
        unique_datatypes.remove("isnan")
        if not len(unique_datatypes):
            final_datatype = None

    # If there is a fill datatype, remove it to determine the datatype of remaining
    if len(unique_datatypes) == 1 and "isfill" in unique_datatypes:
        # All values are fill.
        final_datatype = None

    if len(unique_datatypes) > 1 and "isfill" in unique_datatypes:
        unique_datatypes.remove("isfill")

    if len(unique_datatypes) == 1:
        final_datatype = unique_datatypes[0]
    else:
        if (
            "float" in unique_datatypes
            and "integer" in unique_datatypes
            and "datetime" not in unique_datatypes
        ):
            final_datatype = "float"

        if (
            "float" in unique_datatypes
            and "integer" not in unique_datatypes
            and "datetime" not in unique_datatypes
        ):
            final_datatype = "float"

        if (
            "integer" in unique_datatypes
            and "float" not in unique_datatypes
            and "datetime" not in unique_datatypes
        ):
            final_datatype = "integer"

        # if more than one datetime datatype (like date and time and datetime), generalize it to datetime
        if (
            "datetime" in unique_datatypes
            or "time" in unique_datatypes
            or "date" in unique_datatypes
            and "integer" not in unique_datatypes
            and "float" not in unique_datatypes
            and "string" not in unique_datatypes
        ):
            final_datatype = "datetime"

        # if (
        #     "date" in unique_datatypes
        #     and "integer" not in unique_datatypes
        #     and "float" not in unique_datatypes
        #     and "string" not in unique_datatypes
        # ):
        #     final_datatype = "date"

        # if (
        #     "time" in unique_datatypes
        #     and "integer" not in unique_datatypes
        #     and "float" not in unique_datatypes
        #     and "string" not in unique_datatypes
        # ):
        #     final_datatype = "time"

        if "string" in unique_datatypes:
            final_datatype = "string"

    return final_datatype


def check_datetime_format_length(
    col_vals: list, format: str | None, datatype: str | None
) -> tuple:
    """_summary_

    Returns:
        _type_: _description_
    """

    out_datatype = datatype
    out_format = format

    if format is not None and datatype is not None:
        is_datetime = "datetime" in datatype or "time" in datatype or "date" in datatype
    else:
        is_datetime = False

    if format == "%H%M" or format == "%H%M.%f" and is_datetime:
        # check that the numeric length is 4 before the decimal point

        first_piece = []
        second_piece = []

        for val in col_vals:
            pieces = val.split(".")
            try:
                if len(pieces[0]) != 4:
                    continue
                first_piece.append(int(pieces[0]))
                second_piece.append(int(pieces[1]))
            except:
                pass

        if len(first_piece) and not len(second_piece):
            out_format = "%H%M"
            out_datatype = "time"
        elif len(first_piece) and len(second_piece):
            out_format = "%H%M.%f"
            out_datatype = "time"
        elif not len(first_piece) and not len(second_piece):
            out_format = None
            out_datatype = "integer"
        elif not len(first_piece) and len(second_piece):
            out_format = None
            out_datatype = "float"

    return out_format, out_datatype


def fine_tune_formats(col_vals: list, unique_formats: list) -> list:
    """
    Take a list of inferred parameter formats and determine which one best
    describes the parameter column values. If one can't be determined,
    return all the incoming formats.

    Returns:
        list: out_formats
    """

    out_formats = unique_formats

    # Check alternate date forms
    # Check two digit positions to see if they are definitely a day value (>12)
    if (
        "%d/%m/%Y" in unique_formats
        and "%m/%d/%Y" in unique_formats
        and len(unique_formats) == 2
    ):
        first_piece = []
        second_piece = []

        for val in col_vals:
            pieces = val.split("/")
            try:
                first_piece.append(int(pieces[0]))
                second_piece.append(int(pieces[1]))
            except:
                pass

        # check if first_piece > 12. Then it's definitely a day
        vals = [val for val in first_piece if val > 12]
        if len(vals):
            is_day_first_piece = True
        else:
            is_day_first_piece = False

        # check if second_piece > 12. Then it's definitely a day
        vals = [val for val in second_piece if val > 12]
        if len(vals):
            is_day_second_piece = True
        else:
            is_day_second_piece = False

        if is_day_first_piece:
            out_format = "%d/%m/%Y"

        elif is_day_second_piece:
            out_format = "%m/%d/%Y"

        else:
            out_format = "starting formats"

        if out_format == "starting formats":
            out_formats = unique_formats
        else:
            out_formats = [out_format]

    # Check alternate date forms
    if (
        "%d-%m-%Y" in unique_formats
        and "%m-%d-%Y" in unique_formats
        and len(unique_formats) == 2
    ):
        first_piece = []
        second_piece = []

        for val in col_vals:
            pieces = val.split("-")
            try:
                first_piece.append(int(pieces[0]))
                second_piece.append(int(pieces[1]))
            except:
                pass

        # check if first_piece > 12. Then it's definitely a day
        vals = [val for val in first_piece if val > 12]
        if len(vals):
            is_day_first_piece = True
        else:
            is_day_first_piece = False

        # check if second_piece > 12. Then it's definitely a day
        vals = [val for val in second_piece if val > 12]
        if len(vals):
            is_day_second_piece = True
        else:
            is_day_second_piece = False

        if is_day_first_piece:
            out_format = "%d/%m/%Y"

        elif is_day_second_piece:
            out_format = "%m/%d/%Y"

        else:
            out_format = "starting formats"

        if out_format == "starting formats":
            out_formats = unique_formats
        else:
            out_formats = [out_format]

        out_formats = [out_format]

    # Check various date forms
    # Here assume 0 padding
    if (
        "%d%m%Y" in unique_formats
        and "%m%d%Y" in unique_formats
        and len(unique_formats) == 2
    ):
        first_piece = []
        second_piece = []

        for val in col_vals:
            if len(val) == 8:
                try:
                    first_piece.append(int(val[0:2]))
                    second_piece.append(int(val[2:4]))
                except:
                    pass

        # check if first_piece > 12. Then it's definitely a day
        vals = [val for val in first_piece if val > 12]
        if len(vals):
            is_day_first_piece = True
        else:
            is_day_first_piece = False

        # check if second_piece > 12. Then it's definitely a day
        vals = [val for val in second_piece if val > 12]
        if len(vals):
            is_day_second_piece = True
        else:
            is_day_second_piece = False

        if is_day_first_piece:
            out_format = "%d%m%Y"

        elif is_day_second_piece:
            out_format = "%m%d%Y"

        else:
            out_format = "starting formats"

        if out_format == "starting formats":
            out_formats = unique_formats
        else:
            out_formats = [out_format]

    # Check various date forms
    # Assume 0 padding
    if (
        "%Y%m%d" in unique_formats
        and "%m%d%Y" in unique_formats
        and len(unique_formats) == 2
    ):
        # 1st 4 chars to possibly be a year
        col_pos_03 = []

        # last 4 chars to possibly be a year
        col_pos_47 = []

        for val in col_vals:
            if len(val) == 8:
                try:
                    col_pos_03.append(int(val[0:4]))
                    col_pos_47.append(int(val[4:]))
                except:
                    pass

        # check if col_pos_03 is bigger than 1231 which is either month > 12 or day > 31
        # then it's definitely a year
        vals = [val for val in col_pos_03 if val > 1231]
        if len(vals):
            is_year_col_pos_03 = True
        else:
            is_year_col_pos_03 = False

        # check if col_pos_47 is bigger than 1231, which is either month > 12 or day > 31
        # then it's definitely a year
        vals = [val for val in col_pos_47 if val > 1231]
        if len(vals):
            is_year_col_pos_47 = True
        else:
            is_year_col_pos_47 = False

        if is_year_col_pos_03:
            out_format = "%Y%m%d"

        elif is_year_col_pos_47:
            out_format = "%m%d%Y"

        else:
            out_format = "starting formats"

        if out_format == "starting formats":
            out_formats = unique_formats
        else:
            out_formats = [out_format]

    # Check alternate date forms
    if (
        "%d/%m/%y" in unique_formats
        and "%m/%d/%y" in unique_formats
        and len(unique_formats) == 2
    ):
        first_piece = []
        second_piece = []

        for val in col_vals:
            pieces = val.split("/")
            try:
                first_piece.append(int(pieces[0]))
                second_piece.append(int(pieces[1]))
            except:
                pass

        # check if first_piece > 12. Then it's definitely a day
        vals = [val for val in first_piece if val > 12]
        if len(vals):
            is_day_first_piece = True
        else:
            is_day_first_piece = False

        # check if second_piece > 12. Then it's definitely a day
        vals = [val for val in second_piece if val > 12]
        if len(vals):
            is_day_second_piece = True
        else:
            is_day_second_piece = False

        if is_day_first_piece:
            out_format = "%d/%m/%y"

        elif is_day_second_piece:
            out_format = "%m/%d/%y"

        else:
            out_format = "starting formats"

        if out_format == "starting formats":
            out_formats = unique_formats
        else:
            out_formats = [out_format]

    # Check various time forms
    #  "%H:%M:%S" and "%H:%M:%S%f"
    if (
        "%H:%M:%S" in unique_formats
        and "%H:%M:%S%f" in unique_formats
        and len(unique_formats) == 2
    ):
        out_format = "%H:%M:%S%f"
        out_formats = [out_format]

    # Check various time formats
    # "%H%M" and "%H%M.%f"
    if (
        "%H%M" in unique_formats
        and "%H%M.%f" in unique_formats
        and len(unique_formats) == 2
    ):
        out_format = "%H%M.%f"
        out_formats = [out_format]

    return out_formats


def find_datatypes_from_formats(unique_formats: list) -> list:
    """_summary_

    Returns:
        _type_: _description_
    """

    # If there are multiple datetime formats, a format can't be determined,
    # so the datatype will not be a datetime
    # Now determine the datatype if it's not a datetime

    # Replace % in format with '' and then check if there is one decimal point
    # to determine if it's an integer or float. But if there are extra characters
    # like ":", call it a string.

    new_datatypes = []
    for format in unique_formats:
        bare_format = format.replace("%", "")

        non_alpha_chars = []
        for character in bare_format:
            if not character.isalpha():
                non_alpha_chars.append(character)

        if bare_format.isalpha():
            datatype = "integer"
        elif "." in non_alpha_chars:
            # Find number of chars if one period removed
            num_chars = len(non_alpha_chars) - 1

            # Check if any chars left, and if they are, the datatype is not a float
            if num_chars:
                datatype = "string"
            else:
                datatype = "float"
        else:
            datatype = "string"

        new_datatypes.append(datatype)

    # Now get unique datatypes. If they are different, the datatype is a string
    unique_datatypes = list(set(new_datatypes))

    return unique_datatypes


def get_parameter_unique_datatypes(
    col_name: str,
    col_values: list,
    datatypes: list,
    parameter_formats: list,
    parameter_official_names: dict,
) -> list:
    """
    Fine tune whether a datetime type is date, time, or datetime
    Need to know what the format looks like,
    If it has H,M, or S in it, and no other letters, it's a time
    If it has no H,M,S, it's a date

    Returns:
        list: unique_datatypes
    """

    name_in_bcodmo_datetimes = get_is_name_in_bcodmo_datetime_vars(
        col_name, parameter_official_names
    )

    time_format_letters = ["H", "M", "S", "f"]

    alphabet = string.ascii_lowercase + string.ascii_uppercase
    date_format_letters = alphabet.translate({ord(letter): None for letter in "HMSfzZ"})

    date_format_letters = list(date_format_letters)

    new_datatypes = []

    for i in range(len(datatypes)):
        elem_datatype = datatypes[i]
        elem_format = parameter_formats[i]
        col_val = col_values[i]

        if elem_format is not None and elem_datatype == "datetime":
            datatype_letters = re.split(r"[^a-zA-Z]*", elem_format)

            common_time_letters = list(set(time_format_letters) & set(datatype_letters))

            common_date_letters = list(set(date_format_letters) & set(datatype_letters))

            if (
                common_time_letters
                and not common_date_letters
                and name_in_bcodmo_datetimes
            ):
                elem_datatype = "time"

            elif (
                not common_time_letters
                and common_date_letters
                and name_in_bcodmo_datetimes
            ):
                elem_datatype = "date"

            else:
                elem_datatype = "datetime"

            new_datatypes.append(elem_datatype)

        elif elem_format is None and elem_datatype == "datetime":
            # keep datatype datetime and if format = None,
            # go through list of options to find the datatype
            # Would need the col value to do this

            possible_fill_values = get_possible_fill_values()

            if col_val in possible_fill_values:
                datatype = "isfill"
            else:
                try:
                    val_float = float(col_val)

                    if math.isnan(val_float):
                        datatype = "isnan"
                    elif "." not in col_val:
                        datatype = "integer"
                    else:
                        datatype = "float"
                except:
                    datatype = "string"

            new_datatypes.append(datatype)

        else:
            new_datatypes.append(elem_datatype)

    unique_datatypes = list(set(new_datatypes))

    unique_datatypes = [elem for elem in unique_datatypes if elem]

    # If there is a fill datatype, remove it to determine the datatype of remaining
    if len(unique_datatypes) == 1 and "isfill" in unique_datatypes:
        # All values are fill.
        unique_datatypes = [None]

    elif len(unique_datatypes) > 1 and "isfill" in unique_datatypes:
        unique_datatypes.remove("isfill")

    return unique_datatypes


def infer_values_second_pass(results: dict, parameter_official_names: dict) -> dict:
    """_summary_

    Returns:
        _type_: _description_
    """

    # Refine datetime to be time, date, or datetime

    # time_format_letters = ["H", "M", "S", "f"]

    # alphabet = string.ascii_lowercase + string.ascii_uppercase
    # date_format_letters = alphabet.translate({ord(letter): None for letter in "HMSfzZ"})

    # date_format_letters = list(date_format_letters)

    column_names = list(results.keys())

    final_results = {}

    for col_name in column_names:
        final_results[col_name] = {}

        col_values = results[col_name]["col_values"]
        final_results[col_name]["col_values"] = col_values

        # final_results[col_name]["col_possible_fill"] = results[col_name][
        #     "col_possible_fill"
        # ]

        datatypes = results[col_name]["col_datatypes"]

        parameter_formats = results[col_name]["col_formats"]

        # Find unique datatypes from looking at each parameter datatype and format
        # unique_datatypes may be None if the format for a value is None but has a datetime datatype
        # Will analyze this None datatype later to determine it

        # TODO, change this behavior to keep datatype datetime and if format = None,
        # go through list of options to find the datatype

        unique_datatypes = get_parameter_unique_datatypes(
            col_name,
            col_values,
            datatypes,
            parameter_formats,
            parameter_official_names,
        )

        # Get unique parameter formats and fine-tune
        if len(parameter_formats):
            unique_formats = list(set(parameter_formats))
            unique_formats = [val for val in unique_formats if val]

            if not unique_formats:
                unique_formats = None
        else:
            unique_formats = None

        # If more than one format, see if can fine-tune to one best format
        if unique_formats is not None:
            unique_formats = fine_tune_formats(col_values, unique_formats)

        # If there are still more than one unique_format even after fine tuning,
        # set the unique_format to None and change the datatype to an integer, float, or string
        # depending on the formats.
        # A case is when a time parameter is of the form HHMMSS but is indistinguishable from
        # multiple date formats. For example, it could have both formats "%Y%m%d" and "%m%d%Y".
        # Since 'time' is in the parameter name, it should not be a date format but a time format.

        final_format = None

        if unique_formats is not None and len(unique_formats) > 1:
            unique_datatypes = find_datatypes_from_formats(unique_formats)
            final_format = None

        elif unique_formats is not None and len(unique_formats) == 1:
            final_format = unique_formats[0]

        # TODO
        # If the unique datatypes include string and datetime,
        # the final datatype will be string, but the format will still exist
        # because some of the column values have a datetime format. In this
        # case, should the datetime format become None?
        final_datatype = get_parameter_final_datatype(unique_datatypes)

        # Check if a datetime datatype has an expected length and if not
        # return a new format and datatype

        final_format, final_datatype = check_datetime_format_length(
            col_values, final_format, final_datatype
        )

        final_results[col_name]["final_format"] = final_format
        final_results[col_name]["final_datatype"] = final_datatype

    return final_results


def get_parameter_datatypes(
    col_name: str, col_vals: list, parameter_official_names: dict
) -> list:
    """
    Get the datatype for each value in a parameter column and the
    fill value (out of a list of possible fill values) for a parameter column.

    If column datatypes is a string, don't set a fill value because
    a possible fill value could be a valid string value for the parameter

    Returns:
        list: parameter_datatypes
    """

    is_name_in_bcodmo_datetime_vars = get_is_name_in_bcodmo_datetime_vars(
        col_name, parameter_official_names
    )

    parameter_datatypes = []

    possible_fill_values = get_possible_fill_values()

    for i in range(len(col_vals)):
        col_val = col_vals[i]

        # Remove any spaces that a column value might have
        col_val = col_val.strip()

        if is_name_in_bcodmo_datetime_vars:
            is_datetime = True

        else:
            is_datetime = False

        # Mark as a fill value before marking as a string or datetime
        if col_val in possible_fill_values:
            datatype = "isfill"
        elif is_datetime:
            datatype = "datetime"
        else:
            try:
                val_float = float(col_val)

                if math.isnan(val_float):
                    datatype = "isnan"
                elif "." not in col_val:
                    datatype = "integer"
                else:
                    datatype = "float"
            except:
                datatype = "string"

        parameter_datatypes.append(datatype)

    return parameter_datatypes


def get_parameters_datetime_formats(
    col_name: str, col_vals: list, parameter_official_names: dict
) -> list:
    """
    Using reference file of possible datetime formats if a parameter name
    is one of the BCO-DMO datetime names, find all datetime matches and
    return a list of them.

    """

    is_name_in_bcodmo_datetime_vars = get_is_name_in_bcodmo_datetime_vars(
        col_name, parameter_official_names
    )

    result = []

    for i in range(len(col_vals)):
        parsed_timestamps = {"col_val": col_vals[i], "matches": []}

        if is_name_in_bcodmo_datetime_vars:
            for f in datetime_formats_to_match:
                try:
                    d = datetime.strptime(col_vals[i], f)
                except:
                    continue

                parsed_timestamps["matches"].append({"datetime": d, "format": f})

        matches = parsed_timestamps["matches"]

        new_formats = []
        if matches:
            for match in matches:
                format = match["format"]
                new_formats.append(format)
        else:
            new_formats.append(None)

        result.append(new_formats)

    result = [item for sublist in result for item in sublist]

    return result


# Testing option included in function to limit number of rows to process
def infer_values_first_pass(df: pd.DataFrame, parameter_official_names: dict) -> dict:
    column_names = df.columns

    if TESTING:
        # process limited number of rows to infer datatypes and datetime formats
        df_new = df.iloc[0:NUMBER_TESTING_ROWS]
    else:
        df_new = df.copy()

    results = {}

    for col_name in column_names:
        results[col_name] = {}

        column = df_new[col_name].copy()

        # is_name_in_bcodmo_datetime_vars = get_is_name_in_bcodmo_datetime_vars(
        #     col_name, parameter_official_names
        # )

        col_vals = list(column.values)

        # Infer datetime formats for a column, and if not a datetime column,
        # return None
        parameter_datetime_formats = get_parameters_datetime_formats(
            col_name, col_vals, parameter_official_names
        )

        # TODO
        # If parameter_datetime_formats is just None values, write to file
        # because most likely that format is not in the list of datetime formats to look at

        parameter_datatypes = get_parameter_datatypes(
            col_name, col_vals, parameter_official_names
        )

        results[col_name]["col_values"] = col_vals
        results[col_name]["col_datatypes"] = parameter_datatypes
        results[col_name]["col_formats"] = parameter_datetime_formats

    return results


def get_parameters_official_names(csv_file: str, parameter_col_names: list) -> dict:
    """
    Read in the parameters info file to get the corresponding official name
    of supplied parameter names. Will use this later to determine which
    parameters are datetimes and then get their formats.

    Supplied parameter names from the parameter file mapped to BCO-DMO
    official names which are stored in the parameters info file

    """

    parameters_info_filename = get_parameters_info_filename(csv_file)

    if parameters_info_filename is None:
        print("No parameters info file")

        # Supplied parameter names translated to BCO-DMO official names,
        # but there are none to available to map to
        parameter_official_names = {name: None for name in parameter_col_names}

    else:
        with open(parameters_info_filename, "r") as f:
            parameters_info = json.load(f)

        parameter_supplied_names = []
        parameter_official_names = {}

        for parameter in parameters_info:
            try:
                supplied_name = parameter["supplied_name"]
            except KeyError:
                supplied_name = None

            try:
                official_name = parameter["parameter_official_name"]
            except KeyError:
                official_name = None

            parameter_supplied_names.append(supplied_name)
            parameter_official_names[supplied_name] = official_name

    return parameter_official_names


# def get_file_encoding(file: str) -> typing.Union[str, None]:

#     print(f'file in get_file_encoding is {file}')

#     with open(file, 'rb') as rawdata:
#         result = chardet.detect(rawdata.read(100000))

#         encoding = result['encoding']

#     return encoding


def read_file(filename: str) -> pd.DataFrame:
    """
    Read in file to a pandas dataframe and keep values as strings
    so that integers aren't coerced to float or string depending on a fill value
    And keep NaN text, so set keep_default_na to False.

    Try opening files with different encodings in case there is a UnicodeDecodeError
    when opening the file to read it. Try the UTF-8, Windows-1252,
    and Latin1 encodings

    Returns:
        pd.DataFrame: df
    """

    try:
        try:
            df = pd.read_csv(
                filename,
                encoding="utf-8",
                dtype=str,
                keep_default_na=False,
                skipinitialspace=True,
                sep=",",
            )
        except pd.errors.ParserError as e:
            df = pd.DataFrame()
            print(f"Could not open {filename} with pandas to read it in with utf-8 \n")
            print("Pandas parse error")
            print(e)
    except UnicodeDecodeError as e:
        print(f"UnicodeDecodeError for {filename} opening with utf-8")
        try:
            try:
                df = pd.read_csv(
                    filename,
                    encoding="windows-1252",
                    dtype=str,
                    keep_default_na=False,
                    skipinitialspace=True,
                    sep=",",
                )
                with open(log_encodings_not_utf8_file, "a") as f:
                    f.write(f"{filename} encoding is windows-1252\n")

            except pd.errors.ParserError as e:
                df = pd.DataFrame()
                print(
                    f"Could not open {filename} with pandas to read it in with windows-1252\n"
                )
                print("Pandas parse error")
                print(e)
        except UnicodeDecodeError as e:
            print(f"UnicodeDecodeError for {filename} opening with windows-1252")
            try:
                try:
                    df = pd.read_csv(
                        filename,
                        encoding="latin1",
                        dtype=str,
                        keep_default_na=False,
                        skipinitialspace=True,
                        sep=",",
                    )

                    with open(log_encodings_not_utf8_file, "a") as f:
                        f.write(f"{filename} encoding is latin1\n")

                except pd.errors.ParserError as e:
                    df = pd.DataFrame()
                    print(
                        f"Could not open {filename} with pandas to read it in with ;latin1\n"
                    )
                    print("Pandas parse error")
                    print(e)
            except UnicodeDecodeError as e:
                print(
                    f"UnicodeDecodeError: {filename} not opened as utf-8, windows-1252 or latin1"
                )
                with open(log_encodings_not_utf8_file, "a") as f:
                    f.write(f"{filename} encoding unknown and not opened\n")
                df = pd.DataFrame()

    return df


def get_final_params_datatypes_formats_fill(csv_file: str) -> dict | None:
    # Read in file to a pandas dataframe (all string values)
    df = read_file(csv_file)

    # Get parameter column names as listed in the csv file
    column_names = list(df.columns)

    # Get associated official names for each parameter in the csv file
    # This will be used to determine if a parameter is classified as a
    # datetime (time, date, datetime)
    parameter_official_names = get_parameters_official_names(csv_file, column_names)

    # Do a first pass of inferring to get lists of unique values of
    # format, datatype and fill value for each row in column.
    # Also retrieve the column values into a results dict
    if not df.empty:
        results = infer_values_first_pass(df, parameter_official_names)

    else:
        results = None

    # Fine tune results to get one format and one datatype
    if results:
        try:
            final_results = infer_values_second_pass(results, parameter_official_names)
        except:
            final_results = None
    else:
        final_results = None

        print(f"{csv_file} has no results")
        with open(log_no_results_file, "a") as f:
            f.write(f"{csv_file}\n")

    # Use results (values for each row and col) and final results (values for each col) to determine fill values
    if results and final_results:
        final_results = determine_fill_values(results, final_results)

    return final_results


def process_file(file: Path):
    csv_file = file.as_posix()

    print(f"\n******************\n")
    print(f"File being processed is {csv_file}\n")

    final_results = get_final_params_datatypes_formats_fill(csv_file)

    if final_results is not None:
        # If multiple formats for param, write info to a log file for referencing
        # later to see if will add to current set of possible fill values
        # and not the final file
        save_parameters_overview(csv_file, final_results)

        write_parameters_final_results(csv_file, final_results)


def main():
    parameters_overview_path = Path(parameters_overview_file)
    parameters_overview_path.unlink(missing_ok=True)

    log_encodings_not_utf8_path = Path(log_encodings_not_utf8_file)
    log_encodings_not_utf8_path.unlink(missing_ok=True)

    log_no_results_file_path = Path(log_no_results_file)
    log_no_results_file_path.unlink(missing_ok=True)

    # Remove summary file since want to start fresh for each
    # program run as the output is appended
    os.makedirs("../output", exist_ok=True)
    try:
        os.remove(parameters_summary_file)
    except OSError as e:
        if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or directory
            raise  # re-raise exception if a different error occurred

    files = Path(top_data_folder).glob("**/dataURL/*.csv")

    file_list = list(files)

    # TODO
    # Create Test files

    # The following comments are files to check for various issues

    # test 816278_v1_Chlorophyll_a.csv to look if supplied name UTC_Date has
    # official name in bcodmo_datetimes

    # overflow error
    # variable name is PAR_doghouse
    # [val] is ['33970276008492.5937']
    # if csv_file.name != '3645_v1_underway_met.csv':
    #     continue

    # Look at Iso time with an offset, not catching it all because of the : at the end
    # sample time is "2014-04-30T12:16:00-08:00"
    # for variable ISO_DateTime_Local, most likely is [%Y, -, %m, -, %d, T, %H, :, %H, :, %H, -, %m, :, %H]
    # decided to use pandas instead but inferred is close but uses Hour12 instead of Hour24
    # if csv_file.name != '765108_v1_San_Dieguito_Lagoon_Carbon_export.csv':
    #     continue

    # Problem with guessing the format of date
    # should be %Y-%m-%d but it says %Y-%d-%m
    # It should have looked at the whole column and counted probabilities
    # and if one format is ruled out, don't pick it.
    # But I'm only using a count of 5 rows, that could be it
    # When I use 500 rows, it becomes %Y-%d-%m
    # But also a problem of not realizing a value by
    # the official name and using pandas
    # if csv_file.name != '780092_v1_Squidpop_Assay.csv':
    #     continue

    # has a None parameter name
    # No parameters info file
    # if csv_file.name != '709880_v1_Visual_monitoring_of_A__cervicornis.csv':
    #     continue

    # has a time of format HHMM.SS
    # if csv_file.name != '3640_v1_OC473_alongtrack.csv':
    #     continue

    # Iso date ending with .00Z (and need to strip this off for pandas guess)
    # if csv_file.name != '503133_v1_GP16_CTD___GT_C_Bottle.csv':
    #     continue

    # time_gmt format wrong, why is a '.' being added?
    # "time_gmt": 1012.07
    # "time_gmt": "%Y.%m", should be %H%M.%S
    # if csv_file.name != '2315_v1_emet_W60_1998.csv':
    #     continue

    # Only using 5 rows, but
    # "date_local": "06242010", and get "%Y%d%m" but it should be %m%d%Y
    # What if I set min year at 1000? or at least 32
    # if csv_file != '516495_v1_POC_PON_isotopes.csv':
    #     continue

    # Problem with Iso UTC being null
    # "ISO_DateTime_UTC": "2018-08-16T14:00:13"
    # if csv_file.name != '786013_v1_14C_32Si_Experimental.csv':
    #     continue

    # Problem with iso and + adjustment
    # "ISO_DateTime_UTC": "2013-05-19T20:53:30+0000"
    # and no format inferred
    # if csv_file.name != '3953_v1_event_log.csv':
    #     continue

    # If NaN, don't put a Z on format
    # "ISO_DateTime_UTC": "NaNZ"
    # if csv_file.name != '809945_v1_Lake_Erie_Winter_Surveys_2018_2019.csv':
    #     continue

    # encoding error when opening as utf-8. fixed now
    # if csv_file.name != '2472_v1_trawl_catch___GoA.csv':
    #     continue

    # # error encoding file
    # if csv_file.name != '3758_v1_alongtrack.csv':
    #     continue

    # File with quotes in it because of columns within parameter value
    # if csv_file.name != '717994_v1_Cruise_Event_Log___HRS1415.csv':
    #     continue

    # File with multiple datetime formats for one parameter
    # if csv_file.name != '814713_v1_Sulfonates_in_plankton_cultures.csv':
    #     continue

    # Check z values to see if can find a fill value
    # Mabe see if all values are positive and have a -9, -99, -999 etc value
    # So if all positive and one negative value
    # Put a check in the program looking for values with a series of 9 in them
    # So split string, get unique set, look only at numeric part and see if value is 9

    # 3358_v1_Bottle_Data.csv
    # file_list = [file for file in file_list if file.name == "3358_v1_Bottle_Data.csv"]

    # 765327_v1_TORCH_II.csv not saved to output file. why?
    # file_list = [file for file in file_list if file.name == "765327_v1_TORCH_II.csv"]

    # for file 753036_v1_Lipid_Dendraster_OA_Expt2017.csv, the data type is 'string' for param stomach_area, but there
    # are float values along with a 'NA' fill. Why is it not float as a final data type? Because the param lipid_area also
    # has float values and a fill of 'NA', but it is listed as a float type.
    # Must be a logic error if find string, then all is a string, but if NA is a fill, it's
    # not a string

    # file with a possible fill
    # file_list = [
    #     file
    #     for file in file_list
    #     if file.name
    #     == "644840_v1_Cellular_element_quotas__Si_in_Synechococcus_cells.csv"
    # ]

    # see 644840_v1_Cellular_element_quotas__Si_in_Synechococcus_cells.csv
    # If type is a string, don't look for a fill value
    # But if see an integer or float value in datatype, look for a fill value.
    # If fill value is not one of possible fill values list and not a minus 9s value,
    # the alternate fill value counts as a string and this makes the datatype a
    # string, even if it's a float
    # "cell_S": {
    #     "type": "string",
    #     "fill_value": "bd"
    # }
    # file_list = [
    #     file
    #     for file in file_list
    #     if file.name
    #     == "644840_v1_Cellular_element_quotas__Si_in_Synechococcus_cells.csv"
    # ]

    # see 815092_v1_Wrack_Composition_and_Abundance.csv
    # If type is string, don't look for a fill because it could stand
    # for a value and not a fill value
    # "Type_Code": {
    #     "type": "string",
    #     "fill_value": "NA"
    # }

    # see 652223_v1_MUSiCC_OC1504A___Bacteria_Virus_and_Chlorophyll_Containing_Cell_Abundance.csv
    # if have a fill_value that is not one of possible fill values or minus 9s
    # and datatype is not a string
    # save it to dict as "alt_fill_value" and save to log parameters_overview.txt
    # "station": {
    #     "type": "integer",
    #     "fill_value": "test2-stn01"
    # }

    # see 749412_v1_Geltrap_micrographs.csv
    # Not fine tuning date. Looking at file, it should be %m/%d/%y
    # "Deploy_Date_UTC": [
    #     "%m/%d/%y",
    #     "%d/%m/%y"
    # ],
    # file_list = [
    #     file for file in file_list if file.name == "749412_v1_Geltrap_micrographs.csv"
    # ]

    # see 3911_v1_CTD_Profiles,csv
    # Check why format can't be determined
    # "Time_Local": {
    #     "type": "date",
    #     "format": [
    #         "%Y%m%d",
    #         "%y%m%d",
    #         "%m%d%y",
    #         "%m%d%Y"
    #     ]
    # }
    # has values: 155231, 170025, 60658, 224852
    # why is %Y%m%d even selected if char length is 6 or less,
    # should be 8 according to %m%d%Y
    #
    # TODO
    # Have choice of setting datatype back to string and format to None if have
    # more than one format to choose from
    # file_list = [file for file in file_list if file.name == "3911_v1_CTD_Profiles.csv"]

    # TODO
    # check format of %H%M.%f when times are of form 61.88
    # Need to check if it is of length 4 before the decimal point
    # file_list = [file for file in file_list if file.name == "734364_v1_Shipboard_bioassay_experiments.csv"]
    # because for file 3907_v1_lionfish_cleaner_bottle_expt.csv, %H%M.%f makes sense for time of 1038.43

    # 2783_v1_Bigelow_Bight_sm_invertebrates.csv
    # The parameter date_local has a datetime format but it's data type
    # is string becomes some of the values end with a ? mark

    # No results returned for this file. Has a pandas parsing error.
    # Expected 20 fields and saw 21
    # filename = "3111_v1_MOC_zoop_AK_LTOP.csv"

    # Expect notes field to be string type with NaN fill
    # but it's not indicating the fill value because I don't
    # look for a fill in a string column. The idea was maybe
    # a fill value could be a valid string. But should I just check
    # for NaN fill at least in a string column?
    # filename = "811614_v2_GN01_Dissolved_Fe_II_.csv"

    # file_list = [file for file in file_list if file.name == filename]

    num_files = len(file_list)
    print(f"Number of files to process is {num_files}")

    num_cores = multiprocessing.cpu_count()

    start_time = time.time()

    PROCESSES = num_cores - 2
    with multiprocessing.Pool(PROCESSES) as pool:
        pool.map(process_file, file_list)

    try:
        # Add [] to summary file of dicts of datatypes and formats
        with open(parameters_summary_file, "r") as f:
            summary = f.read()

        # strip last ','
        summary = summary[:-1]

        final_str = f"[{summary}]"

        with open(parameters_summary_file, "w") as f:
            f.write(final_str)
    except FileNotFoundError:
        print("Summary file not created")

    end_time = time.time()

    print(f"program took {(end_time - start_time)/60} minutes")


if __name__ == "__main__":
    main()
