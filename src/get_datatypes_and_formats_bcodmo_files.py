# Create JSON information files

# Sample JSON information file looks like this
#
# filename = dataset_2291.json:
# ```
# {
#   "source": "/jgofs-capture/2299/dataURL/chloro_bottle_ctd_join.csv",
#   "filename": "chloro_bottle_ctd_join.csv",
#   "columns": [
#     "cruiseid": {"type": "string"},
#     "year": {"type":"integer"},
#     "lat": {"type": "float"},
#     "time_local": {"type": "time", format: "HH:mm:ss"}
#     ...
#   ]
# }
# ```

# to speed up, see https://stackoverflow.com/questions/64370739/multiprocessing-pool-much-slower-than-manually-instantiating-multiple-processes

# https://stackoverflow.com/questions/47989418/multiprocessing-python-program-inside-docker

# TODO
# Add logging

from pathlib import Path
import re
import pandas as pd
import numpy as np
import json
import math
from datetime import datetime
import time
import string
import multiprocessing
from scipy import stats

# import chardet
# from chardet import detect
# import codecs

# ## Idea for processing
#
# Use the process parameter via a check of possible formats for each row. Then collect them and do probability stats on which ones are most likely
#
# First step, get the unique possibilities
#
# If it is NaN, skip over it
#
# Following steps from
#
# https://stackoverflow.com/questions/53892450/get-the-format-in-dateutil-parse
#
#
# TODO
#
# Make a list of variables in bcodmo datetime formats that ended up being a string. This means a format was unaccounted for
#
# If a datetime format has all NaNs, what is the output?
#


top_data_folder = f"../data"

log_errors = "log_files_with_errors_opening.txt"
log_output = "parameter_output.txt"
log_encodings_not_utf8 = "log_file_no_utf8_encoding.txt"

possible_formats_file = "possible_datetime_formats.txt"
df_formats = pd.read_fwf(possible_formats_file)
datetime_formats_to_match = df_formats["datetime_formats"].tolist()


# List of possible nan types in BCO-DMO data files
def get_possible_fill_values() -> list:
    possible_fill_values = ["nd", "ND"]

    # Could also be NA, but this can also stand for sodium
    additional_nan_fills = ["n.d.", "n.a.", "N/A", "na", "n/a"]

    possible_fill_values.extend(additional_nan_fills)

    return possible_fill_values


def get_bcodmo_datetime_parameters() -> list:
    """
    Get list of parameters that have a datetime datatype

    These will be the parameters that will have a format inferred for

    Returns:
        list: BCO-DMO datetime parameter official names
    """
    bcodmo_datetime_parameters_file = "bcodmo_datetime_parameters.txt"

    with open(bcodmo_datetime_parameters_file, "r") as f:
        bcodmo_datetime_parameters = f.read().splitlines()

    bcodmo_datetime_parameters = [val.lower() for val in bcodmo_datetime_parameters]

    return bcodmo_datetime_parameters


def get_dataset_id(csv_file: str) -> str | None:
    match = re.search(r"/(\d+)/dataURL/.*\.csv$", csv_file)

    if match:
        dataset_id = match.group(1)
    else:
        dataset_id = None

    return dataset_id


def get_is_name_in_bcodmo_datetime_vars(
    col_name: str, parameter_official_names: dict, bcodmo_datetime_parameters: list
) -> bool:
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
        name_in_bcodmo_datetimes = col_name.lower() in bcodmo_datetime_parameters

    return name_in_bcodmo_datetimes


def get_parameters_info_filename(dataset_id: str | None) -> str | None:
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


def save_output(csv_file: str, final_results: dict):
    log_output = "parameter_output.txt"

    # Get one line of parameter values to see sample values
    values = {}
    fill_values = {}
    final_parameters_datetime_formats = {}
    final_parameters_datatypes = {}

    if final_results is not None:
        # for col_name, val in results.items():
        for col_name, val in final_results.items():
            fill_values[col_name] = final_results[col_name]["fill_value"]

            final_parameters_datetime_formats[col_name] = final_results[col_name][
                "unique_formats"
            ]
            final_parameters_datatypes[col_name] = final_results[col_name][
                "unique_datatypes"
            ]

            col_values = val["col_values"]
            values[col_name] = col_values[0]

        final_parameters_datetime_formats = {
            key: values
            for key, values in final_parameters_datetime_formats.items()
            if not all(val is None for val in values)
        }

        fill_values = {key: val for key, val in fill_values.items() if val}

    with open(log_output, "a") as f:
        f.write(f"**********************\n")
        f.write(f"file: {csv_file}\n")
        f.write(f"**********************\n")
        f.write(f"Sample values\n")
        f.write(json.dumps(values, indent=4))
        f.write(f"\nPossible fill values\n")
        f.write(json.dumps(fill_values, indent=4))
        f.write(f"\nFinal datetime formats\n")
        f.write(json.dumps(final_parameters_datetime_formats, indent=4))
        f.write(f"\nFinal parameter data types\n")
        f.write(json.dumps(final_parameters_datatypes, indent=4))
        f.write(f"\n\n")


# Get data types and formats using inference of whole colummns
def get_parameters_summary_obj(
    csv_file: str, parameter_col_names, final_results
) -> dict | None:
    # Output example
    # {
    # "source": "/jgofs-capture/2299/dataURL/chloro_bottle_ctd_join.csv",
    # "filename": "chloro_bottle_ctd_join.csv",
    # "columns": [
    #     "cruiseid": {"type": "string"},
    #     "year": {"type":"integer"},
    #     "lat": {"type": "float"},
    #     "time_local": {"type": "time", format: "HH:mm:ss"}
    #     ...
    # ]
    # }

    # TODO
    # Better define what an error is below. Is it a problem with the file or reading params

    # For each parameter, create columns entry with datatype and formats just for datetime parameters
    datatypes = {}
    datetime_formats = {}

    summary_obj = {}
    summary_obj["source"] = csv_file
    summary_obj["filename"] = Path(csv_file).name

    columns = []

    parameter_names_from_file = final_results.keys()

    for parameter_col_name in parameter_col_names:
        # already included parameter names from file if all parameter infor names none
        # TODO
        # make list of files with parameter names in json file that
        # are not in the csv file
        if parameter_col_name not in parameter_names_from_file:
            continue

        # Need this to match the column names in the files,
        # but for final output, want the official name

        try:
            datatype = final_results[parameter_col_name]
            datatypes[parameter_col_name] = datatype
        except:
            print("Error reading in final_parameters_datatypes[parameter_name]")
            print(parameter_col_name)
            print(csv_file)

    #     try:
    #         datetime_format = final_parameters_datetime_formats[parameter_col_name]
    #     except KeyError:
    #         datetime_format = None

    #     datetime_formats[parameter_col_name] = datetime_format

    #     column_value = {}
    #     column_value["type"] = datatypes[parameter_col_name]

    #     print(final_parameters_datetime_formats)

    #     col_non_null_formats = {}
    #     for col_name,formats in final_parameters_datetime_formats.items():
    #         col_non_null_formats[col_name] = [format for format in formats if format is not None]

    #     # Check if formats all None
    #     # a = [format for format in col_formats if format is not None]
    #     # print(a)

    #     if col_non_null_formats[parameter_col_name]:
    #         column_value["format"] = datetime_formats[parameter_col_name]

    #         print(f'column value format {column_value["format"]}')

    #     try:
    #         column_entry_str = f'"{parameter_col_name}": {{"type":{column_value["type"]}, "format": {column_value["format"]}}}'
    #     except KeyError:
    #         column_entry_str = f'"{parameter_col_name}": {{"type": {column_value["type"]}}}'

    #     print('column entry string')
    #     print(column_entry_str)

    #     columns.append(column_entry_str)

    # summary_obj['columns'] = columns

    summary_obj = {}

    return summary_obj


def check_possible_minus_9s_fill_value(
    col_name: str, minus_9s: list, numeric_values: list
) -> int | float | None:
    fill_value = None

    # minus_9s fill takes priority over any string values
    if len(minus_9s):
        found_9s_fill = list(set(minus_9s))
        print(f"{col_name} possible minus 9s fill {found_9s_fill}")

        # Don't set as a fill value if there are multiple versions of a minus_9s fill
        if len(found_9s_fill) == 1:
            found_fill = found_9s_fill[0]
        else:
            found_fill = None

        if found_fill is not None:
            negative_numeric_values = [
                val
                for val in numeric_values
                if val < 0 and not math.isclose(val, float(found_fill))
            ]

            if len(negative_numeric_values):
                # Until check for larger value than possible for negative values,
                # don't include a minus_9s fill for negative numeric values
                print(
                    f"Fill value found in negative list of col values other than fill"
                )
                fill_value = None
            else:
                fill_value = found_fill

        else:
            fill_value = None

    return fill_value


def check_is_minus_9s(value):
    # split string value into a list of chars
    pieces = list(value)
    unique_pieces = list(set(pieces))

    if len(unique_pieces) == 2:
        is_minus_9s = "-" in unique_pieces and "9" in unique_pieces

    elif len(unique_pieces) == 3:
        is_minus_9s = (
            "-" in unique_pieces and "9" in unique_pieces and "." in unique_pieces
        )
    else:
        is_minus_9s = False

    return is_minus_9s


def find_fill_and_numeric_values(col_value: str, datatype: str) -> tuple:
    value = col_value.strip()

    string_value = None
    minus_9s_value = None
    numeric_value = None

    if datatype == "isfill":
        numeric_value = np.NaN
    elif datatype == "string":
        string_value = value
        numeric_value = np.NaN
    elif datatype == "integer":
        # check if it is negative and then all 9's
        if check_is_minus_9s(value):
            minus_9s_value = value
        try:
            numeric_value = int(value)
        except ValueError:
            # invalid literal for int() with base 10: '1e-12' for 765327_v1_TORCH_II.csv
            pass
    elif datatype == "float":
        # check if it is negative and then all 9's
        if check_is_minus_9s(value):
            minus_9s_value = value
        try:
            numeric_value = float(value)
        except ValueError:
            pass
    else:
        # check if it is negative and then all 9's
        if check_is_minus_9s(value):
            minus_9s_value = value
        numeric_value = float(value)

    return string_value, minus_9s_value, numeric_value


def find_fill_values(results: dict, final_results: dict) -> dict:
    # Check for fill values in columns with numeric values

    columns = final_results.keys()

    fill_values = {}

    for col_name in columns:
        datatypes = results[col_name]["col_datatypes"]
        col_values = final_results[col_name]["col_values"]

        # Look for fill in "float" or "integer" columns
        # Looking for minus 9s fills or string fill
        if "float" in datatypes or "integer" in datatypes:
            string_values = []
            numeric_values = []
            minus_9s = []

            for i in range(len(col_values)):
                value = col_values[i]
                datatype = datatypes[i]

                (
                    string_value,
                    minus_9s_value,
                    numeric_value,
                ) = find_fill_and_numeric_values(value, datatype)

                if string_value is not None:
                    string_values.append(string_value)

                if minus_9s_value is not None:
                    minus_9s.append(minus_9s_value)

                if numeric_value is not None:
                    numeric_values.append(numeric_value)

            # TODO
            # check if there are negative numbers and if they are, that
            # -999 fill is larger than other negative numbers

            if len(minus_9s):
                # minus_9s fill takes priority over any string values
                fill_values[col_name] = check_possible_minus_9s_fill_value(
                    col_name, minus_9s, numeric_values
                )
            elif len(string_values):
                unique_string_vals = list(set(string_values))

                if len(unique_string_vals) == 1:
                    print(f"\n{col_name}")
                    print(f"Possible fill value is {unique_string_vals[0]}")

                    fill_values[col_name] = unique_string_vals[0]
                else:
                    fill_values[col_name] = None

            else:
                fill_values[col_name] = None

        else:
            fill_values[col_name] = None

    return fill_values


def fine_tune_formats(col_vals: list, unique_formats: list) -> list:
    # Check two digit positions to see if they are definitely a day value (>12)

    out_formats = unique_formats

    if (
        "%d/%m/%Y" in unique_formats
        and "%m/%d/%Y" in unique_formats
        and len(unique_formats) == 2
    ):
        col_pos_01 = []
        col_pos_34 = []

        for val in col_vals:
            try:
                col_pos_01.append(int(val[0:2]))
                col_pos_34.append(int(val[3:5]))
            except:
                pass

        # check if col_pos_01 > 12. Then it's definitely a day
        vals = [val for val in col_pos_01 if val > 12]
        if len(vals):
            is_day_col_pos_01 = True
        else:
            is_day_col_pos_01 = False

        # check if col_pos_34 > 12. Then it's definitely a day
        vals = [val for val in col_pos_34 if val > 12]
        if len(vals):
            is_day_col_pos_34 = True
        else:
            is_day_col_pos_34 = False

        if is_day_col_pos_01:
            out_format = "%d/%m/%Y"

        elif is_day_col_pos_34:
            out_format = "%m/%d/%Y"

        else:
            out_format = "%m/%d/%Y"

        out_formats = [out_format]

    if (
        "%d-%m-%Y" in unique_formats
        and "%m-%d-%Y" in unique_formats
        and len(unique_formats) == 2
    ):
        col_pos_01 = []
        col_pos_34 = []

        for val in col_vals:
            try:
                col_pos_01.append(int(val[0:2]))
                col_pos_34.append(int(val[3:5]))
            except:
                pass

        # check if col_pos_01 > 12. Then it's definitely a day
        vals = [val for val in col_pos_01 if val > 12]
        if len(vals):
            is_day_col_pos_01 = True
        else:
            is_day_col_pos_01 = False

        # check if col_pos_34 > 12. Then it's definitely a day
        vals = [val for val in col_pos_34 if val > 12]
        if len(vals):
            is_day_col_pos_34 = True
        else:
            is_day_col_pos_34 = False

        if is_day_col_pos_01:
            out_format = "%d/%m/%Y"

        elif is_day_col_pos_34:
            out_format = "%m/%d/%Y"

        else:
            out_format = "%m/%d/%Y"

        out_formats = [out_format]

    if (
        "%d%m%Y" in unique_formats
        and "%m%d%Y" in unique_formats
        and len(unique_formats) == 2
    ):
        col_pos_01 = []
        col_pos_23 = []

        for val in col_vals:
            try:
                col_pos_01.append(int(val[0:2]))
                col_pos_23.append(int(val[2:4]))
            except:
                pass

        # check if col_pos_01 > 12. Then it's definitely a day
        vals = [val for val in col_pos_01 if val > 12]
        if len(vals):
            is_day_col_pos_01 = True
        else:
            is_day_col_pos_01 = False

        # check if col_pos_23 > 12. Then it's definitely a day
        vals = [val for val in col_pos_23 if val > 12]
        if len(vals):
            is_day_col_pos_23 = True
        else:
            is_day_col_pos_23 = False

        if is_day_col_pos_01:
            out_format = "%d%m%Y"

        elif is_day_col_pos_23:
            out_format = "%m%d%Y"

        else:
            out_format = "%m%d%Y"

        out_formats = [out_format]

    if (
        "%Y%m%d" in unique_formats
        and "%m%d%Y" in unique_formats
        and len(unique_formats) == 2
    ):
        col_pos_03 = []
        col_pos_47 = []

        for val in col_vals:
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
            out_format = "%m%d%Y"

        out_formats = [out_format]

    return out_formats


def get_final_results(results: dict, parameter_official_names: dict) -> dict:
    final_results = {}

    # Refine datetime to be time, date, or datetime

    time_format_letters = ["H", "M", "S", "f"]

    alphabet = string.ascii_lowercase + string.ascii_uppercase
    date_format_letters = alphabet.translate({ord(letter): None for letter in "HMSfzZ"})

    date_format_letters = list(date_format_letters)

    column_names = list(results.keys())

    final_results = {}

    for col_name in column_names:
        final_results[col_name] = {}

        col_values = results[col_name]["col_values"]
        final_results[col_name]["col_values"] = col_values

        final_results[col_name]["fill_value"] = results[col_name]["fill_value"]

        datatypes = results[col_name]["col_datatypes"]

        # Fine tune whether a datetime type is date, time, or datetime
        # Need to know what the format looks like,
        # If it has H,M, or S in it, and no other letters, it's a time
        # If it has no H,M,S, it's a date

        bcodmo_datetime_parameters = get_bcodmo_datetime_parameters()

        name_in_bcodmo_datetimes = get_is_name_in_bcodmo_datetime_vars(
            col_name, parameter_official_names, bcodmo_datetime_parameters
        )

        parameter_formats = results[col_name]["col_formats"]

        parameter_formats = [item for sublist in parameter_formats for item in sublist]

        new_datatypes = []

        for i in range(len(datatypes)):
            elem_datatype = datatypes[i]
            elem_format = parameter_formats[i]

            if elem_format is not None and elem_datatype == "datetime":
                datatype_letters = re.split(r"[^a-zA-Z]*", elem_format)

                common_time_letters = list(
                    set(time_format_letters) & set(datatype_letters)
                )

                common_date_letters = list(
                    set(date_format_letters) & set(datatype_letters)
                )

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
                new_datatypes.append(None)

            else:
                new_datatypes.append(elem_datatype)

        unique_datatypes = list(set(new_datatypes))

        unique_datatypes = [elem for elem in unique_datatypes if elem]

        if len(unique_datatypes) > 1 and "isnan" in unique_datatypes:
            unique_datatypes.remove("isnan")

        # TODO
        # should I leave this in in case all values are a fill value?
        if len(unique_datatypes) > 1 and "isfill" in unique_datatypes:
            unique_datatypes.remove("isfill")

        if not len(unique_datatypes):
            unique_datatypes = [None]

        if (
            "float" in unique_datatypes
            and "integer" in unique_datatypes
            and "datetime" not in unique_datatypes
        ):
            unique_datatypes = ["float"]

        if "string" in unique_datatypes:
            unique_datatypes = ["string"]

        final_results[col_name]["unique_datatypes"] = unique_datatypes

        # Get unique parameter formats and fine-tune
        if len(parameter_formats):
            unique_formats = list(set(parameter_formats))
            unique_formats = [val for val in unique_formats if val]

            if not unique_formats:
                unique_formats = [None]
        else:
            unique_formats = [None]

        # If more than one format, see if can fine-tune to one best format
        unique_formats = fine_tune_formats(col_values, unique_formats)

        final_results[col_name]["unique_formats"] = unique_formats

    return final_results


def get_parameters_datatypes(col_vals: list, name_in_bcodmo_datetimes: bool) -> tuple:
    result = []

    fill_values = []

    possible_fill_values = get_possible_fill_values()

    for i in range(len(col_vals)):
        col_val = col_vals[i]

        if name_in_bcodmo_datetimes:
            is_datetime = True

        else:
            is_datetime = False

        if is_datetime:
            datatype = "datetime"

        else:
            if col_val in possible_fill_values:
                datatype = "isfill"
                fill_values.append(col_val)
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

        result.append(datatype)

    possible_fill_value = list(set(fill_values))
    if len(possible_fill_value) == 1:
        fill_value = possible_fill_value[0]
        print(f"fill value is {possible_fill_value}")
    else:
        fill_value = None

    return result, fill_value


def get_parameters_datetime_formats(
    col_vals: list, is_name_in_bcodmo_datetime_vars: bool
) -> list:
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

    return result


def process_file_df(df: pd.DataFrame, parameter_official_names: dict) -> dict:
    # For BCO-DMO datetime paramters, get format
    bcodmo_datetime_parameters = get_bcodmo_datetime_parameters()

    # formats = get_datetime_formats_to_match()

    column_names = df.columns

    # Testing
    testing = False

    if testing:
        # process 5 rows to infer datatypes and datetime formats

        df_new = df.iloc[0:5]

    else:
        df_new = df.copy()

    # df_datetime_formats = pd.DataFrame(dtype=str)
    # df_datatypes = pd.DataFrame(dtype=str)

    results = {}

    for col_name in column_names:
        results[col_name] = {}

        column = df_new[col_name].copy()

        is_name_in_bcodmo_datetime_vars = get_is_name_in_bcodmo_datetime_vars(
            col_name, parameter_official_names, bcodmo_datetime_parameters
        )

        col_vals = list(column.values)

        parameter_datetime_formats = get_parameters_datetime_formats(
            col_vals, is_name_in_bcodmo_datetime_vars
        )

        parameters_datatypes, fill_value = get_parameters_datatypes(
            col_vals, is_name_in_bcodmo_datetime_vars
        )

        results[col_name]["col_values"] = col_vals
        results[col_name]["col_datatypes"] = parameters_datatypes
        results[col_name]["col_formats"] = parameter_datetime_formats
        results[col_name]["fill_value"] = fill_value

    return results


# def get_data_types_formats(df: pd.DataFrame, parameter_official_names: dict) -> tuple:
#     """
#     Process a file by reading it in to determine the datatype of each parameter value.
#     And when a parameter is a datetime type, fine-tune it to be a time, date, or datetime
#     along with finding the parameter column's format.

#     Returns:
#         dict: all paramter values
#         dict: final_parameters_datatypes
#         dict: final_parameters_datetime_formats
#     """

#     results = process_file_df(df, parameter_official_names)

#     final_results = get_final_results(results, parameter_official_names)

#     return results, final_results


def get_parameter_official_names(csv_file: str, parameter_col_names: list) -> dict:
    dataset_id = get_dataset_id(csv_file)

    # Read in the parameters info file to get the corresponding official name
    # of supplied parameter names. Will use this later to determine which
    # parameters are datetimes and then get their formats.

    # Supplied parameter names from the parameter file mapped to BCO-DMO
    # official names which are stored in the parameters info file

    parameters_info_filename = get_parameters_info_filename(dataset_id)

    if parameters_info_filename is None:
        print("No parameters info file")

        # Supplied parameter names translated to BCO-DMO official names,
        # but there are none to available to map to
        parameter_official_names = {name: None for name in parameter_col_names}

    else:
        with open(parameters_info_filename, "r") as f:
            parameters_info = json.load(f)

        # parameters output looks like
        # "lat": {"type": "float"},
        # "time_local": {"type": "time", format: "HH:mm:ss"}

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


def get_file_df(filename: str) -> pd.DataFrame:
    """
    Read in file to a pandas dataframe and keep values as strings
    so that integers aren't coerced to float or string depending on a fill value
    And keep NaN text, so set keep_default_na to False

    Returns:
        _type_: pd.DataFrame
    """

    try:
        try:
            df = pd.read_csv(
                filename, encoding="utf-8", dtype=str, keep_default_na=False
            )
        except pd.errors.ParserError:
            df = pd.DataFrame()
    except UnicodeDecodeError:
        try:
            try:
                df = pd.read_csv(
                    filename, encoding="windows-1252", dtype=str, keep_default_na=False
                )
                with open(log_encodings_not_utf8, "a") as f:
                    f.write(f"{filename} encoding is windows-1252\n")

            except pd.errors.ParserError:
                df = pd.DataFrame()
        except UnicodeDecodeError:
            try:
                try:
                    df = pd.read_csv(
                        filename, encoding="latin1", dtype=str, keep_default_na=False
                    )

                    with open(log_encodings_not_utf8, "a") as f:
                        f.write(f"{filename} encoding is latin1\n")

                except pd.errors.ParserError:
                    df = pd.DataFrame()
            except UnicodeDecodeError:
                print("Error: File not opened as utf-8, windows-1252 or latin1")
                with open(log_errors_path, "a") as f:
                    f.write(f"{filename}\n")
                df = pd.DataFrame()

    return df


def get_parameters_datatypes_dateformats(csv_file: str) -> tuple:
    df = get_file_df(csv_file)

    column_names = list(df.columns)

    parameter_official_names = get_parameter_official_names(csv_file, column_names)

    if not df.empty:
        results = process_file_df(df, parameter_official_names)

    else:
        results = None

    if results:
        try:
            final_results = get_final_results(results, parameter_official_names)
        except:
            final_results = None
    else:
        final_results = None

    return results, final_results


def process_file(file: Path):
    csv_file = file.as_posix()

    print(f"\n******************\n")
    print(f"file being processed is {csv_file}\n")

    results, final_results = get_parameters_datatypes_dateformats(csv_file)

    if final_results:
        found_fill_values = find_fill_values(results, final_results)

        for col_name in final_results.keys():
            col_found_fill_value = found_fill_values[col_name]
            fill_value = final_results[col_name]["fill_value"]

            fill_values = [col_found_fill_value, fill_value]

            fill_values = [val for val in fill_values if val]

            print(f"Final fill value is {fill_values}")

            if len(fill_values) == 1:
                final_results[col_name]["fill_value"] = fill_values[0]
            else:
                final_results[col_name]["fill_value"] = None

        # # print(f"{col_name} z-value: {z_value[col_name]}")
        # print("z_values")
        # print(z_values)

        save_output(csv_file, final_results)

        parameters_names = final_results.keys()

        summary_obj = get_parameters_summary_obj(
            csv_file, parameters_names, final_results
        )

    else:
        summary_obj = None

    if summary_obj is None:
        print(f"Error with file, no results inferred {csv_file}")
    else:
        print(f"info obj is \n{summary_obj}")

    return


if __name__ == "__main__":
    files = Path(top_data_folder).glob("**/dataURL/*.csv")

    file_list = list(files)

    # unicodedecode_error = ['3758_v1_alongtrack.csv', '2472_v1_trawl_catch___GoA.csv', '2321_v1_eventlogs.csv']

    # #ParserError: Error tokenizing data. C error: EOF inside string starting at row 1
    # parse_error = ['2468_v1_w0008_seasoar_CTD.csv']

    # bad_files = [*unicodedecode_error, *parse_error]

    # if csv_file.name in bad_files:
    #     continue

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

    # Error with datetime designation
    # datatypes ['datetime', 'datetime', 'datetime', 'datetime', 'datetime'] for parameter bot
    # if csv_file.name != '3752_v2_DIC.csv':
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

    # if csv_file.name != '768627_v1_Global_observed_nitrate_d15N.csv':
    #     continue

    # file_list = [
    #     file
    #     for file in file_list
    #     if file.name == "2462_v1_ccs_proc_CTD.csv"
    # ]

    # Check z values to see if can find a fill value
    # Mabe see if all values are positive and have a -9, -99, -999 etc value
    # So if all positive and one negative value
    # Put a check in the program looking for values with a series of 9 in them
    # So split string, get unique set, look only at numeric part and see if value is 9

    # 3358_v1_Bottle_Data.csv
    # file_list = [file for file in file_list if file.name == "3358_v1_Bottle_Data.csv"]

    # 765327_v1_TORCH_II.csv not saved to output file. why?
    # file_list = [file for file in file_list if file.name == "765327_v1_TORCH_II.csv"]

    # 658275_v1_Mapping_information_and_signs_for_individual_diseased_corals
    # Why is possible fill "isfill"?
    # file_list = [
    #     file
    #     for file in file_list
    #     if file.name
    #     == "658275_v1_Mapping_information_and_signs_for_individual_diseased_corals.csv"
    # ]

    num_files = len(file_list)
    print(f"num files is {num_files}")

    log_output_path = Path(log_output)
    log_output_path.unlink(missing_ok=True)

    log_errors_path = Path(log_errors)
    log_errors_path.unlink(missing_ok=True)

    log_encodings_not_utf8_path = Path(log_encodings_not_utf8)
    log_encodings_not_utf8_path.unlink(missing_ok=True)

    num_cores = multiprocessing.cpu_count()

    print(f"number of cores is {num_cores}")

    start_time = time.time()

    PROCESSES = num_cores - 2
    with multiprocessing.Pool(PROCESSES) as pool:
        pool.map(process_file, file_list)

    end_time = time.time()

    print(f"program took {(end_time - start_time)/60} minutes")
