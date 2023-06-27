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

from pathlib import Path
import re
import pandas as pd
import json
import math
from datetime import datetime
import time
import string
import multiprocessing

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


top_data_folder = f"../erddap_data_files_temp5"

possible_formats_file = "possible_datetime_formats.txt"
df_formats = pd.read_fwf(possible_formats_file)
datetime_formats_to_match = df_formats["datetime_formats"].tolist()


# List of possible nan types in BCO-DMO data files
def get_possible_nan_fills() -> list:
    possible_nan_fills = ["nd", "NaN"]

    return possible_nan_fills


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


def get_parameters_summary_obj_filename(dataset_id: str | None) -> str | None:
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
    file_out = "parameter_output.txt"

    # Get one line of parameter values to see sample values
    values = {}
    final_parameters_datetime_formats = {}
    final_parameters_datatypes = {}

    if final_results is not None:
        # for col_name, val in results.items():
        for col_name, val in final_results.items():
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

    else:
        final_parameters_datetime_formats = {}

    with open(file_out, "a") as f:
        f.write(f"**********************\n")
        f.write(f"file: {csv_file}\n")
        f.write(f"**********************\n")
        f.write(f"Sample values\n")
        f.write(json.dumps(values, indent=4))
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


def get_final_results(results: dict, parameter_official_names: dict) -> dict:
    final_results = {}

    time_format_letters = ["H", "M", "S", "f"]

    alphabet = string.ascii_lowercase + string.ascii_uppercase
    date_format_letters = alphabet.translate({ord(letter): None for letter in "HMSfzZ"})

    date_format_letters = list(date_format_letters)

    column_names = list(results.keys())

    final_results = {}

    for col_name in column_names:
        final_results[col_name] = {}

        final_results[col_name]["col_values"] = results[col_name]["col_values"]

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

        if len(unique_datatypes) == 1 and None in unique_datatypes:
            unique_datatypes = [None]

        elif len(unique_datatypes) > 1 and "isnan" in unique_datatypes:
            unique_datatypes.remove("isnan")

            if not unique_datatypes:
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

        if len(parameter_formats):
            unique_formats = list(set(parameter_formats))
            unique_formats = [val for val in unique_formats if val]

            if not unique_formats:
                unique_formats = [None]
        else:
            unique_formats = [None]

        final_results[col_name]["unique_formats"] = unique_formats

    return final_results


def get_parameters_datatypes(
    col_vals: list, name_in_bcodmo_datetimes: bool, possible_nan_fills: list
) -> list:
    result = []

    for i in range(len(col_vals)):
        col_val = col_vals[i]

        if name_in_bcodmo_datetimes:
            is_datetime = True

            # TODO
            # refine into type: date, time, datetime depending on what letters appear in format

        else:
            is_datetime = False

        if is_datetime:
            datatype = "datetime"

        else:
            if col_val in possible_nan_fills:
                datatype = "isnan"
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

    return result


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

    possible_nan_fills = get_possible_nan_fills()

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

        parameters_datatypes = get_parameters_datatypes(
            col_vals, is_name_in_bcodmo_datetime_vars, possible_nan_fills
        )

        results[col_name]["col_values"] = col_vals
        results[col_name]["col_datatypes"] = parameters_datatypes
        results[col_name]["col_formats"] = parameter_datetime_formats

    return results


def get_data_types_formats(df: pd.DataFrame, parameter_official_names: dict) -> dict:
    """
    Process a file by reading it in to determine the datatype of each parameter value.
    And when a parameter is a datetime type, fine-tune it to be a time, date, or datetime
    along with finding the parameter column's format.

    Returns:
        dict: all paramter values
        dict: final_parameters_datatypes
        dict: final_parameters_datetime_formats
    """

    results = process_file_df(df, parameter_official_names)

    final_results = get_final_results(results, parameter_official_names)

    return final_results


def get_parameter_official_names(csv_file: str, parameter_col_names: list) -> dict:
    dataset_id = get_dataset_id(csv_file)

    # Read in the parameters info file to get the corresponding official name
    # of supplied parameter names. Will use this later to determine which
    # parameters are datetimes and then get their formats.

    # Supplied parameter names from the parameter file mapped to BCO-DMO
    # official names which are stored in the parameters info file

    parameters_info_filename = get_parameters_summary_obj_filename(dataset_id)

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
            except pd.errors.ParserError:
                df = pd.DataFrame()
        except UnicodeDecodeError:
            try:
                try:
                    df = pd.read_csv(
                        filename, encoding="latin1", dtype=str, keep_default_na=False
                    )
                except pd.errors.ParserError:
                    df = pd.DataFrame()
            except UnicodeDecodeError:
                print("Error: File not opened as utf-8, windows-1252 or latin1")
                df = pd.DataFrame()

    return df


def get_parameters_datatypes_dateformats(csv_file: str) -> tuple:
    df = get_file_df(csv_file)

    column_names = list(df.columns)

    if not df.empty:
        parameter_official_names = get_parameter_official_names(csv_file, column_names)

        final_results = get_data_types_formats(df, parameter_official_names)

    else:
        final_results = None

    return final_results, column_names


def process_file(file: Path):
    csv_file = file.as_posix()

    print(f"\n******************\n")
    print(f"file being processed is {csv_file}\n")

    final_results, parameters_names = get_parameters_datatypes_dateformats(csv_file)

    if final_results:
        save_output(csv_file, final_results)

        summary_obj = get_parameters_summary_obj(
            csv_file, parameters_names, final_results
        )

    else:
        summary_obj = None

    if summary_obj is None:
        print(f"Error with file {csv_file}")
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

    # number of column names doesn't match parameter columns
    # if csv_file.name != '2408_v1_mooring_ef.csv':
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

    file_list = [
        file for file in file_list if file.name == "2796_v1_inshore_trawl_stations.csv"
    ]

    num_files = len(file_list)
    print(f"num files is {num_files}")

    file_out = "parameter_output.txt"

    file_out_path = Path(file_out)

    file_out_path.unlink(missing_ok=True)

    num_cores = multiprocessing.cpu_count()

    start_time = time.time()

    PROCESSES = num_cores - 2
    with multiprocessing.Pool(PROCESSES) as pool:
        pool.map(process_file, file_list)

    end_time = time.time()

    print(f"program took {(end_time - start_time)/60} minutes")
