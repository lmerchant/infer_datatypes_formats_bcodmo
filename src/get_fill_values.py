import math
import numpy as np

# TODO
# search for fill values that are postitive 9s fill in a negative numeric
# column or datetime column


log_fill_w_neg_param_values_file = "../logs/log_fill_w_neg_param_value.txt"


def get_possible_fill_values() -> list:
    """
    List of possible fill types in BCO-DMO data files

    Returns:
        list: possible_fill_values
    """

    # TODO
    # Find out if "bd" counts as a fill value (means below detection)

    # Added empty string as a possible fill value in a numeric column
    # look at dataset_893293.csv
    # Says Silicate is a string but that's because it encountered a blank value
    # Need code to skip over blank values when determining a col datatype
    possible_fill_values = [
        "NaN",
        "nan",
        "nd",
        "ND",
        "n.d.",
        "n.a.",
        "N/A",
        "NA",
        "na",
        "n/a",
        "",
    ]

    return possible_fill_values


def check_datetime_minus_9s_fill_value(
    col_name: str, minus_9s: list
) -> int | float | None:
    """
    Check if a fill of minus 9s sequence is acceptable. It's
    acceptable if there is only one possiblity in a column
    and that the column is a datetime

    Returns:
        str | None: fill_value
    """

    found_9s_fill = list(set(minus_9s))
    print(f"{col_name} possible minus 9s fill {found_9s_fill}")

    # Don't set as a fill value if there are multiple versions of a minus_9s fill
    if len(found_9s_fill) == 1:
        found_fill = found_9s_fill[0]
    else:
        found_fill = None

    return found_fill


def check_numeric_minus_9s_fill_value(
    csv_file: str, col_name: str, minus_9s: list, numeric_values: list
) -> int | float | None:
    """
    Check if a fill of minus 9s sequence is acceptable. It's
    acceptable if there is only one possiblity in a column
    and that the numeric values are all positive.

    Returns:
        str | None: fill_value
    """

    fill_value = None

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

        # If there are negative numbers in a column besides the fill value, don't save a minus 9s fill
        if len(negative_numeric_values):
            fill_value = None

            # TODO
            # Save to a log file that a fill value was found
            # in the csv_file
            # but there were negative values besides the fill value
            with open(log_fill_w_neg_param_values_file, "a") as f:
                f.write(
                    f"file: {csv_file} param {col_name} has minus 9s fills {found_9s_fill} with neg param values\n"
                )

        else:
            fill_value = found_fill

    else:
        fill_value = None

    return fill_value


def check_is_minus_9s(value: str):
    """
    Split a column string value into pieces to check if
    all characters are -, 9, ., or 0 and then check if it
    is variation of -999, etc or float -999.0 or a
    value with more 9s than 3.

    If the characters satisfy this condition and is at least -999, it's a minus 9s number and a possible fill value.

    Returns:
        bool: is_minus_9s
    """

    # split string value into a list of chars
    pieces = list(value)
    unique_pieces = list(set(pieces))

    if len(unique_pieces) == 2:
        # Find values with only a '-' and '9's
        # finds minus integer values of -999 or larger minus 9s value
        is_minus_9s = "-" in unique_pieces and "9" in unique_pieces and len(value) >= 3

    elif len(unique_pieces) == 4:
        # Find values with only a '-', '.', '9's, and '0's
        # Finds minus float values of -999.0 or larger minus 9s value
        has_decimal_9s = (
            "-" in unique_pieces
            and "9" in unique_pieces
            and "." in unique_pieces
            and "0" in unique_pieces
        )

        if has_decimal_9s:
            numeric_pieces = value.split(".")

            if len(numeric_pieces) == 2:
                integer_portion = numeric_pieces[0]
                decimal_portion = numeric_pieces[1]

                # Make sure integer portion is at least -999
                if decimal_portion == "0" and len(integer_portion) >= 4:
                    is_minus_9s = True

                else:
                    is_minus_9s = False

            else:
                is_minus_9s = False

        else:
            is_minus_9s = False

    else:
        is_minus_9s = False

    return is_minus_9s


def find_datetime_cell_fill_values(
    col_value: str,
) -> tuple:
    """
    Check whether a column value is a fill value. A fill value can
    be one of the possible fill values defined in the function
    get_possible_fill_values, a minus 9s value, or a string which will
    be determined later if it is unique in a datetime column.

    Returns:
        str | None: possible_fill_value
        str | None: string_value
        str | None: minus_9s_value
    """

    value = col_value.strip()

    found_possible_fill_value = None
    minus_9s_value = None
    is_possible_fill_value = None

    possible_fill_values = get_possible_fill_values()

    if col_value in possible_fill_values:
        is_possible_fill_value = True

    if is_possible_fill_value:
        # find which fill value it is
        possible_fill_values = get_possible_fill_values()
        found_possible_fill_value = list(set([col_value]) & set(possible_fill_values))

        if not found_possible_fill_value:
            found_possible_fill_value = None
        elif len(found_possible_fill_value) == 1:
            found_possible_fill_value = found_possible_fill_value[0]

    else:
        # check if it is negative and then all 9's
        if check_is_minus_9s(value):
            minus_9s_value = value

    return found_possible_fill_value, minus_9s_value


def find_fill_and_numeric_value(col_value: str, datatype: str) -> tuple:
    """_summary_

    Returns:
        _type_: _description_
    """

    value = col_value.strip()

    possible_fill_value = None
    string_value = None
    minus_9s_value = None
    numeric_value = None

    if datatype == "isfill":
        # find which possible fill value it is
        possible_fill_values = get_possible_fill_values()
        possible_fill_value = list(set([col_value]) & set(possible_fill_values))

        if not possible_fill_value:
            possible_fill_value = None
        elif len(possible_fill_value) == 1:
            possible_fill_value = possible_fill_value[0]

    elif datatype == "string":
        string_value = value
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

    return possible_fill_value, string_value, minus_9s_value, numeric_value


def find_non_datetime_fill_values(
    value: str,
    datatype: str,
    string_values: list,
    numeric_values: list,
    fills_obj: dict,
) -> tuple:
    # If the column is not a datetime column, gather
    # numeric values in a column to check if they are
    # all positive or not since checking a minus 9s fill
    # only for columns with all positive values. For a
    # mixed value column or negative column, a minus 9s fill
    # value may actually be a data point. Also
    # look at strings to find alternate string fill values
    (
        possible_fill_value,
        string_value,
        minus_9s_value,
        numeric_value,
    ) = find_fill_and_numeric_value(value, datatype)

    found_possible_fill_values = fills_obj["found_possible_fill_values"]
    all_fill_values = fills_obj["all_fill_values"]
    all_possible_and_minus9s_fills = fills_obj["all_possible_and_minus9s_fills"]
    minus_9s = fills_obj["minus_9s"]

    if possible_fill_value is not None:
        found_possible_fill_values.append(possible_fill_value)
        all_fill_values.append(possible_fill_value)
        all_possible_and_minus9s_fills.append(possible_fill_value)

    if string_value is not None:
        string_values.append(string_value)
        all_fill_values.append(string_value)
        all_possible_and_minus9s_fills.append(None)

    if minus_9s_value is not None:
        minus_9s.append(minus_9s_value)
        all_fill_values.append(minus_9s_value)
        all_possible_and_minus9s_fills.append(minus_9s_value)

    if numeric_value is not None:
        numeric_values.append(numeric_value)

    if possible_fill_value is None and string_value is None and minus_9s_value is None:
        all_fill_values.append(None)
        all_possible_and_minus9s_fills.append(None)

    fills_obj["found_possible_fill_values"] = found_possible_fill_values
    fills_obj["all_fill_values"] = all_fill_values
    fills_obj["all_possible_and_minus9s_fills"] = all_possible_and_minus9s_fills
    fills_obj["minus_9s"] = minus_9s

    return string_values, numeric_values, fills_obj


def find_datetime_fill_values(value: str, fills_obj: dict) -> dict:
    # datatype = "datetime" was determined for whole column
    # by paramter official name. Here find fills in a
    # datetime column. Later will use detection of a
    # datetime format if a column has datetime values
    # besides a fill value or if it is numeric or a string.

    # if the column is a datetime datatype due to
    # it's official name, look for fill values
    # that are one of the defined possible fill values
    # and any minus9s fills.
    (
        possible_fill_value,
        minus_9s_value,
    ) = find_datetime_cell_fill_values(value)

    found_possible_fill_values = fills_obj["found_possible_fill_values"]
    all_fill_values = fills_obj["all_fill_values"]
    all_possible_and_minus9s_fills = fills_obj["all_possible_and_minus9s_fills"]
    minus_9s = fills_obj["minus_9s"]

    if possible_fill_value is not None:
        found_possible_fill_values.append(possible_fill_value)
        all_fill_values.append(possible_fill_value)
        all_possible_and_minus9s_fills.append(possible_fill_value)

    if minus_9s_value is not None:
        minus_9s.append(minus_9s_value)
        all_fill_values.append(minus_9s_value)
        all_possible_and_minus9s_fills.append(minus_9s_value)

    if possible_fill_value is None and minus_9s_value is None:
        all_fill_values.append(None)
        all_possible_and_minus9s_fills.append(None)

    fills_obj["found_possible_fill_values"] = found_possible_fill_values
    fills_obj["all_fill_values"] = all_fill_values
    fills_obj["all_possible_and_minus9s_fills"] = all_possible_and_minus9s_fills
    fills_obj["minus_9s"] = minus_9s

    return fills_obj


# def get_numeric_datetime_fill_values(
#     col_name: str, col_values: list, datatypes: list, is_datetime: bool
# ) -> dict:
#     """
#     Find fill value as one of a defined list of possible fill values
#     or a minus 9s fill value. Find alternate fill value as a
#     unique string in a numeric column.

#     Returns:
#         str | None: fill_value
#         str | None: alt_fill_value
#     """

#     string_values = []
#     numeric_values = []

#     fills_obj = {}

#     fills_obj["all_fill_values"] = []
#     fills_obj["all_possible_and_minus9s_fills"] = []
#     fills_obj["minus_9s"] = []
#     fills_obj["found_possible_fill_values"] = []

#     # Get fill values at each column value
#     for i in range(len(col_values)):
#         value = col_values[i]
#         datatype = datatypes[i]

#         if datatype is None or datatype == "isnan":
#             fills_obj["all_fill_values"].append(None)
#             fills_obj["all_possible_and_minus9s_fills"].append(None)

#         elif datatype == "datetime":
#             fills_obj = find_datetime_cell_fill_values(value, fills_obj)

#         else:
#             (
#                 string_values,
#                 numeric_values,
#                 fills_obj,
#             ) = find_non_datetime_fill_values(
#                 value, datatype, string_values, numeric_values, fills_obj
#             )

#     found_possible_fill_values = fills_obj["found_possible_fill_values"]
#     minus_9s = fills_obj["minus_9s"]

#     # TODO
#     # Can there be a large minus 9s value in a column with
#     # negative values?
#     # check if there are negative numbers and if they are, that
#     # -999 fill is larger than other negative numbers
#     if is_datetime:
#         if len(found_possible_fill_values) and not len(minus_9s):
#             unique_possible_fill_vals = list(set(found_possible_fill_values))

#             if len(unique_possible_fill_vals) == 1:
#                 fill_value = found_possible_fill_values[0]
#             else:
#                 fill_value = None

#             alt_fill_value = None
#         elif len(minus_9s) and not len(found_possible_fill_values):
#             fill_value = check_datetime_minus_9s_fill_value(col_name, minus_9s)
#             alt_fill_value = None

#         else:
#             fill_value = None
#             alt_fill_value = None

#     else:
#         if (
#             len(found_possible_fill_values)
#             and not len(minus_9s)
#             and not len(string_values)
#         ):
#             unique_possible_fill_vals = list(set(found_possible_fill_values))

#             if len(unique_possible_fill_vals) == 1:
#                 fill_value = found_possible_fill_values[0]
#             else:
#                 fill_value = None

#             alt_fill_value = None
#         elif (
#             len(minus_9s)
#             and not len(found_possible_fill_values)
#             and not len(string_values)
#         ):
#             fill_value = check_numeric_minus_9s_fill_value(
#                 col_name, minus_9s, numeric_values
#             )
#             alt_fill_value = None
#         elif (
#             len(string_values)
#             and not len(minus_9s)
#             and not len(found_possible_fill_values)
#         ):
#             fill_value = None

#             unique_string_vals = list(set(string_values))

#             if len(unique_string_vals) == 1:
#                 alt_fill_value = unique_string_vals[0]
#             else:
#                 alt_fill_value = None

#         else:
#             fill_value = None
#             alt_fill_value = None

#     fills_obj["found_possible_fill_values"] = found_possible_fill_values
#     fills_obj["minus_9s"] = minus_9s
#     fills_obj["fill_value"] = fill_value
#     fills_obj["alt_fill_val"] = alt_fill_value

#     return fills_obj


def get_final_fill_values(csv_file: str, col_name: str, results: dict) -> dict:
    is_datetime = results[col_name]["is_datetime"]
    numeric_values = results[col_name]["numeric_values"]
    string_values = results[col_name]["string_values"]

    fills_obj = results[col_name]["fills_obj"]
    found_possible_fill_values = fills_obj["found_possible_fill_values"]
    minus_9s = fills_obj["minus_9s"]

    # TODO
    # Can there be a large minus 9s value in a column with
    # negative values?
    # check if there are negative numbers and if they are, that
    # -999 fill is larger than other negative numbers
    if is_datetime:
        if len(found_possible_fill_values) and not len(minus_9s):
            unique_possible_fill_vals = list(set(found_possible_fill_values))

            if len(unique_possible_fill_vals) == 1:
                fill_value = found_possible_fill_values[0]
            else:
                fill_value = None

            alt_fill_value = None
        elif len(minus_9s) and not len(found_possible_fill_values):
            fill_value = check_datetime_minus_9s_fill_value(col_name, minus_9s)
            alt_fill_value = None

        else:
            fill_value = None
            alt_fill_value = None

    else:
        if (
            len(found_possible_fill_values)
            and not len(minus_9s)
            and not len(string_values)
        ):
            unique_possible_fill_vals = list(set(found_possible_fill_values))

            if len(unique_possible_fill_vals) == 1:
                fill_value = found_possible_fill_values[0]
            else:
                fill_value = None

            alt_fill_value = None
        elif (
            len(minus_9s)
            and not len(found_possible_fill_values)
            and not len(string_values)
        ):
            fill_value = check_numeric_minus_9s_fill_value(
                csv_file, col_name, minus_9s, numeric_values
            )
            alt_fill_value = None
        elif (
            len(string_values)
            and not len(minus_9s)
            and not len(found_possible_fill_values)
        ):
            fill_value = None

            unique_string_vals = list(set(string_values))

            if len(unique_string_vals) == 1:
                alt_fill_value = unique_string_vals[0]
            else:
                alt_fill_value = None
        elif (
            len(string_values) and len(minus_9s) and not len(found_possible_fill_values)
        ):
            fill_value = None
            alt_fill_value = None
        elif (
            len(string_values) and not len(minus_9s) and len(found_possible_fill_values)
        ):
            fill_value = None
            alt_fill_value = None
        else:
            fill_value = None
            alt_fill_value = None

    fills_obj["fill_value"] = fill_value
    fills_obj["alt_fill_val"] = alt_fill_value

    return fills_obj


# def find_param_fill_values(col_name: str, results: dict) -> dict:
#     """

#     Check for fill values in columns with numeric values

#      Return fill value if there is only one possibility
#      because if multiple possibilities, probably not a fill value

#      TODO
#      If there is a one string in a numeric column, it's a fill value

#      But should write these to a log file in case the column is
#      truly a string column even though it has integer values like
#      a station id column. See 813140_v1_Dissolved_oxygen.csv St_ID
#      so save it to a log file as a way of locating new possible fill values

#      TODO
#      But could be a case of not entering same fill value such
#      as -999.0 and -999. So check for this

#      Returns:
#          dict: results
#     """

#     datatypes = results[col_name]["col_datatypes"]
#     col_values = results[col_name]["col_values"]
#     is_datetime = results[col_name]["is_datetime"]

#     # Get fill values in a datetime or numeric column
#     fills_obj = get_numeric_datetime_fill_values(
#         col_name, col_values, datatypes, is_datetime
#     )

#     # if col_name == "o2":
#     #     print(fills_obj)

#     # all fill values that include string values in a numeric
#     # column and possible defined fills and minus 9s fills
#     # in datetime and numeric columns
#     results[col_name]["col_all_fill_values"] = fills_obj["all_fill_values"]

#     results[col_name]["col_all_possible_and_minus9s_fills"] = fills_obj[
#         "all_possible_and_minus9s_fills"
#     ]

#     # A fill value that is either one of the defined
#     # possible fill values  or a minus 9s value
#     results[col_name]["col_fill_value"] = fills_obj["fill_value"]

#     # A string that is unique to a numeric column but is not
#     # a defined possible fill value
#     results[col_name]["col_alt_fill_value"] = fills_obj["alt_fill_val"]

#     return results


# def find_params_fill_values(results: dict) -> dict:
#     columns = results.keys()

#     for col_name in columns:
#         results = find_params_cell_fill_values(col_name, results)

#     return results


# def determine_fill_values(results: dict) -> dict:
#     columns = results.keys()

#     for col_name in columns:
#         results = find_param_fill_values(col_name, results)

#     return results
