import math
import numpy as np


# List of possible fill types in BCO-DMO data files
# I've treated any NaN values as np.NaN to ignore in determining the
# datatype of a column
def get_possible_fill_values() -> list:
    """_summary_

    Returns:
        _type_: _description_
    """

    # TODO
    # Find out if "bd" counts as a fill value (means below detection)
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
    ]

    return possible_fill_values


def check_datetime_possible_minus_9s_fill_value(minus_9s: list) -> int | float | None:
    """_summary_

    Returns:
        _type_: _description_
    """

    fill_value = None

    found_9s_fill = list(set(minus_9s))

    # Don't set as a fill value if there are multiple versions of a minus_9s fill
    if len(found_9s_fill) == 1:
        fill_value = found_9s_fill[0]
    else:
        fill_value = None

    return fill_value


def check_possible_minus_9s_fill_value(
    col_name: str, minus_9s: list, numeric_values: list
) -> int | float | None:
    """_summary_

    Returns:
        _type_: _description_
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

        if len(negative_numeric_values):
            # Until check for larger value than possible for negative values,
            # don't include a minus_9s fill for negative numeric values
            print(
                f"Fill value found in negative list of {col_name} col values other than fill"
            )
            fill_value = None
        else:
            fill_value = found_fill

    else:
        fill_value = None

    return fill_value


def check_is_minus_9s(value: str):
    """_summary_

    Returns:
        _type_: _description_
    """

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


def find_datetime_fill_values(col_value: str, datatype: str) -> tuple:
    """_summary_

    Returns:
        _type_: _description_
    """

    value = col_value.strip()

    possible_fill_value = None
    string_value = None
    minus_9s_value = None

    if datatype == "isfill":
        # find which fill value it is
        possible_fill_values = get_possible_fill_values()
        possible_fill_value = set(col_value) & set(possible_fill_values)

        if not possible_fill_value:
            possible_fill_value = None

    elif datatype == "string":
        string_value = value

    elif datatype == "float" or datatype == "integer":
        # check if it is negative and then all 9's
        if check_is_minus_9s(value):
            minus_9s_value = value

    return possible_fill_value, string_value, minus_9s_value


def get_datetime_fill_values(col_values: list, datatypes: list) -> tuple:
    """_summary_

    Returns:
        _type_: _description_
    """

    possible_fill_values = []
    string_values = []
    minus_9s = []

    for i in range(len(col_values)):
        value = col_values[i]
        datatype = datatypes[i]

        possible_fill_value, string_value, minus_9s_value = find_datetime_fill_values(
            value, datatype
        )

        if possible_fill_value is not None:
            possible_fill_values.append(possible_fill_value)

        if string_value is not None:
            string_values.append(string_value)

        if minus_9s_value is not None:
            minus_9s.append(minus_9s_value)

    if len(minus_9s) and not len(string_values) and not len(possible_fill_values):
        fill_values = check_datetime_possible_minus_9s_fill_value(minus_9s)

        alt_fill_values = None
    elif len(string_values) and not len(minus_9s) and not len(possible_fill_values):
        fill_values = None

        unique_string_vals = list(set(string_values))

        if len(unique_string_vals) == 1:
            alt_fill_values = unique_string_vals[0]
        else:
            alt_fill_values = None

    elif len(possible_fill_values) and not len(minus_9s) and not len(string_values):
        fill_values = possible_fill_values
        alt_fill_values = None
    else:
        fill_values = None
        alt_fill_values = None

    return fill_values, alt_fill_values


def find_fill_and_numeric_values(col_value: str, datatype: str) -> tuple:
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
        # find which fill value it is
        possible_fill_values = get_possible_fill_values()
        possible_fill_value = set(col_value) & set(possible_fill_values)
        if not possible_fill_value:
            possible_fill_value = None
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

    return possible_fill_value, string_value, minus_9s_value, numeric_value


def get_numeric_fill_values(col_name: str, col_values: list, datatypes: list) -> tuple:
    """_summary_

    Returns:
        _type_: _description_
    """

    possible_fill_values = []
    string_values = []
    numeric_values = []
    minus_9s = []

    for i in range(len(col_values)):
        value = col_values[i]
        datatype = datatypes[i]

        (
            possible_fill_value,
            string_value,
            minus_9s_value,
            numeric_value,
        ) = find_fill_and_numeric_values(value, datatype)

        if possible_fill_value is not None:
            possible_fill_values.append(possible_fill_value)

        if string_value is not None:
            string_values.append(string_value)

        if minus_9s_value is not None:
            minus_9s.append(minus_9s_value)

        if numeric_value is not None:
            numeric_values.append(numeric_value)

    # TODO
    # check if there are negative numbers and if they are, that
    # -999 fill is larger than other negative numbers

    if len(possible_fill_values) and not len(minus_9s) and not len(string_values):
        fill_values = possible_fill_values
        alt_fill_values = None
    elif len(minus_9s) and not len(possible_fill_values) and not len(string_values):
        fill_values = check_possible_minus_9s_fill_value(
            col_name, minus_9s, numeric_values
        )
        alt_fill_values = None
    elif len(string_values) and not len(minus_9s) and not len(possible_fill_values):
        fill_values = None

        unique_string_vals = list(set(string_values))

        if len(unique_string_vals) == 1:
            alt_fill_values = unique_string_vals[0]
        else:
            alt_fill_values = None

    else:
        fill_values = None
        alt_fill_values = None

    return fill_values, alt_fill_values


def find_params_fill_values(results: dict, final_results: dict) -> tuple:
    """_summary_

    Returns:
        _type_: _description_
    """

    # Check for fill values in columns with numeric values

    # Return fill value if there is only one possibility
    # because if multiple possibilities, probably not a fill value

    # TODO
    # If there is a one string in a numeric column, it's a fill value
    #
    # But should write these to a log file in case the column is
    # truly a string column even though it has integer values like
    # a station id column. See 813140_v1_Dissolved_oxygen.csv St_ID
    # so save it to a log file as a way of locating new possible fill values

    # TODO
    # But could be a case of not entering same fill value such
    # as -999.0 and -999. So check for this

    columns = final_results.keys()

    fill_values = {}
    alt_fill_values = {}

    for col_name in columns:
        datatypes = results[col_name]["col_datatypes"]
        col_values = final_results[col_name]["col_values"]

        # Look for fill in "float" or "integer" columns
        # Looking for minus 9s fills or string fill
        is_datetime = (
            "date" in datatypes or "time" in datatypes or "datetime" in datatypes
        )
        is_numeric = "float" in datatypes or "integer" in datatypes and not is_datetime

        if is_numeric:
            numeric_fill_values, numeric_alt_fill_values = get_numeric_fill_values(
                col_name, col_values, datatypes
            )

            fill_values[col_name] = numeric_fill_values
            alt_fill_values[col_name] = numeric_alt_fill_values

        elif is_datetime:
            # Check if a possible fill value in it or an alternate fill value

            datetime_fill_values, datetime_alt_fill_values = get_datetime_fill_values(
                col_values, datatypes
            )

            fill_values[col_name] = datetime_fill_values
            alt_fill_values[col_name] = datetime_alt_fill_values

        else:
            fill_values[col_name] = None
            alt_fill_values[col_name] = None

    return fill_values, alt_fill_values
