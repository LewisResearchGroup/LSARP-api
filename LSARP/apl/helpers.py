import numpy as np
import pandas as pd


def timedelta_to_hours(series):
    """
    Converts a pandas.Series containing pandas.timedelta
    to hours as type float.
        Timedelta('1 hour 30 minutes') -> 1.5
    """
    hours_from_days = series.dt.days * 24
    hours_from_seconds = series.dt.seconds / 3600
    return hours_from_days + hours_from_seconds


def replace_interp(x):
    if not isinstance(x, list):
        return x
    if "R" in x:
        return "R"
    elif "I" in x:
        return "I"
    elif "S" in x:
        return "S"
    else:
        return np.NaN


def get_element(x):
    if isinstance(x, list):
        x = list(set(x))
        try:
            x.remove(np.NaN)
        except:
            pass
        try:
            x.remove(None)
        except:
            pass
        if len(x) == 1:
            return x[0]
        return x
    else:
        return x


def key_func_SIRN(x):
    mapping = {"S": 0, "I": 1, "R": 2, "N": 3}
    return pd.Index(pd.Series(x).replace(mapping))
