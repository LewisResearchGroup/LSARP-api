import pandas as pd
from datetime import date


def format_apl_data(df, years=None, datetime_numeric=False):
    df = df.copy()
    df["YEAR"] = df.COLLECT_DTM.dt.year
    df["NTH_YEAR"] = df.COLLECT_DTM.dt.year - df.ENCNTR_ADMIT_DTM.dt.year
    df["AGE_GRP"] = df["NAGE_YR"].apply(age_to_age_group)
    df["COLLECT_HOURS_AFTER_ADMIT"] = (df.COLLECT_DTM - df.ENCNTR_ADMIT_DTM).astype(
        "timedelta64[s]"
    ).astype(int) / 3600
    df["INTERP"] = df["INTERP"].fillna("N")
    df["FLAG_COLLECT_24h_BEFORE_ADMIT"] = df["COLLECT_HOURS_AFTER_ADMIT"] < 24
    df["HOSPITAL_ONSET_48H"] = df["COLLECT_HOURS_AFTER_ADMIT"] > 48
    df["HOSPITAL_ONSET_72H"] = df["COLLECT_HOURS_AFTER_ADMIT"] > 72  
    add_date_features_from_datetime_col(df, "COLLECT_DTM", numeric=datetime_numeric)
    if years is not None:
        df = df[df.YEAR.astype(int).isin(years)]
    df = fill_missing_times(df)
    df = df.replace('', None)
    return df


def fill_missing_times(df, fill_value=None):
    df = df.copy()
    if fill_value is None:
        fill_value = pd.to_datetime(date(1970, 1, 1))
    cols = df.dtypes[df.dtypes=='datetime64[ns]'].index.to_list()
    for col in cols:
        df[col] = df[col].fillna(fill_value)
    return df 


def today():
    return date.today().strftime("%y%m%d")


def log2p1(x):
    try:
        return np.log2(x + 1)
    except:
        return x

    
def age_to_age_group(x):
    x = int(x)
    if x < 0:
        return "Unknown"
    elif x < 10:
        return "00-09"
    elif x < 20:
        return "10-19"
    elif x < 30:
        return "20-29"
    elif x < 40:
        return "30-39"
    elif x < 50:
        return "40-49"
    elif x < 60:
        return "50-59"
    elif x < 70:
        return "60-69"
    elif x < 80:
        return "70-79"
    else:
        return "80+"


def add_date_features_from_datetime_col(
    df, date_col_name, numeric=False, add_prefix=False
):
    """
    Takes a column with dtype pandas.datetime and extracts time features.
    The year, quarter, month, week, date (without time) and combined features.

    Parameters
    ----------
    df - pandas.DataFrame
    date_col_name - str, column name of df
    Added columns
    -------------
    YEAR - int, year contained in date column
    QUARTER - int, year contained in date column
    MONTH - int, year contained in date column
    WEEK - int, year contained in date column
    DAY - int, year contained in date column
    DATE - datetime, date contained in date column
        without time
    YEAR_DAY - str, format: "%4d-D%03d"
    YEAR_WEEK - str, format: "%4d-W%02d"
    YEAR_MONTH - str, format: "%4d-M%02d"
    YEAR_QUATER - str, format: "%4d-Q%1d"
    """

    month_to_quadrimester = {i: ((i + 3) // 4) for i in range(1, 13)}

    assert date_col_name in df.columns, "%s not in df.columns" % (date_col_name)
    assert df[date_col_name].dtype
    
    prefix = f"{date_col_name}_" if add_prefix else ""
    
    df.loc[:, f"{prefix}YEAR"] = df[date_col_name].dt.year
    df.loc[:, f"{prefix}MONTH"] = df[date_col_name].dt.month
    df.loc[:, f"{prefix}WEEK"] = df[date_col_name].dt.isocalendar().week
    df.loc[:, f"{prefix}DAYOFWEEK"] = df[date_col_name].dt.dayofweek
    df.loc[:, f"{prefix}DAY"] = df[date_col_name].dt.day
    df.loc[:, f"{prefix}DAYOFYEAR"] = df[date_col_name].dt.dayofyear
    df.loc[:, f"{prefix}DATE"] = df[date_col_name].dt.date
    df.loc[:, f"{prefix}QUARTER"] = df[date_col_name].dt.quarter

    df.loc[:, f"{prefix}QUADRIMESTER"] = df.loc[:, f"{prefix}MONTH"].replace(
        month_to_quadrimester
    )

    if numeric is True:
        df.loc[:, f"{prefix}YEAR_DAY"] = df.YEAR + df[f"{prefix}DAYOFYEAR"].apply(
            lambda x: (x - 1) / 365
        )
        df.loc[:, f"{prefix}YEAR_WEEK"] = df.YEAR + df[f"{prefix}WEEK"].apply(
            lambda x: (x - 1) / 52
        )
        df.loc[:, f"{prefix}YEAR_MONTH"] = df.YEAR + df[f"{prefix}MONTH"].apply(
            lambda x: (x - 1) / 12
        )
        df.loc[:, f"{prefix}YEAR_QUARTER"] = df.YEAR + df[f"{prefix}QUARTER"].apply(
            lambda x: (x - 1) / 4
        )
        df.loc[:, f"{prefix}YEAR_QUADRIMESTER"] = df.YEAR + df[f"{prefix}QUADRIMESTER"].apply(
            lambda x: (x - 1) / 3
        )
    elif numeric is False:
        df.loc[:, f"{prefix}YEAR_DAY"] = (
            df.YEAR.astype(str) + "-" + df.DAYOFYEAR.map("{:03.0f}".format)
        )
        df.loc[:, f"{prefix}YEAR_WEEK"] = (
            df.YEAR.astype(str) + "-" + df.WEEK.map("{:02.0f}".format)
        )
        df.loc[:, f"{prefix}YEAR_MONTH"] = (
            df.YEAR.astype(str) + "-" + df.MONTH.map("{:02.0f}".format)
        )
        df.loc[:, f"{prefix}YEAR_QUARTER"] = (
            df.YEAR.astype(str) + "-" + df.QUARTER.astype(str)
        )
        df.loc[:, f"{prefix}YEAR_QUADRIMESTER"] = (
            df.YEAR.astype(str) + "-" + df.QUADRIMESTER.astype(str)
        )
    elif numeric == "mixed":
        df.loc[:, f"{prefix}YEAR_WEEK"] = df.YEAR + df[f"{prefix}WEEK"].apply(
            lambda x: (x - 1) / 52
        )
        df.loc[:, f"{prefix}YEAR_MONTH"] = df.YEAR + df[f"{prefix}MONTH"].apply(
            lambda x: (x - 1) / 12
        )
        df.loc[:, f"{prefix}YEAR_QUARTER"] = df.YEAR + df[f"{prefix}QUARTER"].apply(
            lambda x: (x - 1) / 4
        )
        df.loc[:, f"{prefix}YEAR_QUADRIMESTER"] = df.YEAR + df[f"{prefix}QUADRIMESTER"].apply(
            lambda x: (x - 1) / 3
        )


def sort_df_by_row_count(df, axis=1, ascending=True):
    ndx = df.sum(axis=axis).sort_values(ascending=ascending).index
    return df.reindex(ndx, axis=(axis + 1) % 2)


def crosstab_agegrp_gender_year(df):
    return (
        df.groupby(["YEAR", "AGE_GRP", "GENDER"])
        .count()["NAGE_YR"]
        .unstack("YEAR")
        .T.fillna(0)
        .astype(int)
    )
