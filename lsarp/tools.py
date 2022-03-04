def age_to_age_group(x):
    x = int(x)
    if x < 0:
        return "Unknown"
    elif x < 11:
        return "00-10"
    elif x < 21:
        return "11-20"
    elif x < 31:
        return "21-30"
    elif x < 41:
        return "31-40"
    elif x < 51:
        return "41-50"
    elif x < 61:
        return "51-60"
    elif x < 71:
        return "61-70"
    elif x < 81:
        return "71-80"
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

    month_to_trimester = {i: ((i + 3) // 4) for i in range(1, 13)}

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

    df.loc[:, f"{prefix}TRIMESTER"] = df.loc[:, f"{prefix}MONTH"].replace(
        month_to_trimester
    )

    if numeric:
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
        df.loc[:, f"{prefix}YEAR_TRIMESTER"] = df.YEAR + df[f"{prefix}TRIMESTER"].apply(
            lambda x: (x - 1) / 3
        )
    else:
        df.loc[:, f"{prefix}YEAR_DAY"] = (
            df.YEAR.astype(str) + "-" + df.DAYOFYEAR.astype(str)
        )
        df.loc[:, f"{prefix}YEAR_WEEK"] = (
            df.YEAR.astype(str) + "-" + df.WEEK.astype(str)
        )
        df.loc[:, f"{prefix}YEAR_MONTH"] = (
            df.YEAR.astype(str) + "-" + df.MONTH.astype(str)
        )
        df.loc[:, f"{prefix}YEAR_QUARTER"] = (
            df.YEAR.astype(str) + "-" + df.QUARTER.astype(str)
        )
        df.loc[:, f"{prefix}YEAR_TRIMESTER"] = (
            df.YEAR.astype(str) + "-" + df.TRIMESTER.astype(str)
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
