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

def add_date_features_from_datetime_col(df, date_col_name):
    '''
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
    '''
    assert date_col_name in df.columns, '%s not in df.columns' %(date_col_name)
    assert df[date_col_name].dtype
    df.loc[:, 'YEAR'] = df[date_col_name].dt.year
    df.loc[:, 'MONTH'] = df[date_col_name].dt.month
    df.loc[:, 'WEEK'] = df[date_col_name].dt.isocalender().week
    df.loc[:, 'DAY'] = df[date_col_name].dt.day
    df.loc[:, 'DAYOFYEAR'] = df[date_col_name].dt.dayofyear    
    df.loc[:, 'DATE'] = df[date_col_name].dt.date
    df.loc[:, 'QUARTER'] = df[date_col_name].dt.quarter
    df.loc[:, 'YEAR_DAY'] = df.YEAR +\
        df.DAYOFYEAR.apply(lambda x: (x-1)/365)
    df.loc[:, 'YEAR_WEEK'] = df.YEAR +\
        df.WEEK.apply(lambda x: (x-1)/52)
    df.loc[:, 'YEAR_MONTH'] = df.YEAR +\
        df.MONTH.apply(lambda x: (x-1)/12)
    df.loc[:, 'YEAR_QUARTER'] = df.YEAR +\
        df.QUARTER.apply(lambda x: (x-1)/4)

def sort_df_by_row_count(df, axis=1):
    ndx = df.sum(axis=axis).sort_values(ascending=False).index
    return df.reindex(ndx, axis=axis%1)