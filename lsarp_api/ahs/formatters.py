import pandas as pd



def format_accs(fn):
    df = pd.read_csv(
        fn,
        low_memory=False,
        dtype={
            "MISPRIME": str,
            "VISDATE": str, 
            "DISDATE": str, 
            "DISTIME": str,
            'DOCSVC1': str,
            'DOCSVC2': str,
            'DOCSVC3': str,
            'DOCSVC4': str,
            'DOCSVC5': str,
            'PROVTYPE1': str,
            'PROVTYPE2': str,
            'PROVTYPE3': str,
            'PROVTYPE4': str,
            'PROVTYPE5': str,
            'DXCODE1': str,
            'DXCODE2': str,
            'DXCODE3': str,
            'DXCODE4': str,
            'DXCODE5': str,
            'DXCODE6': str,
            'DXCODE7': str,
            'DXCODE8': str,
            'DXCODE9': str,
            'DXCODE10': str,
            'PROCCODE1': str,
            'PROCCODE2': str,
            'PROCCODE3': str,
            'PROCCODE4': str,
            'PROCCODE5': str,
            'PROCCODE6': str,
            'PROCCODE7': str,
            'PROCCODE8': str,
            'PROCCODE9': str,
            'PROCCODE10': str,
        },
        na_values=[""],
    ).rename(columns={
        'VISDATE': 'VISIT_DATE', 
        'DISDATE': 'DISP_DATE', 
        'DISTIME': 'DISP_TIME', 
        "SEX": "GENDER", 
        "ISOLATE_NBR": "BI_NBR",
        "Post_code": "POSTCODE",
        "DISP": "DISPOSITION",
        "AHS_ZONE": 'INST_ZONE',
        "LOS_MINUTES": "VISIT_LOS_MINUTES",
        "MISPRIME": "MIS_CODE",
        }
    )
    df['MIS_CODE'] = df['MIS_CODE'].str.pad(9, side='right', fillchar='0')
    df["VISIT_DATE"] = pd.to_datetime(df["VISIT_DATE"], format="%Y%m%d", errors="coerce")
    df["DISP_DATE"] = pd.to_datetime(df["DISP_DATE"], format="%Y%m%d", errors="coerce")
    df["DISP_TIME"] = pd.to_datetime(
        df["DISP_TIME"], format="%H%M", errors="coerce"
    ).dt.time

    drop_columns = df.filter(
        regex="DXCODE|PROCCODE|PROVIDER_SVC|PROVTYPE|DOCSVC"
    ).columns.to_list()
    df["DXCODES"] = df.filter(regex="DXCODE").apply(
        lambda x: [e for e in x if e is not None], axis=1
    )
    df["PROCCODES"] = df.filter(regex="PROCCODE").apply(
        lambda x: [e for e in x if e is not None], axis=1
    )
    df["PROVIDER_SVCS"] = df.filter(regex="DOCSVC").apply(
        lambda x: [e for e in x if e is not None], axis=1
    )
    df["PROVIDER_TYPES"] = df.filter(regex="PROVTYPE").apply(
        lambda x: [e for e in x if e is not None], axis=1
    )
    df["GENDER"] = df["GENDER"].replace(gender_map)
    df = df.drop(drop_columns, axis=1)
    return df.set_index('BI_NBR')


def format_nacrs(fn):
    df = pd.read_csv(
        fn,
        low_memory=False,
        na_values=[""],
        dtype={
            "INST": str,
            "INSTFROM": str,
            "INSTTO": str,
            "INST_ZONE": str,
            "MIS_CODE": str,
            "PROVIDER_TYPE1": str,
            "PROVIDER_SVC1": str,
            "PROVIDER_TYPE2": str,
            "PROVIDER_SVC2": str,
            "PROVIDER_TYPE3": str,
            "PROVIDER_SVC3": str,
            "PROVIDER_TYPE4": str,
            "PROVIDER_SVC4": str,
            "PROVIDER_TYPE5": str,
            "PROVIDER_SVC5": str,
            "PROVIDER_TYPE6": str,
            "PROVIDER_SVC6": str,
            "PROVIDER_TYPE7": str,
            "PROVIDER_SVC7": str,
            "PROVIDER_TYPE8": str,
            "PROVIDER_SVC8": str,
            "DXCODE1": str,
            "DXCODE2": str,
            "DXCODE3": str,
            "DXCODE4": str,
            "DXCODE5": str,
            "DXCODE6": str,
            "DXCODE7": str,
            "DXCODE8": str,
            "DXCODE9": str,
            "DXCODE10": str,
            "PROCCODE1": str,
            "PROCCODE2": str,
            "PROCCODE3": str,
            "PROCCODE4": str,
            "PROCCODE5": str,
            "PROCCODE6": str,
            "PROCCODE7": str,
            "PROCCODE8": str,
            "PROCCODE9": str,
            "PROCCODE1": str,
        },
    )
    df["VISIT_DATE"] = pd.to_datetime(
        df["VISIT_DATE"], format="%Y%m%d", errors="coerce"
    )
    df["DISP_DATE"] = pd.to_datetime(df["DISP_DATE"], format="%Y%m%d", errors="coerce")
    df["ED_DEPT_DATE"] = pd.to_datetime(
        df["ED_DEPT_DATE"], format="%Y%m%d", errors="coerce"
    )
    df["DISP_TIME"] = pd.to_datetime(
        df["DISP_TIME"], format="%H%M", errors="coerce"
    ).dt.time
    df["ED_DEPT_TIME"] = pd.to_datetime(
        df["ED_DEPT_TIME"], format="%H%M", errors="coerce"
    ).dt.time
    for col in ["VISIT_LOS_MINUTES", "EIP_MINUTES", "ED_ER_MINUTES", "AGE_ADMIT"]:
        df[col] = df[col].astype(float)
    df = df.rename(columns={"SEX": "GENDER", "ISOLATE_NBR": "BI_NBR"})
    df["GENDER"] = df["GENDER"].replace(gender_map)
    df = df.rename(columns={"Post_code": "POSTCODE"})
    drop_columns = df.filter(
        regex="DXCODE|PROCCODE|PROVIDER_SVC|PROVIDER_TYPE"
    ).columns.to_list()
    df["DXCODES"] = df.filter(regex="DXCODE").apply(
        lambda x: [e for e in x if e is not None], axis=1
    )
    df["PROCCODES"] = df.filter(regex="PROCCODE").apply(
        lambda x: [e for e in x if e is not None], axis=1
    )
    df["PROVIDER_SVCS"] = df.filter(regex="PROVIDER_SVC").apply(
        lambda x: [e for e in x if e is not None], axis=1
    )
    df["PROVIDER_TYPES"] = df.filter(regex="PROVIDER_TYPE").apply(
        lambda x: [e for e in x if e is not None], axis=1
    )
    df = df.drop(drop_columns, axis=1)
    return df.set_index("BI_NBR")


def format_claims(fn):
    df = pd.read_csv(
        fn,
        low_memory=False,
        dtype={
            "DISDATE": str,
            "DISTIME": str,
            "HLTH_DX_ICD9X_CODE_1": str,
            "HLTH_DX_ICD9X_CODE_2": str,
            "HLTH_DX_ICD9X_CODE_3": str,
            "HLTH_SRVC_CCPX_CODE": str,
        },
        na_values=[""],
    )
    df["HLTH_DX_ICD9X_CODES"] = df.filter(regex="ICD9X").apply(
        lambda x: [e for e in x if e is not None], axis=1
    )
    df = df.drop(
        ["HLTH_DX_ICD9X_CODE_1", "HLTH_DX_ICD9X_CODE_2", "HLTH_DX_ICD9X_CODE_3"], axis=1
    )
    df["SE_END_DATE"] = convert_datetime(df["SE_END_DATE"])
    df["SE_START_DATE"] = convert_datetime(df["SE_START_DATE"])
    df = df.rename(columns={"ISOLATE_NBR": "BI_NBR"})
    df = df.sort_values(["BI_NBR", "SE_START_DATE", "SE_END_DATE"])
    return df.set_index("BI_NBR")


def format_dad(fn):
    df = pd.read_csv(
        fn,
        low_memory=False,
        dtype={"INST": str, "INSTFROM": str, "INSTTO": str},
        na_values=[""],
    )
    df["ADMITDATE"] = pd.to_datetime(df["ADMITDATE"], format="%Y%m%d", errors="coerce")
    df["DISDATE"] = pd.to_datetime(df["DISDATE"], format="%Y%m%d", errors="coerce")
    df["ADMITTIME"] = pd.to_datetime(
        df["ADMITTIME"], format="%H%M", errors="coerce"
    ).dt.time
    df["DISTIME"] = pd.to_datetime(
        df["DISTIME"], format="%H%M", errors="coerce"
    ).dt.time
    df = df.rename(columns={"SEX": "GENDER", "ISOLATE_NBR": "BI_NBR"})
    df["GENDER"] = df["GENDER"].replace(gender_map)
    drop_columns = df.filter(regex="DXCODE|DXTYPE|PROCCODE").columns.to_list()
    df["DXCODES"] = df.filter(regex="DXCODE").apply(
        lambda x: [e for e in x if e is not None], axis=1
    )
    df["PROCCODES"] = df.filter(regex="PROCCODE").apply(
        lambda x: [e for e in x if e is not None], axis=1
    )
    df = df.drop(drop_columns, axis=1)
    df = df.rename(columns={"Post_code": "POSTCODE"})
    return df.set_index("BI_NBR")


def format_lab(fn):
    df = pd.read_csv(
        fn, low_memory=False, dtype={"TEST_CD": str}, na_values=[""]
    )
    df = df.rename(columns={"ISOLATE_NBR": "BI_NBR"})
    df["TEST_VRFY_DTTM"] = convert_datetime(df["TEST_VRFY_DTTM"])
    return df.set_index("BI_NBR")


def format_pin(fn):
    df = pd.read_csv(
        fn, low_memory=False, dtype=str, na_values=[""]
    )
    df["DSPN_DATE"] = convert_datetime(df["DSPN_DATE"])
    for col in ["DSPN_AMT_QTY", "DSPN_DAY_SUPPLY_QTY"]:
        df[col] = df[col].astype(float)
    df = df.rename(columns={"RCPT_GENDER_CD": "GENDER", "ISOLATE_NBR": "BI_NBR"})
    df = df.drop('GENDER', axis=1)
    return df.set_index("BI_NBR")


def format_reg(fn):
    df = pd.read_csv(
        fn, low_memory=False, dtype=str, na_values=[""]
    )
    df["PERS_REAP_END_DATE"] = convert_datetime(df["PERS_REAP_END_DATE"])
    for col in [
        "ACTIVE_COVERAGE",
        "AGE_GRP_CD",
        "DEATH_IND",
        "FYE",
        "IN_MIGRATION_IND",
        "OUT_MIGRATION_IND",
    ]:
        df[col] = df[col].astype(int)
    df = df.drop(["SEX", "AGE_GRP_CD"], axis=1)
    df = df.rename(columns={"ISOLATE_NBR": "BI_NBR", "Post_code": "POSTCODE"})
    df['IN_MIGRATION_IND'] = df['IN_MIGRATION_IND'].astype(bool)
    df['OUT_MIGRATION_IND'] = df['OUT_MIGRATION_IND'].astype(bool)
    
    return df.set_index("BI_NBR")


def format_vs(fn):
    df = pd.read_csv(
        fn, low_memory=False, dtype=str, na_values=[""]
    )
    df["DETHDATE"] = convert_datetime(df["DETHDATE"])
    df = df.rename(
        columns={"SEX": "GENDER", "DETHDATE": "DEATH_DATE", "ISOLATE_NBR": "BI_NBR"}
    )
    df["GENDER"] = df["GENDER"].replace(gender_map)
    df["AGE"] = df["AGE"].astype(int)
    df = df.rename(columns={"Post_code": "POSTCODE"})
    return df.set_index("BI_NBR")



def format_atc(fn):
    df = pd.read_parquet(fn)
    df["DRUG_LABEL"] = df.DRUG_LABEL.str.capitalize()
    return df


def format_postcodes_meta(fn):
    return (
        pd.read_csv(fn)
        .rename(columns={"Postal Code": "POSTCODE", "Place Name": "REGION", 'Province' : 'PROVINCE', 'Latitude': 'LATITUDE', 'Longitude': 'LONGITUDE'})
        .set_index("POSTCODE")
        .drop(["Unnamed: 5", "Unnamed: 6"], axis=1)
    )


def format_population(fn):
    df = pd.read_csv(fn).rename(
        columns={
            "Year": "YEAR",
            "Sex": "GENDER",
            "Age": "AGE",
            "Population": "POPULATION",
        }
    )
    dense = (
        df[(df.GENDER != "BOTH") & (df.AGE != "ALL")][
            ["YEAR", "GENDER", "AGE", "POPULATION"]
        ]
        .set_index(["YEAR", "GENDER", "AGE"])
        .unstack(
            [
                "GENDER",
                "AGE",
            ]
        )
        .astype(int)
        .sort_index(axis=1)
    )
    return dense
