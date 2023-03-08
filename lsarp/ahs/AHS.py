import functools
import pandas as pd
from pathlib import Path as P


gender_map = {"M": "Male", "F": "Female"}

DPATH = P("/bulk/LSARP/datasets/AHS/220506")

FNS = {
    "accs": "RMT24154_PIM_ACCS_May2022.csv",
    "claims": "RMT24154_PIM_CLAIMS_May2022.csv",
    "dad": "RMT24154_PIM_DAD_DE_IDENT.csv",
    "lab": "RMT24154_PIM_LAB_May2022.csv",
    "nacrs": "RMT24154_PIM_NACRS_May2022.csv",
    "pin": "RMT24154_PIM_PIN_May2022.csv",
    "vs": "RMT24154_PIM_VS_May2022.csv",
    "reg": "RMT24154_PIM_REGISTRY_May2022.csv",
    "dad": "RMT24154_PIM_DAD_May2022.csv",
    "atc": "/bulk/LSARP/datasets/211108-sw__ATC-codes/ATC_small.csv",
    "population": "/bulk/LSARP/datasets/221114-am_calgary_population_zone/221114-am_calgary_population_zone.csv",
    "postcodes": "/bulk/LSARP/datasets/221125-sw__postal-codes-from_www-aggdata-com/221125-sw__postal-codes-from_www-aggdata-com.csv",
    "proccodes": "/bulk/LSARP/datasets/AHS/221020-sw__proccodes.parquet",
    "dxcodes": "/bulk/LSARP/datasets/AHS/221020-sw__dxcodes.parquet",
}


def csv_parquet(fn):
    """
    Decorator that  file
    """
    fn = P(fn).with_suffix(".csv")
    fnpq = fn.with_suffix(".parquet")

    def parquet_or_generate_parquet_from_csv(func):
        @functools.wraps(func)
        def wrapper():
            if fnpq.is_file():
                return pd.read_parquet(fnpq)
            else:
                df = func(fn)
                df.to_parquet(fnpq)
                return df

        return wrapper

    return parquet_or_generate_parquet_from_csv


def convert_datetime(x):
    return pd.to_datetime(x, format="%d%b%Y:%H:%M:%S", errors="coerce")


@csv_parquet(fn=DPATH / FNS["accs"])
def get_accs(fn):
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


@csv_parquet(fn=DPATH / FNS["nacrs"])
def get_nacrs(fn):
    df = pd.read_csv(
        fn.with_suffix(".csv"),
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


@csv_parquet(fn=DPATH / FNS["claims"])
def get_claims(fn):
    df = pd.read_csv(
        fn.with_suffix(".csv"),
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


@csv_parquet(fn=DPATH / FNS["dad"])
def get_dad(fn):
    df = pd.read_csv(
        fn.with_suffix(".csv"),
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


@csv_parquet(fn=DPATH / FNS["lab"])
def get_lab(fn):
    df = pd.read_csv(
        fn.with_suffix(".csv"), low_memory=False, dtype={"TEST_CD": str}, na_values=[""]
    )
    df = df.rename(columns={"ISOLATE_NBR": "BI_NBR"})
    df["TEST_VRFY_DTTM"] = convert_datetime(df["TEST_VRFY_DTTM"])
    return df.set_index("BI_NBR")


@csv_parquet(fn=DPATH / FNS["pin"])
def get_pin(fn):
    df = pd.read_csv(
        fn.with_suffix(".csv"), low_memory=False, dtype=str, na_values=[""]
    )
    df["DSPN_DATE"] = convert_datetime(df["DSPN_DATE"])
    for col in ["DSPN_AMT_QTY", "DSPN_DAY_SUPPLY_QTY"]:
        df[col] = df[col].astype(float)
    df = df.rename(columns={"RCPT_GENDER_CD": "GENDER", "ISOLATE_NBR": "BI_NBR"})
    df["GENDER"] = df["GENDER"].replace(gender_map)
    atc = get_atc()
    atc["DRUG_LABEL"] = atc.DRUG_LABEL.str.capitalize()
    df = pd.merge(df, atc, how="left")
    return df.set_index("BI_NBR")


@csv_parquet(fn=DPATH / FNS["reg"])
def get_reg(fn):
    df = pd.read_csv(
        fn.with_suffix(".csv"), low_memory=False, dtype=str, na_values=[""]
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
    return df.set_index("BI_NBR")


@csv_parquet(fn=DPATH / FNS["vs"])
def get_vs(fn):
    df = pd.read_csv(
        fn.with_suffix(".csv"), low_memory=False, dtype=str, na_values=[""]
    )
    df["DETHDATE"] = convert_datetime(df["DETHDATE"])
    df = df.rename(
        columns={"SEX": "GENDER", "DETHDATE": "DEATH_DATE", "ISOLATE_NBR": "BI_NBR"}
    )
    df["GENDER"] = df["GENDER"].replace(gender_map)
    df["AGE"] = df["AGE"].astype(int)
    df = df.rename(columns={"Post_code": "POSTCODE"})
    return df.set_index("BI_NBR")


@csv_parquet(fn=FNS["atc"])
def get_atc(fn):
    df = pd.read_csv(fn)
    df["DRUG_LABEL"] = df.DRUG_LABEL.str.capitalize()
    return df


def get_postcodes(fn):
    return (
        pd.read_csv(fn)
        .rename(columns={"Postal Code": "POSTCODE", "Place Name": "REGION", 'Province' : 'PROVINCE', 'Latitude': 'LATITUDE', 'Longitude': 'LONGITUDE'})
        .set_index("POSTCODE")
        .drop(["Unnamed: 5", "Unnamed: 6"], axis=1)
    )


def get_population(fn):
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


class AHS:
    """
    Has AHS datasets stored in attributes:
    -----
    accs - older version of the NACRS database

    claims - Practitioner Claims,Physician Claims, Physician Billing
         * FRE_ACTUAL_PAID_AMT - Financial Resource Event Actual Paid Amount
         * HLTH_DX_ICD9X_CODE - Diagnosis Code
         * HLTH_SRVC_CCPX_CODE - Health Service Canadian Classification of Procedures Extended Code
         * PGM_APP_IND - Program Alternate Payment Plan Indicator
         * RCPT_AGE_SE_END_YRS - Recipient Years of Age at Service Event End Date
         * RCPT_GENDER_CODE - Recipient Gender Code
         * SE_END_DATE - Service Event End Date
         * SE_START_DATE - Service Event Start Date
         * SECTOR - ? Not explained in metadata | one from ['INPATIENT', 'COMMUNITY', 'EMERGENCY', 'DIAGNOSTIC-THERAP', 'AMBULAT-OTHER']
         * DOCTOR_CLASS - ? Not explained in metadata | one from ['SPECIALIST', 'GP', 'ALLIED']

    dad - Discharge Abstract Database (DAD), Inpatient
         * INST - "Institution Number (Submitting Institution Number)"
         * ADMITDATE - Date of admission
         * ADMITTIME - Time of admission
         * INSTFROM - Instituion From
         * ADMITCAT - Admit Category describes the initial status of the patient at the time of admission to the reporting facility.
         * ENTRYCODE - Entry Code indicates the last point of entry prior to being admitted as an inpatient to the reporting facility.
         * ADMITBYAMB - Admit via Ambulance
         * DISDATE - Date of discharge
         * DISTIME - Time of discharge
         * INSTTO - Institution To
         * DISP - Discharge Disposition Code
         * DXCODE - Diagnosis Code
         * DXTYPE - Diagnosis Type Code
         * COMASCALE - Glasgow Coma Scale Code
         * ICU_HOURS - Intensive Care Unit LOS in hours
         * CCU_HOURS - Coronary Care Unit LOS in hours
         * AGE_ADMIT - Age at Admission
         * CMG - Case Mix Group Code
         * COMORB_LVL - Comorbidity Level Code
         * RIW_CODE - Resource Intensity Weight (RIW) Code | An Atypical code (01-99) is assigned based on an unusual CMG assignment, invalid length of stay, death, transfers to/from other acute care institutions, and sign-outs.  Typical cases are assigned code 00.
         * RIL - Resource Intensity Level Code |
         * RIW - Resource Intensity Weight | Resource Intensity Weight (RIW) values provide a measure of a patient's relative resource consumption compared to an average typical inpatient cost.
         * RCPT_ZONE - Zone of residence of the patient | Analytics derived.  The number of the AHS Zone where the patient lives.  If the patient does not live in Alberta, 9 - Unknown or out of province/country is populated.
         * INST_ZONE - Institution zone | The number that identifies the Alberta Health Services-zone where the submitting institution is located.  Zone boundaries are set by Alberta Health with input from Alberta Health Services.
         * ALL_DAYS - Total length of stay (LOS)
         * ACUTE_DAYS - Acute length of stay | "The Acute LOS is the Calculated Length of Stay minus the number of Alternate Level of Care (ALC) days. The ALC days (service) starts at the time of designation and ends at the time of discharge/transfer to a discharge destination or when the patient’s needs or condition changes and the designation of ALC no longer applies, as documented by the clinician.
         * POSTCODE - Three digits postal code of patients residence
         * PROCCODE - Intervention Code

    lab - Provinicial Laboratory(Lab)

    nacrs - National Ambulatory Care Reporting System (NACRS) & Alberta Ambulatory Care Reporting System (AACRS)

    pin - Pharmaceutical Information Network(PIN)

    reg - Alberta Health Care Insurance Plan (AHCIP) Registry
            * ACTIVE_COVERAGE - Person Active Coverage Indicator Fiscal Year End
            * DEATH_IND - Person Death Indicator Fiscal Year End
            * FYE - Fiscal Year End
            * PERS_REAP_END_DATE - Person Registration Eligibility And Premiums End Date
            * PERS_REAP_END_RSN_CODE - ? Not explained in metadata ?
            * POSTCODE - Three letter postal code

    vs - Vital Statistics-Death: The source of information in this dataset is Alberta Vital Statistics.  All deaths in Alberta must be registered with Alberta Vital Statistics. Information in the dataset is derived from the Death Registration form, medical certificate of death, and the medical examiners certificate of death (where applicable).  Additional derived variables are added to the file to facilitate queries and analysis of the data. The file is supplied to Alberta Health Services through Alberta Health.

    atc - ATC codes

    postcodes - postal code data

    population - population estimates in Calgary

    proccodes - PROCCODE and description

    dxcodes - DXCODE and description

    """

    def __init__(self):

        self.accs = get_accs()
        self.nacrs = get_nacrs()
        self.claims = get_claims()
        self.dad = get_dad()
        self.lab = get_lab()
        self.pin = get_pin()
        self.reg = get_reg()
        self.vs = get_vs()
        self.atc = get_atc()
        self.population = get_population(FNS["population"])
        self.postcodes = get_postcodes(FNS["postcodes"])
        self.proccodes = pd.read_parquet(FNS["proccodes"])
        self.dxcodes = pd.read_parquet(FNS["dxcodes"])

        self.antibiotics_names = self.pin[
            self.pin.SUPP_DRUG_ATC_CODE.fillna("").str.match("^J01")
        ].DRUG_LABEL.unique()

        self.datasets = dict(
            accs=self.accs,
            claims=self.claims,
            dad=self.dad,
            lab=self.lab,
            nacrs=self.nacrs,
            pin=self.pin,
            reg=self.reg,
            vs=self.vs,
        )

    @property
    def drug_by_bi_nbr(self):
        return (
            pd.pivot_table(
                data=self.pin, index="BI_NBR", columns="DRUG_LABEL", aggfunc="sum"
            )
            .fillna(0)
            .astype(int)
        )

    def select_isolate_numbers(self, isolate_nbrs):
        self.accs = self.accs[self.accs.BI_NBR.isin(isolate_nbrs)].copy()
        self.claims = self.claims[self.claims.BI_NBR.isin(isolate_nbrs)].copy()
        self.dad = self.dad[self.dad.BI_NBR.isin(isolate_nbrs)].copy()
        self.lab = self.lab[self.lab.BI_NBR.isin(isolate_nbrs)].copy()
        self.nacrs = self.nacrs[self.nacrs.BI_NBR.isin(isolate_nbrs)].copy()
        self.pin = self.pin[self.pin.BI_NBR.isin(isolate_nbrs)].copy()
        self.reg = self.reg[self.reg.BI_NBR.isin(isolate_nbrs)].copy()
        self.vs = self.vs[self.vs.BI_NBR.isin(isolate_nbrs)].copy()
