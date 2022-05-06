import functools
import pandas as pd
from pathlib import Path as P


gender_map = {"M": "Male", "F": "Female"}

DPATH = P('/bulk/LSARP/datasets/AHS/220506')

FNS = {
    'accs': 'RMT24154_PIM_ACCS_May2022.csv',
    'claims': 'RMT24154_PIM_CLAIMS_May2022.csv',
    'dad': 'RMT24154_PIM_DAD_DE_IDENT.csv',
    'lab': 'RMT24154_PIM_LAB_May2022.csv',
    'narcs': 'RMT24154_PIM_NACRS_May2022.csv',
    'pin': 'RMT24154_PIM_PIN_May2022.csv',
    'vs': 'RMT24154_PIM_VS_May2022.csv',
    'reg': 'RMT24154_PIM_REGISTRY_May2022.csv',
    'dad': 'RMT24154_PIM_DAD_May2022.csv',
    'atc': '/bulk/LSARP/datasets/211108-sw__ATC-codes/ATC_small.csv',
    'pop': '/bulk/LSARP/datasets/211109-sw__Calgary-Population-Estimates/211109-sw__Interpolated-Monthly-Population-Calgary.csv',
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


@csv_parquet(fn=DPATH / FNS['accs'])
def get_accs(fn):
    df = pd.read_csv(
        fn, low_memory=False, dtype={"VISDATE": str, "DISDATE": str, "DISTIME": str},
    )
    df["VISDATE"] = pd.to_datetime(df["VISDATE"], format="%Y%m%d", errors="coerce")
    df["DISDATE"] = pd.to_datetime(df["DISDATE"], format="%Y%m%d", errors="coerce")
    df["DISTIME"] = pd.to_datetime(
        df["DISTIME"], format="%H%M", errors="coerce"
    ).dt.time
    df = df.rename(columns={"SEX": "GENDER", "ISOLATE_NBR": "BI_NBR"})
    df["GENDER"] = df["GENDER"].replace(gender_map)
    return df


@csv_parquet(fn=DPATH / FNS['claims'])
def get_claims(fn):
    df = pd.read_csv(
        fn.with_suffix(".csv"), low_memory=False, dtype={"DISDATE": str, "DISTIME": str}
    )
    df["SE_END_DATE"] = convert_datetime(df["SE_END_DATE"])
    df["SE_START_DATE"] = convert_datetime(df["SE_START_DATE"])
    df = df.rename(columns={"ISOLATE_NBR": "BI_NBR"})
    return df


@csv_parquet(fn=DPATH / FNS['dad'])
def get_dad(fn):
    df = pd.read_csv(fn.with_suffix(".csv"), low_memory=False)
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
    return df


@csv_parquet(fn=DPATH / FNS['lab'])
def get_lab(fn):
    df = pd.read_csv(fn.with_suffix(".csv"), low_memory=False)
    df = df.rename(columns={"ISOLATE_NBR": "BI_NBR"})
    df["TEST_VRFY_DTTM"] = convert_datetime(df["TEST_VRFY_DTTM"])
    return df


@csv_parquet(fn=DPATH / FNS['narcs'])
def get_nacrs(fn):
    df = pd.read_csv(fn.with_suffix(".csv"), low_memory=False, dtype=str)
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
    return df


@csv_parquet(fn=DPATH / FNS['pin'])
def get_pin(fn):
    df = pd.read_csv(fn.with_suffix(".csv"), low_memory=False, dtype=str)
    df["DSPN_DATE"] = convert_datetime(df["DSPN_DATE"])
    for col in ["DSPN_AMT_QTY", "DSPN_DAY_SUPPLY_QTY"]:
        df[col] = df[col].astype(float)
    df = df.rename(columns={"RCPT_GENDER_CD": "GENDER", "ISOLATE_NBR": "BI_NBR"})
    df["GENDER"] = df["GENDER"].replace(gender_map)
    atc = get_atc()
    atc["DRUG_LABEL"] = atc.DRUG_LABEL.str.capitalize()
    df = pd.merge(df, atc, how="left")
    return df


@csv_parquet(fn=DPATH / FNS['reg'])
def get_reg(fn):
    df = pd.read_csv(fn.with_suffix(".csv"), low_memory=False, dtype=str)
    df["PERS_REAP_END_DATE"] = convert_datetime(df["PERS_REAP_END_DATE"])
    for col in [
        "ACTIVE_COVERAGE",
        "AGE_GRP_CD",
        "DEATH_IND",
        "FYE",
        "IN_MIGRATION_IND",
        "OUT_MIGRATION_IND",
    ]:
        df[col] = df[col].astype(float)
    df = df.rename(columns={"SEX": "GENDER", "ISOLATE_NBR": "BI_NBR"})
    df["GENDER"] = df["GENDER"].replace(gender_map)
    return df


@csv_parquet(fn=DPATH / FNS['vs'])
def get_vs(fn):
    df = pd.read_csv(fn.with_suffix(".csv"), low_memory=False, dtype=str)
    df["DETHDATE"] = convert_datetime(df["DETHDATE"])
    df = df.rename(
        columns={"SEX": "GENDER", "DETHDATE": "DEATH_DATE", "ISOLATE_NBR": "BI_NBR"}
    )
    df["GENDER"] = df["GENDER"].replace(gender_map)
    df["AGE"] = df["AGE"].astype(int)
    return df


@csv_parquet(fn=FNS['atc'])
def get_atc(fn):
    df = pd.read_csv(fn)
    df["DRUG_LABEL"] = df.DRUG_LABEL.str.capitalize()
    return df


@csv_parquet(
    fn=FNS['pop']
)
def get_population(fn):
    df = pd.read_csv(fn)
    return df


class AHS:
    """
    Has AHS datasets stored in attributes:
    -----
    accs - older version of the NACRS database
    claims - Practitioner Claims,Physician Claims, Physician Billing
    dad - Discharge Abstract Database (DAD), Inpatient
    lab - Provinicial Laboratory(Lab)
    narcs - National Ambulatory Care Reporting System (NACRS) & Alberta Ambulatory Care Reporting System (AACRS)
    pin - Pharmaceutical Information Network(PIN)
    reg - Alberta Health Care Insurance Plan (AHCIP) Registry
    vs - Vital Statistics-Death
    """

    def __init__(self):

        self.accs = get_accs()
        self.claims = get_claims()
        self.dad = get_dad()
        self.lab = get_lab()
        self.narcs = get_nacrs()
        self.pin = get_pin()
        self.reg = get_reg()
        self.vs = get_vs()
        self.atc = get_atc()
        self.population = get_population()

        self.antibiotics_names = self.pin[
            self.pin.SUPP_DRUG_ATC_CODE.fillna("").str.match("^J01")
        ].DRUG_LABEL.unique()

        self.datasets = dict(
         accs = self.accs,
         claims = self.claims,
         dad = self.dad,
         lab = self.lab,
         narcs = self.narcs,
         pin = self.pin,
         reg = self.reg,
         vs = self.vs,
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
        self.narcs = self.narcs[self.narcs.BI_NBR.isin(isolate_nbrs)].copy()
        self.pin = self.pin[self.pin.BI_NBR.isin(isolate_nbrs)].copy()
        self.reg = self.reg[self.reg.BI_NBR.isin(isolate_nbrs)].copy()
        self.vs = self.vs[self.vs.BI_NBR.isin(isolate_nbrs)].copy()
   
