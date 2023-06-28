import functools
import pandas as pd
import logging
from pathlib import Path as P
from datetime import datetime, time, timedelta

from .formatters import *
from ..tools import today

gender_map = {"M": "Male", "F": "Female"}

# Convert time to timedelta
to_timedelta = lambda t: timedelta(hours=t.hour, minutes=t.minute)

VERSION = '230515'


def convert_datetime(x):
    return pd.to_datetime(x, format="%d%b%Y:%H:%M:%S", errors="coerce")


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

    postcodes_meta - postal code data

    population - population estimates in Calgary

    proccodes_meta - PROCCODE and description

    dxcodes_meta - DXCODE and description

    """

    
    
    
    def __init__(self, version=VERSION):

        self.accs, self.claims, self.dad, self.lab, self.nacrs, self.pin, self.reg, self.vs = None, None, None, None, None, None, None, None

        PATH = P(f'/bulk/LSARP/datasets/AHS/versions/{version}')

        self.FNS = {
            "accs": {
                "fn": PATH / f"{version}_RMT24154_PIM_ACCS_May2022.parquet",
            },
            "claims": {
                "fn": PATH / f"{version}_RMT24154_PIM_CLAIMS_May2022.parquet",
            },
            "dad": {
                "fn": PATH / f"{version}_RMT24154_PIM_DAD_May2022.parquet",
            },
            "lab": {
                "fn": PATH / f"{version}_RMT24154_PIM_LAB_May2022.parquet",
            },
            "nacrs": {
                "fn": PATH / f"{version}_RMT24154_PIM_NACRS_May2022.parquet",
            },
            "pin": {
                "fn": PATH / f"{version}_RMT24154_PIM_PIN_May2022.parquet",
            },
            "vs": {
                "fn": PATH / f"{version}_RMT24154_PIM_VS_May2022.parquet",
            },
            "reg": {
                "fn": PATH / f"{version}_RMT24154_PIM_REGISTRY_May2022.parquet",
            },
            
            "atccodes": {
                "fn": PATH / f"/bulk/LSARP/datasets/211108-sw__ATC-codes/ATC_small.parquet",
            },

            "ccpcodes": {
                    "fn": "/bulk/LSARP/datasets/AHS/CCP-CODES/CCP-codes.parquet"
            },    

            "population": {
                "fn": PATH / "/bulk/LSARP/datasets/221114-am_calgary_population_zone/221114-am_calgary_population_zone.csv",
            },
            
            "postcodes_meta": {
                "fn": PATH / "/bulk/LSARP/datasets/221125-sw__postal-codes-from_www-aggdata-com/221125-sw__postal-codes-from_www-aggdata-com.csv",
            },
            
            "proccodes_meta": {
                "fn": PATH / "/bulk/LSARP/datasets/AHS/PROCCODES/221020-sw__proccodes.csv",
            },
            
            "dxcodes_meta": {
                "fn": PATH / "/bulk/LSARP/datasets/AHS/DXCODES/221020-sw__dxcodes.parquet",
            },
            "postcodes": {
                "fn":  PATH / f"{version}_AHS-derived__consensus_postal_codes.parquet",
            },

            "proccodes": {
                'fn': PATH / f"{version}_AHS-derived__proccodes_long.parquet"
            },

            "dxcodes": {
                'fn': PATH / f"{version}_AHS-derived__dxcodes_long.parquet"
            }   
        }
        
        FNS = self.FNS
        
        self.atccodes = format_atc(FNS["atccodes"]['fn'])
        
        self.ccpcodes = pd.read_parquet(FNS['ccpcodes']['fn'])

        self.population = format_population(FNS["population"]['fn'])
        
        self.postcodes_meta = format_postcodes_meta(FNS["postcodes_meta"]['fn'])
        
        self.proccodes_meta = pd.read_csv(FNS["proccodes_meta"]['fn'])
        
        self.dxcodes_meta = pd.read_parquet(FNS["dxcodes_meta"]['fn'])
           
        fn = FNS["postcodes"]["fn"]
        if fn.is_file():
            logging.warning(f'Reading consensus postal codes from {fn}')
            self.postcodes = pd.read_parquet(fn)
        else:
            logging.warning(f'File not found {fn}')

        fn = FNS["proccodes"]["fn"]
        if fn.is_file():
            logging.warning(f'Reading proccodes from {fn}')
            self.proccodes = pd.read_parquet(fn)
        else:
            logging.warning(f'File not found {fn}')

        fn = FNS["dxcodes"]["fn"]
        if fn.is_file():
            logging.warning(f'Reading dxcodes from {fn}')
            self.dxcodes = pd.read_parquet(fn)
        else:
            logging.warning(f'File not found {fn}')                



    def load(self, what):
        if what in ['accs', 'all']:
            fn = self.FNS['accs']['fn']
            logging.warning(f'Loading ACCS data from {fn}')
            self.acc = pd.read_parquet(fn)

        if what in ['claims', 'all']:    
            fn = self.FNS['claims']['fn']
            logging.warning(f'Loading CLAIMS data from {fn}')
            self.claims = pd.read_parquet(fn)

        if what in ['dad', 'all']:
            fn = self.FNS['dad']['fn']
            logging.warning(f'Loading DAD data from {fn}')
            self.dad = pd.read_parquet(fn)

        if what in ['lab', 'all']:    
            fn = self.FNS['lab']['fn']
            logging.warning(f'Loading LAB data from {fn}')
            self.lab = pd.read_parquet(fn)

        if what in ['nacrs', 'all']:
            fn = self.FNS['nacrs']['fn']
            logging.warning(f'Loading NACRS data from {fn}')
            self.nacrs = pd.read_parquet(fn)

        if what in ['pin', 'all']:    
            fn = self.FNS['pin']['fn']
            logging.warning(f'Loading PIN data from {fn}')
            self.pin = pd.read_parquet(fn)

        if what in ['reg', 'all']:
            fn = self.FNS['reg']['fn']
            logging.warning(f'Loading REG data from {fn}')
            self.reg = pd.read_parquet(fn)

        if what in ['vs', 'all']:    
            fn = self.FNS['vs']['fn']
            logging.warning(f'Loading VS data from {fn}')
            self.vs = pd.read_parquet(fn)


    def get_slice(self, kind='dxcodes', days_before=365, days_after=-7, reference_time_col='COLLECT_DTM', time_col='ADMITDATE'):
        if kind == 'dxcodes':
            df = self.dxcodes
        elif kind == 'proccodes':
            df = self.proccodes
        return get_slice(df, days_before=days_before, days_after=days_after, reference_time_col=reference_time_col, time_col=time_col)

    def merge_dxcodes(self):
        icd10codes = self.dxcodes_meta[self.dxcodes_meta.DIAG_CLASS_CD=='ICD10CA'].reset_index().groupby('DXCODE').first()
        self.dxcodes = pd.merge(self.dxcodes, icd10codes, left_on='DXCODES', right_index=True)

    def merge_proccodes(self):
        self.proccodes = pd.merge(self.proccodes, self.proccodes_meta, left_on='PROCCODES', right_on='PROCCODE')


def get_slice(df, days_before=365, days_after=-7, reference_time_col='COLLECT_DTM', time_col='ADMITDATE'):
    df = df.dropna().copy()
    n_days = (df[time_col].dt.date - df[reference_time_col].dt.date)
    
    df['n_days'] = [e.days if e is not None else None for e in n_days]


    #df = df[df.n_days.notna()]
    
    slize = df[(df.n_days > -days_before) & (df.n_days < days_after)]
    return slize