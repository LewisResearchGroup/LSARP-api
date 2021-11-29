import pandas as pd
from pathlib import Path as P

PATH = P('/bulk/LSARP/datasets/AHS_data')

gender_map = {'M': 'Male', 'F': 'Female'}

def convert_datetime(x):
    return pd.to_datetime(x, format='%d%b%Y:%H:%M:%S', errors='coerce')


def get_accs():
    fn = 'RMT24154_PIM_ACCS_DE_IDENT.csv'
    df = pd.read_csv(f'{PATH/fn}', low_memory=False, dtype={'VISDATE': str,'DISDATE': str, 'DISTIME': str})
    df['VISDATE'] =  pd.to_datetime(df['VISDATE'], format='%Y%m%d', errors='coerce')
    df['DISDATE'] =  pd.to_datetime(df['DISDATE'], format='%Y%m%d', errors='coerce')
    df['DISTIME'] =  pd.to_datetime(df['DISTIME'], format='%H%M', errors='coerce').dt.time
    df = df.rename(columns={'SEX': 'GENDER'})
    df['GENDER'] = df['GENDER'].replace(gender_map)
    return df


def get_claims():
    fn = 'RMT24154_PIM_CLAIMS_DE_IDENT.csv'
    df = pd.read_csv(f'{PATH/fn}', low_memory=False, dtype={'DISDATE': str, 'DISTIME': str})
    df['SE_END_DATE'] = convert_datetime(df['SE_END_DATE'])
    df['SE_START_DATE'] = convert_datetime(df['SE_START_DATE'] )
    return df  


def get_dad():
    fn = 'RMT24154_PIM_DAD_DE_IDENT.csv'
    df = pd.read_csv(f'{PATH/fn}', low_memory=False)
    df['ADMITDATE'] =  pd.to_datetime(df['ADMITDATE'], format='%Y%m%d', errors='coerce')
    df['DISDATE'] =  pd.to_datetime(df['DISDATE'], format='%Y%m%d', errors='coerce')
    df['ADMITTIME'] =  pd.to_datetime(df['ADMITTIME'], format='%H%M', errors='coerce').dt.time
    df['DISTIME'] =  pd.to_datetime(df['DISTIME'], format='%H%M', errors='coerce').dt.time
    df = df.rename(columns={'SEX': 'GENDER'})
    df['GENDER'] = df['GENDER'].replace(gender_map)    
    df = df.set_index('ISOLATE_NBR')    
    return df


def get_lab():
    fn = 'RMT24154_PIM_LAB_DE_IDENT.csv'
    df = pd.read_csv(f'{PATH/fn}', low_memory=False)
    df['TEST_VRFY_DTTM'] = convert_datetime(df['TEST_VRFY_DTTM'])
    df = df.set_index('ISOLATE_NBR')    
    return df


def get_nacrs():
    fn = 'RMT24154_PIM_NACRS_DE_IDENT.csv'
    df = pd.read_csv(f'{PATH/fn}', low_memory=False, dtype=str)
    df['VISIT_DATE'] =  pd.to_datetime(df['VISIT_DATE'], format='%Y%m%d', errors='coerce')
    df['DISP_DATE'] =  pd.to_datetime(df['DISP_DATE'], format='%Y%m%d', errors='coerce')
    df['ED_DEPT_DATE'] =  pd.to_datetime(df['ED_DEPT_DATE'], format='%Y%m%d', errors='coerce')
    df['DISP_TIME'] =  pd.to_datetime(df['DISP_TIME'], format='%H%M', errors='coerce').dt.time
    df['ED_DEPT_TIME'] =  pd.to_datetime(df['ED_DEPT_TIME'], format='%H%M', errors='coerce').dt.time
    for col in ['VISIT_LOS_MINUTES', 'EIP_MINUTES', 'ED_ER_MINUTES', 'AGE_ADMIT']: df[col] = df[col].astype(float)
    df = df.rename(columns={'SEX': 'GENDER'})
    df['GENDER'] = df['GENDER'].replace(gender_map)    
    df = df.set_index('ISOLATE_NBR')    
    return df


def get_pin():
    fn = 'RMT24154_PIM_PIN_DE_IDENT.csv'
    df = pd.read_csv(f'{PATH/fn}', low_memory=False, dtype=str)
    df['DSPN_DATE'] = convert_datetime(df['DSPN_DATE'])
    for col in ['DSPN_AMT_QTY', 'DSPN_DAY_SUPPLY_QTY']: df[col] = df[col].astype(float)
    df = df.rename(columns={'RCPT_GENDER_CD': 'GENDER'})
    df['GENDER'] = df['GENDER'].replace(gender_map)
    atc = pd.read_csv('/bulk/LSARP/datasets/ATC-codes/ATC_small.csv')
    atc['DRUG_LABEL'] = atc.DRUG_LABEL.str.capitalize()
    df = pd.merge(df, atc, how='left')
    return df


def get_reg():
    fn = 'RMT24154_PIM_REGISTRY_DE_IDENT.csv'
    df = pd.read_csv(f'{PATH/fn}', low_memory=False, dtype=str)
    df['PERS_REAP_END_DATE'] = convert_datetime(df['PERS_REAP_END_DATE'])
    for col in ['ACTIVE_COVERAGE', 'AGE_GRP_CD', 'DEATH_IND', 'FYE', 'IN_MIGRATION_IND', 'OUT_MIGRATION_IND']: df[col] = df[col].astype(float)
    df = df.rename(columns={'SEX': 'GENDER'})
    df['GENDER'] = df['GENDER'].replace(gender_map)
    return df


def get_vs():
    fn = 'RMT24154_PIM_VS_DE_IDENT.csv'
    df = pd.read_csv(f'{PATH/fn}', low_memory=False, dtype=str)
    df['DETHDATE'] = convert_datetime(df['DETHDATE'])
    df = df.rename(columns={'SEX': 'GENDER', 'DETHDATE': 'DEATH_DATE'})
    df['GENDER'] = df['GENDER'].replace(gender_map)
    df['AGE'] = df['AGE'].astype(int)
    return df

def get_atc():
    df =pd.read_csv('/bulk/LSARP/datasets/ATC-codes/ATC_small.csv')
    df['DRUG_LABEL'] = df.DRUG_LABEL.str.capitalize()
    return df

def get_population():
    df = pd.read_csv('/bulk/LSARP/datasets/211109-sw__Calgary-Population-Estimates/211109-sw__Interpolated-Monthly-Population-Calgary.csv')
    return df

class AHS:
    '''
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
    '''
    accs = get_accs()
    claims = get_claims()
    dad = get_dad()
    lab = get_lab()
    narcs = get_nacrs()
    pin = get_pin()
    reg = get_reg()
    vs = get_vs()
    atc = get_atc()
    pop = get_population()
    
    @property
    def drug_by_bi_nbr(self):
        return pd.pivot_table(data=self.pin, index='ISOLATE_NBR', columns='DRUG_LABEL', aggfunc='sum').fillna(0).astype(int)
