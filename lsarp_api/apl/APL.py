import pandas as pd

import logging
import datetime

from ..tools import age_to_age_group, add_date_features_from_datetime_col
from .helpers import replace_interp, get_element, key_func_SIRN

FN = "/bulk/LSARP/datasets/APL/versions/current/230512-sw__APL.parquet"
FN_RESULTS = "/bulk/LSARP/datasets/APL/versions/current/230512-sw__APL-results-INTERP.parquet"

RESULTS_INDEX_COLS = [
    "SOURCE_LIS",
    "PID",
    "BI_NBR",
    "ORGANISM",
    "ORG_LONG_NAME",
    "ORG_SHORT_NAME",
    "ORG_GENUS",
    "ORG_GRAM_TYPE",
    "ORG_GROUP",
    "ORG_GROUP_SHORT",
    "AGE_GRP",
    "NAGE_YR",
    "GENDER",
    "ORD_ENCNTR_TYPE",
    "ENCR_GRP",
    "CURRENT_PT_FACILITY",
    "ENCNTR_ADMIT_DTM",
    "COLLECT_DTM",
    "CULT_START_DTM",
    "DSCHG_DTM",
    "DAY",
    "DAYOFWEEK",
    "DAYOFYEAR",
    "DATE",
    "MONTH",
    "QUARTER",
    "QUADRIMESTER",
    "YEAR",
    "YEAR_DAY",
    "YEAR_WEEK",
    "YEAR_MONTH",
    "YEAR_QUARTER",
    "YEAR_QUADRIMESTER",
    "HOSPITAL_ONSET_48H",
    "HOSPITAL_ONSET_72H",
    "COLLECT_HOURS_AFTER_ADMIT",
    "FLAG_COLLECT_24h_BEFORE_ADMIT",
    "FLAG_DSCHG_OUT_OF_BOUNDS",
    "FLAG_AGE_GT_130YR",
]


def load_apl_data(fn=FN, format_data=False, years=None, datetime_numeric=False):
    df = pd.read_parquet(fn)
    return df


def gen_encounters(df):
    cols = [
        "ORD_ENCNTR_NBR",
        "PID",
        "ENCNTR_ADMIT_DTM",
        "DSCHG_DTM",
        "ENCR_GRP",
        "AGE_GRP",
        "GENDER",
        "ORD_ENCNTR_TYPE",
        "VISIT_REASON",
        "SOURCE_LIS",
    ]
    return df[cols].drop_duplicates().reset_index(drop=True)


def gen_cultures(df):
    cols = [
        "ORD_ENCNTR_NBR",
        "BI_NBR",
        "PID",
        "NAGE_YR",
        "AGE_GRP",
        "GENDER",
        "ENCNTR_ADMIT_DTM",
        "DSCHG_DTM",
        "COLLECT_DTM",
        "ORGANISM",
        "YEAR",
        "CURRENT_PT_FACILITY",
    ]
    return df[cols].drop_duplicates().reset_index(drop=True)


def gen_bi_info(df):
    cols = [
        "ORD_ENCNTR_NBR",
        "BI_NBR",
        "PID",
        "NAGE_YR",
        "AGE_GRP",
        "GENDER",
        "ENCNTR_ADMIT_DTM",
        "DSCHG_DTM",
        "COLLECT_DTM",
        "CULT_START_DTM",
        "BODY_SITE",
        "ORGANISM",
        "SOURCE_LIS",
        "CURRENT_PT_FACILITY",
        "CURRENT_PT_LOCN",
        "YEAR",
    ]
    df = df[cols].drop_duplicates()
    df = df[df.BI_NBR.notna()]
    assert df.BI_NBR.value_counts().max() == 1, df.BI_NBR.value_counts()
    return df.reset_index(drop=True)


def gen_drugs(df):
    return (
        df.filter(regex="DRUG")
        .drop_duplicates()
        .dropna(how="all")
        .reset_index(drop=True)
    )


def gen_organisms(df):
    return (
        df.filter(regex="ORG")
        .drop_duplicates()
        .sort_values("ORG_LONG_NAME")
        .reset_index(drop=True)
    )


def gen_results(
    df,
    values=None,
    columns=None,
    index=None,
):

    if values is None:
        values = ['INTERP']
    if columns is None:
        columns = ['DRUG']
    if index is None:
        index = RESULTS_INDEX_COLS
    
    apl_df = df.copy()
    
    # Store index for later
    INDEX = apl_df[index].drop_duplicates()
    logging.info(f'Results index length: {len(INDEX)}')

    apl_df = apl_df[apl_df["DRUG"].notna()]
       
    apl_df = apl_df[apl_df.INTERP.isin(['S', 'R', 'I'])]    
        
    apl_df.BI_NBR = apl_df.BI_NBR.fillna("NA")
    missing_values = apl_df[index + columns + values].isna().sum()
    if missing_values.sum() != 0:
        missing_values = missing_values.loc[missing_values > 0]
        logging.warning(f"Selected columns contain missing values.\n{missing_values}")
    
    data = pd.pivot_table(
        apl_df,
        index=index,
        columns=columns,
        values=values,
        aggfunc=list,
        dropna=True, # dropna needs to be True otherwise out of memory
    )

    # Reindex here to get all samples
    data = data.reindex(INDEX).fillna('K')
    
    for col in data.columns:
        data[col] = data[col].apply(get_element)
        
    data = data.applymap(replace_interp)
    
    if len(values) == 1:
        data.columns = data.columns.get_level_values(1)
        
    return data.reset_index()


def gen_reports(df):
    apl_reports = df[['BI_NBR', 'REPORT_NAME', 'REPORT_NAME_2', 'PARENT', 'RESULT_ENTRY', 'RESULT_DISPLAY']].set_index(['BI_NBR']).replace('', None).dropna().reset_index()
    reports = pd.pivot_table(apl_reports, index='BI_NBR', columns=['REPORT_NAME'], values='RESULT_DISPLAY', aggfunc=' '.join)
    return reports


class APL:
    def __init__(
        self,
        fn=FN,
        fn_results=FN_RESULTS,
        organisms=None,
        years=None,
        bi_nbrs=None,
        datetime_numeric=False,
    ):
        logging.warning(f"Loading APL data from {fn}")
        df = load_apl_data(fn=fn, years=years, datetime_numeric=datetime_numeric)
        self.df = df
        self.drugs = gen_drugs(df)
        self.organisms = gen_organisms(df)
        self.encounters = gen_encounters(df)
        self.cultures = gen_cultures(df)
        self.bi_info = gen_bi_info(df)
        self.index = None
        self.reports = gen_reports(df)

        if fn_results is not None:
            logging.warning(f"Loading APL-results from {fn_results}")
            self.results = pd.read_parquet(fn_results)

        tmp = df[["ORD_ENCNTR_NBR", "CURRENT_PT_FACILITY", "YEAR"]].drop_duplicates()
        self.total_counts_facility = pd.crosstab(tmp.CURRENT_PT_FACILITY, tmp.YEAR)

        if isinstance(organisms, str):
            organisms = [organisms]
        if (organisms is not None) or (bi_nbrs is not None):
            self.select_samples(organisms=organisms, bi_nbrs=bi_nbrs)
        self.key_func_SIRN = key_func_SIRN

    def select_samples(self, organisms=None, bi_nbrs=None):
        if organisms is not None:
            self.cultures = self.cultures[
                self.cultures.ORGANISM.isin(organisms)
            ].reset_index(drop=True)
        if bi_nbrs is not None:
            self.cultures = self.cultures[
                self.cultures.BI_NBR.isin(bi_nbrs)
            ].reset_index(drop=True)
        ord_nbr = self.cultures.ORD_ENCNTR_NBR.unique()
        bi_nbrs = self.cultures.BI_NBR.unique()
        self.encounters = self.encounters[
            self.encounters.ORD_ENCNTR_NBR.isin(ord_nbr)
        ].reset_index(drop=True)
        self.df = self.df[self.df.ORD_ENCNTR_NBR.isin(ord_nbr)].reset_index(drop=True)
        self.bi_info = self.bi_info[self.bi_info.BI_NBR.isin(bi_nbrs)].reset_index(
            drop=True
        )

    @property
    def summary(self):
        display(self.summary_data.style.background_gradient(axis=None))
        display(self.organism_count.style.background_gradient(axis=None))
        display(self.age_gender.style.background_gradient(axis=None))
        display(self.annual_counts_by_org.style.background_gradient(axis=None))

    @property
    def summary_data(self):
        summary_data = (
            pd.Series(
                {
                    "# Encounters": self.n_encounters,
                    "# Patients": self.n_patients,
                    "# BI numbers": self.n_bi_nbrs,
                }
            )
            .to_frame()
            .rename(columns={0: "Count"})
        )
        return summary_data

    @property
    def organism_count(self):
        organism_count = (
            self.cultures.ORGANISM.value_counts()
            .reset_index()
            .rename(columns={"index": "ORGANISM", "ORGANISM": "Count"})
        )
        return organism_count

    @property
    def age_gender(self):
        df = self.encounters
        age_gender = pd.crosstab(self.encounters.AGE_GRP, self.encounters.GENDER)
        return age_gender

    @property
    def n_patients(self):
        n_patients = len(self.df.PID.unique())
        return n_patients

    @property
    def n_encounters(self):
        n_encounters = len(self.encounters)
        return n_encounters

    @property
    def n_bi_nbrs(self):
        n_bi_nbrs = len(self.bi_info)
        return n_bi_nbrs

    @property
    def n_cultures(self):
        n_cultures = len(self.bi_info)
        return n_cultures

    def check_consistency(self):
        pass

    @property
    def annual_counts_by_org(self):
        df = self.cultures
        annual_counts_by_org = pd.crosstab(df.ORGANISM, df.YEAR)
        annual_counts_by_org = annual_counts_by_org.loc[
            annual_counts_by_org.sum(axis=1)
            .sort_values(ascending=False)
            .index.to_list()
        ]
        return annual_counts_by_org

    def gen_results(self, **kwargs):
        self.results = gen_results(self.df, **kwargs)
        return self.results

    def pivot_results(
        self, antibiotics, columns=["YEAR", "ORGANISM"], stack_col="ORGANISM"
    ):

        results = self.results.copy()

        if isinstance(antibiotics, str):
            antibiotics = [antibiotics]
        swaplevel = len(antibiotics)
        
        df = (
            pd.pivot_table(
                data=results[antibiotics].fillna("N").reset_index(),
                index=antibiotics,
                columns=columns,
                aggfunc="count",
                values="CULT_ID",
            )
            .fillna(0)
            .sort_index(key=key_func_SIRN)
            .astype(int)
            .stack(stack_col, dropna=False)
            .fillna(0)
            .swaplevel(0)
            .sort_index(level=0, sort_remaining=False)
        )
        return df

    @property
    def pid_bi_nbr(self):
        return (
            self.cultures[["PID", "BI_NBR"]]
            .dropna()
            .drop_duplicates()
            .reset_index(drop=True)
        )

    def gen_index(self, episode_cutoff=30, add_to_df=True, add_to_results=True):
        assert self.results is not None
        index = separate_BSI_episodes(self.results, episode_cutoff=episode_cutoff)
        self.index = index
        if add_to_df:
            self.df = pd.merge(self.df, index, on='BI_NBR', how='left')
        if add_to_results:
            self.results = pd.merge(self.results, index, on='BI_NBR', how='left')
        return index

    def export_dashboard_data(self, fn_out):
        export_dashboard_data(self.results.reset_index(), fn_out)
        

        
def separate_BSI_episodes(data, episode_cutoff):
    """
    Mui's algorithm to identify episode index isolates.
    Re-implemented in Python.
    Original script: LSARP/datasets/220726-tmp__APL-grouped-BSI-episodes/code/220726-tmp__APL-grouped_BSI-episodes.R
    """
    data = data[data.BI_NBR.str.startswith('BI')].copy()

    data['COLLECT_DTM'] = pd.to_datetime(data['COLLECT_DTM'], format="%Y-%m-%d")
    df_APL = (data.sort_values(by=['PID', 'ORG_LONG_NAME', 'COLLECT_DTM'])
                .groupby(['PID', 'ORG_LONG_NAME'], group_keys=False)
                .apply(lambda x: x.assign(ORGANISM=x['ORG_LONG_NAME'],
                                          diff=(x['COLLECT_DTM'] - x['COLLECT_DTM'].shift(fill_value=x['COLLECT_DTM'].iloc[0]))
                                                    .apply(lambda x: x / datetime.timedelta(days=1)),
                                          cum_diff=(x['COLLECT_DTM'] - x['COLLECT_DTM'].iloc[0])
                                                    .apply(lambda x: x / datetime.timedelta(days=1))
                                        ))
                .reset_index(drop=True))

    # Separate BSI episode with respect to "episode_cutoff"
    # Assign episode numbers
    for p in df_APL['PID'].unique():
        for o in df_APL[df_APL['PID'] == p]['ORGANISM'].unique():
            rows = df_APL.loc[(df_APL['PID'] == p) & (df_APL['ORGANISM'] == o)].index
            # Select and sort by dates
            df_current = df_APL.loc[rows].sort_values(by='COLLECT_DTM')
            # First isolate belongs to first BSI episode
            df_APL.loc[rows[0], 'episode_NBR'] = 1
            # Flag whether this is an index isolate for the respective BSI episode
            df_APL.loc[rows[0], 'index'] = True
            # Row number of index isolate of current BSI episode; gets updated
            index_isolate = 0
            # Number of current BSI episode; gets updated
            episode_i = 1
            for i in range(1, len(df_current)):
                # Check if current isolate is less than "episode_cutoff" days from index isolate
                if df_current.loc[rows[i], 'cum_diff'] <= df_current.loc[rows[index_isolate], 'cum_diff'] + episode_cutoff:
                    df_APL.loc[rows[i], 'episode_NBR'] = episode_i
                else:
                    # Update index isolate (i.e. new episode)
                    index_isolate = i
                    episode_i += 1
                    df_APL.loc[rows[i], 'episode_NBR'] = episode_i
                    df_APL.loc[rows[i], 'index'] = True

    # Total number of episodes per patient per organism
    df_APL['total_n_BI'] = df_APL.groupby(['PID', 'GENDER', 'ORGANISM'])['episode_NBR'].transform(max)

    # Total number of isolates
    df_APL['n_iso_episode'] = df_APL.groupby(['PID', 'GENDER', 'ORGANISM', 'episode_NBR'])['BI_NBR'].transform('nunique')
    
    #Renaming variables
    out_var_name = f'INDEX_{episode_cutoff}DAYS'
    out_var_name_episode = f'N_ISO_EPISODE_INDEX_{episode_cutoff}DAYS'
    out_var_name_total = 'TOTAL_N_BI_NBRS'
    out_var_name_episode_nbr = 'EPISODE_NBR'
    
    df_APL = df_APL.rename(columns={'index': out_var_name, 
                                    'n_iso_episode': out_var_name_episode, 
                                    'total_n_BI': out_var_name_total, 
                                    'episode_NBR': out_var_name_episode_nbr})

    out_vars = ['BI_NBR', out_var_name, out_var_name_episode, out_var_name_total, out_var_name_episode_nbr]
    return df_APL.reset_index()[out_vars].fillna(False)


DASHBOARD_COLUMNS = ['ORGANISM', 'ORG_LONG_NAME', 'ORG_SHORT_NAME', 'ORG_GENUS',
       'ORG_GRAM_TYPE', 'ORG_GROUP', 'ORG_GROUP_SHORT', 'AGE_GRP', 'NAGE_YR',
       'GENDER', 'ENCR_GRP', 'CURRENT_PT_FACILITY', 'COLLECT_DTM', 'DAY',
       'DAYOFWEEK', 'DAYOFYEAR', 'DATE', 'MONTH', 'QUARTER', 'QUADRIMESTER',
       'YEAR', 'YEAR_DAY', 'YEAR_WEEK', 'YEAR_MONTH', 'YEAR_QUARTER',
       'YEAR_QUADRIMESTER', 'HOSPITAL_ONSET_48H', 'HOSPITAL_ONSET_72H', 
       '5-Fluorocytosine',
       'Amikacin', 'Amoxicillin-clavulanate', 'Amphotericin B', 'Ampicillin',
       'Aztreonam', 'Caspofungin', 'Cefazolin', 'Cefepime', 'Cefotaxime',
       'Cefoxitin', 'Cefpodoxime', 'Ceftazidime', 'Ceftobiprole',
       'Ceftriaxone', 'Cefuroxime', 'Cephalothin', 'Chloramphenicol',
       'Ciprofloxacin', 'Clindamycin', 'Cloxacillin', 'Colistin', 'Daptomycin',
       'Ertapenem', 'Erythromycin', 'Fluconazole', 'Fusidic Acid',
       'Gentamicin', 'Gentamicin - High Level', 'Itraconazole', 'Ketoconazole',
       'Levofloxacin', 'Linezolid', 'Meropenem', 'Metronidazole',
       'Moxifloxacin', 'Mupirocin', 'Nalidixic Acid', 'Nitrofurantoin',
       'Norfloxacin', 'Penicillin', 'Piperacillin', 'Piperacillin-tazobactam',
       'Quinupristin-dalfopristin', 'Rifampin', 'Streptomycin - High Level',
       'Tetracycline', 'Tigecycline', 'Tobramycin', 'Trimethoprim',
       'Trimethoprim-sulfamethoxazole', 'Vancomycin', 'Voriconazole'
]


def export_dashboard_data(df, fn_out):
    df = df.set_index('BI_NBR')
    df = df[df.N_ISO_EPISODE_INDEX_30DAYS == True]
    df[DASHBOARD_COLUMNS].to_parquet(fn_out)
    
    