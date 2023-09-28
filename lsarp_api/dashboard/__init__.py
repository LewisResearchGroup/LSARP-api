import pandas as pd



FN_OUTCOME = '/bulk/LSARP/datasets/220401-AW__AHS_Outcome_Data/230809_Outcomes_and_Scores/230809-AW__Outcomes_and_SeverityScores.csv'

DASHBOARD_COLUMNS = ['ORGANISM', 'ORG_LONG_NAME', 'ORG_SHORT_NAME', 'ORG_GENUS',
       'ORG_GRAM_TYPE', 'ORG_GROUP', 'ORG_GROUP_SHORT', 'AGE_GRP', 'NAGE_YR',
       'GENDER', 'CURRENT_PT_FACILITY', 'COLLECT_DTM', 'DAY',
       'DAYOFWEEK', 'DAYOFYEAR', 'DATE', 'MONTH', 'QUARTER', 'QUADRIMESTER',
       'YEAR', 'YEAR_DAY', 'YEAR_WEEK', 'YEAR_MONTH', 'YEAR_QUARTER',
       'YEAR_QUADRIMESTER', 'HOSPITAL_ONSET_48H', 'HOSPITAL_ONSET_72H', 
       '5-Fluorocytosine', 'Amikacin', 'Amoxicillin',
       'Amoxicillin-clavulanate', 'Amphotericin B', 'Ampicillin',
       'Anidulafungin', 'Azithromycin', 'Aztreonam', 'Caspofungin',
       'Cefazolin', 'Cefepime', 'Cefiderocol', 'Cefixime', 'Cefotaxime',
       'Cefoxitin', 'Cefpodoxime', 'Ceftaroline', 'Ceftazidime',
       'Ceftazidime-avibactam', 'Ceftobiprole', 'Ceftolozane-tazobactam',
       'Ceftriaxone', 'Cefuroxime', 'Cephalexin', 'Cephalothin',
       'Chloramphenicol', 'Ciprofloxacin', 'Clindamycin', 'Cloxacillin',
       'Colistin', 'Daptomycin', 'Doxycycline', 'Ertapenem', 'Erythromycin',
       'Fluconazole', 'Fosfomycin', 'Fusidic Acid', 'Gentamicin',
       'Itraconazole', 'Ketoconazole', 'Levofloxacin', 'Linezolid',
       'Meropenem', 'Metronidazole', 'Micafungin', 'Minocycline',
       'Moxifloxacin', 'Mupirocin', 'Nalidixic Acid', 'Nitrofurantoin',
       'Norfloxacin', 'Penicillin', 'Piperacillin', 'Piperacillin-tazobactam',
       'Plazomicin', 'Posaconazole', 'Quinupristin-dalfopristin', 'Rifampin',
       'Streptomycin', 'Tetracycline', 'Ticarcillin-clavulanate',
       'Tigecycline', 'Tobramycin', 'Trimethoprim',
       'Trimethoprim-sulfamethoxazole', 'Vancomycin', 'Voriconazole'
]



def create_dashboard_data(apl_results, fn_outcome=FN_OUTCOME):
    apl_results = apl_results.set_index('BI_NBR')

    # Filter for index isoloates
    index_isolates = apl_results[apl_results.INDEX_30DAYS.fillna(False)]
    outcome = pd.read_csv(fn_outcome).set_index('BI_NBR').filter(regex='OUTCOME|SCORE|COMORB')
    outcome_colums = outcome.columns.to_list()
    df = pd.merge(index_isolates, outcome, how='left', left_index=True, right_index=True)
    df = df[DASHBOARD_COLUMNS+outcome_colums]
    return df