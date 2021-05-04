from glob import glob

import pandas as pd
import logging

from pathlib import Path as P
from pathlib import PureWindowsPath

from lrg_omics.metabolomics.common import metadata_from_filename

from .standards import WORKLIST_COLUMNS, WORKLIST_MAPPING

def read_all_csv(path, **kwargs):
    fns = get_all_fns(path, '*.csv')
    return pd.concat( [pd.read_csv(fn, **kwargs) for fn in fns] ) 


def read_all_parquet(path, **kwargs):
    fns = get_all_fns(path, '*.parquet')
    return pd.concat( [pd.read_csv(fn, **kwargs) for fn in fns] ) 


def get_all_fns(path, pattern='*.*', recursive=True):
    pattern = str( P(path)/'**'/f'{pattern}' )
    print(pattern)
    return glob(pattern, recursive=recursive)    


def format_morph(df):
    mapping = {'MATRIX_BOX': 'PLATE',
               'MATRIX_LOCN': 'PLATE_LOCN',
               'M_LOCN': 'PLATE_LOCN',
               'ORGM': 'ORGANISM',
               'C_Morph': 'C_MORPH',
               'C_Other': 'C_OTHER',
               'DATE': 'DATE_FROZEN',
               'DATE Frozen': 'DATE_FROZEN'
               }
    df = df.rename(columns=mapping)
    if 'PLATE_LOCN' in df.columns:
        df['PLATE_ROW'] = df.PLATE_LOCN.apply(lambda x: x[0])
        df['PLATE_COL'] = df.PLATE_LOCN.apply(lambda x: x.split(',')[1])
    cols = ['DATE_FROZEN', 'PLATE', 'PLATE_ROW', 'PLATE_COL', 
            'ORGANISM', 'ISOLATE_NBR', 'C_MORPH', 'C_BH', 'C_OTHER']
    df['PLATE'] = df['PLATE'].str.replace('LSARP_', '')
    try:
        df = df[cols]
    except:
        print(df.columns)
        df = df[cols]
    return df    


def format_shipment(df):
    mapping = {'DATE shipped': 'DATE_SHIPPED',
               'LSARP_PLATE': 'PLATE',
               'LSARP_LOCN': 'PLATE_LOCN'
              }
    df = df.rename(columns=mapping)
    if 'PLATE_LOCN' in df.columns:
        df['PLATE_ROW'] = df.PLATE_LOCN.apply(lambda x: x[0])
        df['PLATE_COL'] = df.PLATE_LOCN.apply(lambda x: x.split(',')[1])
    cols = ['DATE_SHIPPED', 'PLATE', 'PLATE_ROW', 'PLATE_COL', 'ORGANISM', 'ISOLATE_NBR']
    df['PLATE'] = df['PLATE'].str.replace('LSARP_', '')
    return df[cols]


def check_shipment(df):
    columns_expected = ['DATE shipped', 'LSARP_PLATE', 'LSARP_LOCN', 'ORGANISM', 'ISOLATE_NBR']
    df[columns_expected]
    vc = df['LSARP_PLATE'].value_counts()
    if not vc.max() == 1:
            assert len(vc) == 1, f'More than one plate ids:\n {vc}'


def get_plate_id_from_shipments_data(df):
    plate_id = df.loc[0, 'LSARP_PLATE'].replace('LSARP_', '')
    assert plate_id is not None, df
    return plate_id


def read_metabolomics_worklist(fn):
    fn = P(fn)
    assert fn.suffix == '.csv'
    df = pd.read_csv(fn, skiprows=1)[WORKLIST_COLUMNS]
    return df


def standardize_metabolomics_worklist(df, extract_metadata_from_filename=True):
    df = df.copy()
    cols = list( WORKLIST_MAPPING.values() )
    df = df.rename(columns=WORKLIST_MAPPING)[cols]
    df['PLATE_ROW'] = df['PLATE_POS'].apply(lambda x: x.split(':')[1][0])
    df['PLATE_COL'] = df['PLATE_POS'].apply(lambda x: ''.join(x.split(':')[1][1:]) )
    df['PLATE_COL'] = df['PLATE_COL'].apply(lambda x: f'{int(x):02.0f}')
    df['METHOD_FILE'] = df['METHOD_FILE'].apply(lambda x: PureWindowsPath(x).name )
    del df['PLATE_POS']

    if extract_metadata_from_filename:
        meta_data = pd.concat([metadata_from_filename(fn) for fn in df.MS_FILE])
        df = pd.merge(df, meta_data, on='MS_FILE')

    df = df[df.PLATE_ID.notna()]
    df = df.sort_values(['PLATE_ID', 'MS_FILE'])
    return df


def check_metabolomics_worklist(df):
    if 'MS_FILE' in df.columns:
        vc = df.MS_FILE.value_counts()
    elif 'File Name' in df.columns:
        vc = df['File Name'].value_counts()
    if not all( vc == 1 ):
        vc = vc[vc>1]
        logging.warning(f'Found {len(vc.index)} duplicated file entries. {vc}')


