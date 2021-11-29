import os

import logging

import pandas as pd

from pathlib import Path as P

from distributed import Client
from dask.cache import Cache

try:
    cache = Cache(2e9)  # Leverage two gigabytes of memory
    cache.register() 
except:
    logging.warning('No cache registered.')

from . import tools as T
from .MaxQuantReader import MaxQuantReader

from .Shipments import Shipments
from .proteomics import MaxQuant, PlexData
from .metabolomics import MetabolomicsWorklist, MintResults


class LSARP():
    
    def __init__(self, path='/home/swacker/data/LSARP/DB/data/formated',
                       maxquant_results_src='/home/swacker/data/LSARP/DB/data/proteomics/MaxQuant/',
                       engine='csv'):

        self._path = P(path)
        self._sub_paths = {'shipment': 'shipments',
                           'morphology': 'morphology',
                           'tmt11-plex': 'tmt11-plex',
                           'maxquant': 'maxquant',
                           'apl': 'APL',
                           'metabolomics': 'metabolomics'
                          }
        
        self._maxquant_results_src = maxquant_results_src
        self.shipments  = Shipments( path=self.path/engine/'shipments', engine=engine )
        self.protein_groups = MaxQuant( path=self.path/engine/'maxquant'/'protein_groups', engine=engine)
        self.plex_data = PlexData( path=self.path/engine/'plates'/'proteomics', engine=engine)
        self.metabolomics_worklist = MetabolomicsWorklist( path=self.path/engine/'metabolomics'/'plates', engine=engine)
        self.metabolomics_mint = MintResults( path=self.path/engine/'metabolomics/mint', engine=engine)


    @property
    def path(self):
        return self._path

    def add_shipment(self, fn):
        self.shipments.create(fn)


    def add_morphology(self, fn, plate):
        fn_new = self.get_morphology_fn(plate=plate, create=True)
        df = pd.read_excel(fn)
        T.format_morph(df).to_csv(fn_new, index=False)
        assert P(fn_new).is_file()        
        
        
    def add_apl_results(self):
        path = self.get_path('apl', create=True)
        fn = self.get_apl_fn('results')
        cls.df_results.to_parquet(fn)
        
        
    def get_apl_fn(self, kind):
        path = self.get_path('apl') / f'{kind}.parquet'
        return path
    
    
    def apl_results(self, index=['BI_NBR']):
        fn = self.get_apl_fn('results')
        df = pd.read_parquet(fn)
        return df
        
        
    def apl_interp(self):
        df = self.apl_results()
        col_ndx = df.columns.get_level_values(3) == 'INTERP'
        df = df.loc[:, col_ndx]
        df.columns = df.columns.get_level_values('ANTIBIOTIC')
        df.index = df.index.get_level_values('BI_NBR')
        return df

    
    def get_path(self, kind=None, create=False):
        if kind is None and plate is None: 
            path = self._path
        else:
            path = self._path / self._sub_paths[kind]
        if create: os.makedirs(path, exist_ok=True)
        return path
    

    def get_morphology_fn(self, plate, create=False):
        return self.get_path(kind='morphology', create=create) / f'{plate}.csv'   
        
        
    def plex_data(self):
        path = self.get_path(kind='tmt11-plex')
        fns = glob(str(path/'*csv'))
        return pd.concat( [pd.read_csv(fn) for fn in fns] ).reset_index(drop=True)


    def morphologies(self):
        df = T.read_all_csv( self.get_path(kind='morphology') )
        return df.sort_values(['PLATE', 'PLATE_ROW', 'PLATE_COL']).reset_index(drop=True)
    
    
    def add_maxquant(self, path, plate, row, raw_file=None):
        _new_path = self.get_path(kind='maxquant', create=True) / plate 
        new_path = _new_path / row
        if new_path.is_dir():
            logging.info(new_path, 'exists')
            return None
        
        expected_files = [
            'allPeptides.txt',
            'modificationSpecificPeptides.txt',
            'msmsScans.txt',  
            'Oxidation (M)Sites.txt',
            'peptides.txt',
            'summary.txt',
            'evidence.txt',
            'msms.txt',
            'mzRange.txt',
            'parameters.txt',
            'proteinGroups.txt']
        
        for fn in expected_files:
            abs_fn = P(path)/fn
            if not abs_fn.is_file():
                logging.info('File not found', abs_fn)
                return None
            
        logging.info('Add maxquant', path, plate, row)
        
        os.makedirs(new_path)
        
        for fn in expected_files:
            fn_in = P(path)/fn
            fn_out = (P(new_path)/fn).with_suffix('.parquet')
            try:
                df = MaxQuantReader().read(fn_in)
                df.assign(PLATE=plate, PLATE_ROW=row, RawFile=raw_file).set_index(['RawFile', 'PLATE', 'PLATE_ROW']).reset_index().to_parquet(fn_out)
            except Exception as e:
                logging.error(f'Parquet conversion failed for {fn}: \n {e}')
                shutil.rmtree(new_path)
                break

                
    def add_maxquant_according_to_plex_data(self):
        plex = self.plex_data()
        for ndx, (plate, rpt, row, raw_file) in plex[['PLATE', 'RPT', 'PLATE_ROW', 'RawFile']].iterrows():
            if (row is None) or (row == '') or isinstance(row, float): continue
            path = P(self._maxquant_results_src) / raw_file
            if path.is_dir():
                self.add_maxquant(path, plate, row, raw_file=raw_file)
            else:
                logging.info('Missing', path)

                
    def get_maxquant_results_path(self, plate, row):
        return self.get_path(kind='maxquant') / plate / row

        
    def list_missing_maxquant(self):
        plex = self.plex_data()
        missing = []
        for ndx, (plate, rpt, row, raw_file) in plex[['PLATE', 'RPT', 'PLATE_ROW', 'RawFile']].iterrows():
            if (row is None) or (row == '') or isinstance(row, float): continue
            if not self.get_maxquant_results_path(plate, row).is_dir():
                missing.append(ndx)  
        df = plex.loc[missing][['PLATE', 'RPT', 'PLATE_ROW', 'RawFile']]
        df['MISSING_MAXQUANT'] = True
        
        
    def get_protein_groups_fns(self):
        path = self.get_path('maxquant')
        fns = T.get_all_fns(path, 'proteinGroups*')
        return fns
        
        
    def protein_groups(self):
        fns = self.get_protein_groups_fns()
        return pd.concat([ pd.read_parquet(fn) for fn in fns]).sort_values(['PLATE', 'PLATE_ROW']).reset_index(drop=True)

    
    def add_plex_data(self):
        '''Needs to be refactored'''
        df = pd.read_excel('../data/proteomics/plates/LSARP-TMT11-runs.xlsx')
        df['PLATE'] = df.RawFile.apply(lambda x: x[:5])
        df['RPT'] = df.RawFile.apply(lambda x: x[6:8])
        df['PLATE_ROW'] = df.RawFile.apply(lambda x: x.split('-')[2]).replace('blank', '')
        df = df.set_index(['PLATE', 'RPT', 'PLATE_ROW']).sort_index().reset_index()
        grps = df.groupby(['PLATE', 'RPT'])
        for (plate, rpt), grp in grps:
            fn_out = P(f'../data/formated/tmt11-plex/{plate}.csv')
            if not fn_out.is_file():
                grp.to_csv(fn_out, index=False)    

