import os
import pandas as pd

from pathlib import Path as P
from glob import glob
from dask import dataframe as dd


class Engine():

    def __init__(self, path):
        self.path = path
        self.read_func = None

        def put(Id, df):
            pass

        def get(Id):
            pass


class FileEngine(Engine):
    def __init__(self, path):
        super().__init__(path)
        self._suffix = None

    def get_ids(self):
        ids = glob(f'{self.path}/*')

    def fns_from_ids(self, ids=None):
        if ids is None:
            ids = self.get_all_ids()
        elif isinstance(ids, str): ids = [ids]
        fns = [self.fn(Id) for Id in ids]
        return fns

    def fn(self, Id):
        fn = (P(self.path)/Id/Id).with_suffix(self._suffix)
        return fn

    def get_all_ids(self):
        ids = map( os.path.basename, glob(f'{self.path/"*"}') )
        return [i for i in ids]

    def get(self, ids=None, kind='df'):
        fns = self.fns_from_ids(ids)
        if kind == 'fns': return fns      
        ddf = self._read_func( fns ).reset_index(drop=True)
        if kind == 'dask': return ddf
        if kind == 'df': return ddf.compute()



class Parquet(FileEngine):

    def __init__(self, path):
        super().__init__(path)
        self._suffix = '.parquet'
        self._format = 'parquet'
        self._read_func = self._read

    def put(self, Id, df):
        fn = self.fn( Id )
        os.makedirs(fn.parent, exist_ok=True)
        df.to_parquet( fn )
    
    @classmethod
    def _read(self, fns):
        return dd.read_parquet( fns, engine='pyarrow')


class CSV(FileEngine):

    def __init__(self, path):
        super().__init__(path)
        self._suffix = '.csv'
        self._format = 'csv'
        self._read = dd.read_csv

    def put(self, Id, df):
        fn = self.fn(Id)
        os.makedirs(fn.parent, exist_ok=True)
        df.to_csv( fn )
    



class SQLite(Engine):
    pass


def get_engine(use):
    if use == 'csv': 
        return CSV
    if use == 'parquet':
        return Parquet
