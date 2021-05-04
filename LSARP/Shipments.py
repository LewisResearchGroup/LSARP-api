
import logging
from pathlib import Path as P
from . import tools as T
from .engines import get_engine

import pandas as pd


class Shipments():

    def __init__(self, path, engine='parquet'):
        self._plate_id = None
        self._df = None
        self._df_formated = None
        self._path = P(path)
        self.engine = get_engine(engine)( path=self._path )
        self.get = self.engine.get


    def create(self, fn):
        self.read(fn).check().format().put()


    def read(self, fn):
        self._df = pd.read_excel(fn)
        return self


    def check(self):
        T.check_shipment(self._df)
        return self


    def format(self):
        self._df_formated = T.format_shipment(self._df)
        self._plate_id = T.get_plate_id_from_shipments_data(self._df)
        return self


    def put(self, fn=None,):
        df = self._df_formated
        Id = self._plate_id
        assert Id is not None, Id
        self.engine.put(Id, df)  



        

        






        
