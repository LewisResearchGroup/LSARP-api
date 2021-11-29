
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
        self.read(fn).format().check().put()


    def read(self, fn):
        self._df = pd.read_excel(fn)
        return self


    def check(self):
        T.check_shipment(self._df)
        return self


    def format(self):
        self._df = T.format_shipment(self._df)
        self._plate_id = T.get_plate_id_from_shipments_data(self._df)
        return self


    def put(self, fn=None,):
        df = self._df
        Id = self._plate_id
        assert Id is not None, Id
        self.engine.put(Id, df)  


    def get_plate_setups(self):
        shipments = self.get()
        plate_setups = shipments[['PLATE_SETUP', 'PLATE_ROW', 'PLATE_COL', 'ISOLATE_NBR']]\
                            .drop_duplicates()\
                            .groupby(['PLATE_SETUP','PLATE_ROW', 'PLATE_COL'])\
                            .first()\
                            .reset_index()
        return plate_setups






        
