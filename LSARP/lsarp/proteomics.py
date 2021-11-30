import urllib.request
import requests

import pandas as pd

from tqdm import tqdm

from .engines import get_engine


class MaxQuant:
    def __init__(self, path, engine):
        self._path = path
        self.engine = get_engine(engine)(path=path)
        self.get = self.engine.get
        self._grps = None

    def create(self, fn=None):
        self.read(fn).check().split().put()

    def read(self, fn):
        self._df = pd.read_csv(fn)
        return self

    def check(self):
        df = self._df
        assert "RawFile" in self._df.columns
        return self

    def split(self):
        df = self._df
        self._grps = df.groupby("RawFile")
        return self

    def put(self):
        for Id, df in tqdm(self._grps):
            self.engine.put(Id, df)


class PlexData:
    def __init__(self, path, engine):
        self._path = path
        self.engine = get_engine(engine)(path=path)
        self.get = self.engine.get
        self._grps = None
        self._expected_input_cols = [
            "RawFile",
            "Date",
            "Bug / Prep Method / seq name",
            "Peptide Sample Name",
            "Injection volume (ul)",
            "MS method",
            "Column used",
            "Nr. Runs on Column",
            "F / NF",
            "Data folder",
            "notes",
            "CHECK",
            "TIC",
            "MQ proteins",
            "MS1 ",
            "MS2 ",
            "MS3",
            "Peptides",
            "Peptides per Protein",
        ]

    def create(self, fn=None):
        self.read(fn).check().format().split().put()

    def read(self, fn):
        self._df = pd.read_excel(fn)
        return self

    def format(self):
        df = self._df
        df["PLATE"] = df.RawFile.apply(lambda x: x[:5])
        df["RPT"] = df.RawFile.apply(lambda x: x[6:8])
        df["PLATE_ROW"] = df.RawFile.apply(lambda x: x.split("-")[2]).replace(
            "blank", ""
        )
        df = df.set_index(["PLATE", "RPT", "PLATE_ROW"]).sort_index().reset_index()
        self._df = df
        return self

    def check(self):
        expected = tuple(self._expected_input_cols)
        cols = tuple(self._df.columns)
        assert cols == expected, f"Expected columns: {expected}"
        return self

    def split(self):
        df = self._df
        self._grps = df.groupby(["PLATE", "RPT"])
        return self

    def put(self):
        for (Id, rpt), df in tqdm(self._grps):
            self.engine.put(f"{Id}-{rpt}", df)
