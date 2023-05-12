import pandas as pd

from tqdm import tqdm

from pathlib import Path as P

from .engines import get_engine

from . import tools as T

from .standards import MINT_MAPPING


class MetabolomicsWorklist:
    def __init__(
        self,
        path,
        engine,
        groupby="PLATE_ID",
        drop_duplicates=True,
        remove_files=[".*AIF.*", ".*Sugar.*", ".*lank.*"],
    ):

        self._path = path
        self.engine = get_engine(engine)(path=path)
        self.get = self.engine.get
        self._grps = None
        self._groupby = groupby
        self._drop_duplicates = drop_duplicates
        self._remove_files = remove_files

    def create(self, fn=None):
        fn = P(fn)
        groupby = self._groupby
        self.read(fn).check().format().remove_files().split(groupby=groupby).put()

    def read(self, fn):
        fn = P(fn)
        assert fn.suffix == ".csv", fn
        self._df = T.read_metabolomics_worklist(fn)
        return self

    def format(self):
        df = self._df
        df = T.standardize_metabolomics_worklist(df)
        if self._drop_duplicates:
            df = df.drop_duplicates()
        self._df = df
        return self

    def remove_files(self):
        df = self._df
        for pattern in self._remove_files:
            df = df[~df["MS_FILE"].str.contains(pattern)]
        self._df = df
        return self

    def check(self):
        df = self._df
        T.check_metabolomics_worklist(df)
        return self

    def split(self, groupby):
        df = self._df
        self._grps = df.groupby(groupby)
        return self

    def put(self):
        for Id, df in tqdm(self._grps):
            self.engine.put(Id, df)


class MintResults:
    def __init__(self, path, engine, groupby="MS_FILE", colname_mapping=MINT_MAPPING):
        self._path = path
        self.engine = get_engine(engine)(path=path)
        self.get = self.engine.get
        self._grps = None
        self._groupby = groupby
        self._colname_mapping = colname_mapping

    def create(self, fn=None):
        fn = P(fn)
        groupby = self._groupby
        self.read(fn).format().split(groupby=groupby).put()

    def read(self, fn):
        fn = P(fn)
        assert fn.suffix == ".csv", fn
        self._df = pd.read_csv(fn)
        return self

    def format(self):
        df = self._df
        df.rename(columns=self._colname_mapping, inplace=True)
        df["MS_FILE"] = df["MS_FILE"].apply(lambda x: str(P(x).with_suffix("")))
        return self

    def split(self, groupby):
        df = self._df
        self._grps = df.groupby(groupby)
        return self

    def put(self):
        for Id, df in tqdm(self._grps):
            self.engine.put(Id, df)

    def crosstab(self, col="peak_max", kind="dask"):
        ddf = self.get(kind="dask")
        ddf = ddf.categorize(columns=["peak_label"]).pivot_table(
            index="MS_FILE", columns="peak_label", values=col
        )
        if kind == "dask":
            return ddf
        elif kind == "df":
            return ddf.compute()
