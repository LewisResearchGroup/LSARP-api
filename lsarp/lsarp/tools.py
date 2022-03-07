from glob import glob

import pandas as pd
import logging
import re

from pathlib import Path as P
from pathlib import PureWindowsPath

from .standards import WORKLIST_COLUMNS, WORKLIST_MAPPING


def read_all_csv(path, **kwargs):
    fns = get_all_fns(path, "*.csv")
    return pd.concat([pd.read_csv(fn, **kwargs) for fn in fns])


def read_all_parquet(path, **kwargs):
    fns = get_all_fns(path, "*.parquet")
    return pd.concat([pd.read_csv(fn, **kwargs) for fn in fns])


def get_all_fns(path, pattern="*.*", recursive=True):
    pattern = str(P(path) / "**" / f"{pattern}")
    print(pattern)
    return glob(pattern, recursive=recursive)


def format_morph(df):
    mapping = {
        "MATRIX_BOX": "PLATE",
        "MATRIX_LOCN": "PLATE_LOCN",
        "LSARP_BOX": "PLATE",
        "LSARP_LOCN": "PLATE_LOCN",
        "M_LOCN": "PLATE_LOCN",
        "ORGM": "ORGANISM",
        "C_Morph": "C_MORPH",
        "C_Other": "C_OTHER",
    }
    df = df.rename(columns=mapping)
    if "PLATE_LOCN" in df.columns:
        df["PLATE_ROW"] = df.PLATE_LOCN.apply(lambda x: x[0])
        df["PLATE_COL"] = df.PLATE_LOCN.apply(lambda x: x.split(",")[1])

    cols = [
        "PLATE",
        "PLATE_ROW",
        "PLATE_COL",
        "ORGANISM",
        "ISOLATE_NBR",
        "C_MORPH",
        "C_BH",
        "C_OTHER",
    ]
    df["PLATE"] = df["PLATE"].str.replace("LSARP_", "")
    try:
        df = df[cols]
    except:
        print(df.columns)
        df = df[cols]
    return df

remove_digits = lambda x: ''.join([i for i in x if not i.isdigit()])


def format_shipment(df):
    mapping = {
        "DATE shipped": "DATE_SHIPPED",
        "LSARP_PLATE": "PLATE",
        "LSARP_LOCN": "PLATE_LOCN",
    }

    df = df.rename(columns=mapping, errors="ignore")
    df = df[df.ORGANISM.notna()]
    df = df[df.DATE_SHIPPED.notna()]

    df["PLATE"] = df["PLATE"].str.replace("LSARP_", "")

    if "PLATE_LOCN" in df.columns:
        try:
            df["PLATE_ROW"] = df.PLATE_LOCN.apply(lambda x: x[0])
        except:
            print(df)
        df["PLATE_COL"] = df.PLATE_LOCN.apply(lambda x: x.split(",")[1]).apply(
            lambda x: f"{int(x):02.0f}"
        )

        try:
            df["RPT"] = df.PLATE.str.replace('_RPT', '-R1').apply(lambda x: x.split("-")[1])
        except:
            df["RPT"] = "R0"
        df["PLATE_SETUP"] = df.PLATE.apply(lambda x: x.split("-")[0]).replace('_RPT', '')

    cols = [
        "DATE_SHIPPED",
        "PLATE",
        "PLATE_SETUP",
        "RPT",
        "PLATE_ROW",
        "PLATE_COL",
        "ORGANISM",
        "ISOLATE_NBR",
        "BI_NBR",
        "ORGANISM_ORIG"
    ]

    df['BI_NBR'] = [i  if i.startswith('BI') else None for i in df.ISOLATE_NBR]
    
    df["ORGANISM_ORIG"] = df["ORGANISM"].copy()
    df["ORGANISM"] = df["ORGANISM"].apply(standardize_organism)

    return df[cols]


def standardize_organism(x):
    x = str(x).replace('#', '')
    replacements = {
        'MSSA': 'SA',
        'MRSA': 'SA',
        'VRE faem': 'ENTFAEM',
        'EFAECALI': 'ENTFAES'  
    }
    x = remove_digits(x)
    x = replacements[x] if x in replacements.keys() else x
    return x

def read_shipment(fn):
    df = format_shipment(pd.read_excel(fn))
    df['SHIPMENT_FILE'] = P(fn).name
    return df

def get_all_shipments(path):
    fns = glob(str(P(path)/'*Shipment*xlsx'))
    fns = [i for i in fns if '$' not in i]
    shipments = pd.concat([read_shipment(fn) for fn in fns]).sort_values(['DATE_SHIPPED', 'PLATE', 'RPT', 'PLATE_COL', 'PLATE_ROW']).reset_index(drop=True)   
    return shipments     

def check_shipment(df):
    expected_cols = [
        "DATE_SHIPPED",
        "PLATE",
        "PLATE_SETUP",
        "RPT",
        "PLATE_ROW",
        "PLATE_COL",
        "ORGANISM",
        "ISOLATE_NBR",
    ]
    columns = df.columns.to_list()
    try:
        df = df[expected_cols]
    except:
        print(columns)
        df[expected_cols]

    vc = df["PLATE"].value_counts()
    if not vc.max() == 1:
        assert len(vc) == 1, f"More than one plate ids:\n {vc}"


def get_plate_id_from_shipments_data(df):
    plate_id = df.loc[0, "PLATE"].replace("LSARP_", "")
    assert plate_id is not None, df
    return plate_id


def read_metabolomics_worklist(fn):
    fn = P(fn)
    assert fn.suffix == ".csv"
    df = pd.read_csv(fn, skiprows=1)[WORKLIST_COLUMNS]
    return df


def standardize_metabolomics_worklist(df, extract_metadata_from_filename=True):
    df = df.copy()
    cols = list(WORKLIST_MAPPING.values())
    df = df.rename(columns=WORKLIST_MAPPING)[cols]
    df["PLATE_ROW"] = df["PLATE_POS"].apply(lambda x: x.split(":")[1][0])
    df["PLATE_COL"] = df["PLATE_POS"].apply(lambda x: "".join(x.split(":")[1][1:]))
    df["PLATE_COL"] = df["PLATE_COL"].apply(lambda x: f"{int(x):02.0f}")
    df["METHOD_FILE"] = df["METHOD_FILE"].apply(lambda x: PureWindowsPath(x).name)
    del df["PLATE_POS"]

    if extract_metadata_from_filename:
        meta_data = pd.concat([metadata_from_filename(fn) for fn in df.MS_FILE])
        df = pd.merge(df, meta_data, on="MS_FILE")

    df = df[df.PLATE_SETUP.notna()]
    df = df.sort_values(["PLATE_SETUP", "MS_FILE"])
    return df


def check_metabolomics_worklist(df):
    if "MS_FILE" in df.columns:
        vc = df.MS_FILE.value_counts()
    elif "File Name" in df.columns:
        vc = df["File Name"].value_counts()
    if not all(vc == 1):
        vc = vc[vc > 1]
        logging.warning(f"Found {len(vc.index)} duplicated file entries. {vc}")


def read_roary_presence_absence(fn, checks=True):
    """
    Reads and formats a presence/absence file generated with Roary.
    """
    df = pd.read_csv(fn, low_memory=False)
    df.Gene = df.Gene.astype(str)
    df.Annotation = df.Annotation.astype(str)
    format_cols = lambda x: re.sub(r"_MOCUDI_?", "", x).replace("-", "_")
    df.columns = [format_cols(c) for c in df.columns]
    df = df.set_index(["Gene", "Annotation"]).filter(regex="^BI_").notna()
    df.index = df.index.map("|".join)
    df = df.T
    df.index.name = "BI_NBR"
    df = df.sort_index()
    if checks:
        assert df.index.value_counts().max() == 1
        assert df.columns.value_counts().max()
        assert all(df.index.str.len() == 10)
    return df


def replace_interp(x):
    if not isinstance(x, list):
        return x
    if "R" in x:
        return "R"
    elif "I" in x:
        return "I"
    elif "S" in x:
        return "S"
    else:
        return np.NaN
