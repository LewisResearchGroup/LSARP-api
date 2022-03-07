import pandas as pd

from pathlib import Path as P

MAXQUANT_STANDARDS = {
    "proteinGroups.txt": {
        "usecols": [
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            26,
            27,
            28,
            29,
            30,
            31,
            55,
            57,
            58,
            59,
            60,
            61,
            62,
            63,
            64,
            65,
            66,
            67,
            68,
            69,
            70,
        ],
        "column_names": [
            "Protein IDs",
            "Majority protein IDs",
            "Peptide counts (all)",
            "Peptide counts (razor+unique)",
            "Peptide counts (unique)",
            "Fasta headers",
            "Number of proteins",
            "Peptides",
            "Razor + unique peptides",
            "Unique peptides",
            "Sequence coverage [%]",
            "Unique + razor sequence coverage [%]",
            "Unique sequence coverage [%]",
            "Mol. weight [kDa]",
            "Sequence length",
            "Sequence lengths",
            "Q-value",
            "Score",
            "Reporter intensity corrected 1",
            "Reporter intensity corrected 2",
            "Reporter intensity corrected 3",
            "Reporter intensity corrected 4",
            "Reporter intensity corrected 5",
            "Reporter intensity corrected 6",
            "Reporter intensity corrected 7",
            "Reporter intensity corrected 8",
            "Reporter intensity corrected 9",
            "Reporter intensity corrected 10",
            "Reporter intensity corrected 11",
            "Intensity",
            "MS/MS count",
            "Only identified by site",
            "Reverse",
            "Potential contaminant",
            "id",
            "Peptide IDs",
            "Peptide is razor",
            "Mod. peptide IDs",
            "Evidence IDs",
            "MS/MS IDs",
            "Best MS/MS",
            "Oxidation (M) site IDs",
            "Oxidation (M) site positions",
            "Taxonomy IDs",
        ],
    }
}


class MaxQuantReader:
    def __init__(self, standardize=True):
        self.standards = MAXQUANT_STANDARDS
        self.standardize = standardize

    def read(self, fn):
        name = P(fn).name
        usecols = None
        columns = None
        if self.standardize and (name in self.standards):
            std_data = self.standards[name]
            usecols = std_data["usecols"]
            columns = std_data["column_names"]
        df = pd.read_csv(
            fn, sep="\t", usecols=usecols, low_memory=False, na_filter=None
        )
        if columns is not None:
            df.columns = columns
        return df
