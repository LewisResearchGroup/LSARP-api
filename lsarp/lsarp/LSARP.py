import pandas as pd
from . import tools as T
import logging

FNS = {
    'organisms': '/bulk/LSARP/datasets/LSARP-organisms/LSARP-organisms.csv',
    'drugs': '/bulk/LSARP/datasets/LSARP-drugs/LSARP-drugs.csv'
}


class LSARP:
    def __init__(self,
        path_shipments = "/bulk/LSARP/lrg-proc/Plate-Register/APL-Shipments/Shipments",
    ):
        self.path_shipments = path_shipments
        self.shipments = None        
        self.organisms = None
        self.drugs = None
        self.mappings = {}
        self.load_organisms()
        self.load_drugs()

    def load_shipments(self, last_repeat=True):
        self.shipments = T.get_all_shipments(self.path_shipments)
        if last_repeat:
            self.shipments = (
                self.shipments.sort_values("RPT")
                    .groupby(["PLATE_SETUP", "PLATE_ROW", "PLATE_COL"], as_index=False)
                    .last()
            )
            self.shipments = self.shipments.sort_values(
                ["PLATE_SETUP", "PLATE_ROW", "PLATE_COL"]
            )
        self.shipments = self.shipments.replace({'ORGANISM': self.mappings['organism']})
        unknowns = self.shipments[~self.shipments.ORGANISM.apply(lambda x: x in self.organisms.ORGANISM.to_list())]
        if len(unknowns) != 0:
            logging.warning(unknowns)

    def load_organisms(self):
        self.organisms = pd.read_csv(FNS['organisms'], na_filter=False)    
        mapping = {old: organism for _, (old, organism) in self.organisms[['OLD', 'ORGANISM']].iterrows() if old != organism}
        self.mappings['organism'] = mapping
 
    def load_drugs(self):
        self.drugs = pd.read_csv(FNS['drugs'], na_filter=False)           

