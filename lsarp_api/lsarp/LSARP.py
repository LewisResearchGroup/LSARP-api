import pandas as pd
from . import tools as T
import logging

PATHS = {
    "shipments": "/bulk/LSARP/lrg-proc/LSARP/1_Raw_data/Plate-Register/APL-Shipments/Shipments/"
}


class LSARP:
    def __init__(
        self,
        path_shipments=PATHS["shipments"],
    ):
        self.path_shipments = path_shipments
        self.shipments = None
        self.organisms = None
        self.drugs = None
        self.mappings = {}

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
