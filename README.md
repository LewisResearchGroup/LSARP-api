# LSARP api

This is supposed to run on ARC. All the default file paths are set to locations on arc.

### Dependencies

Depends on `lrg-omics` package [link](https://github.com/LSARP/lrg-omics).

## Examples


### Get shipments data

    from lsarp import LSARP
    lsarp = LSARP()
    lsarp.load_shipments()
    shipments = lsarp.shipments


|    | PLATE_SETUP   | PLATE_ROW   |   PLATE_COL | DATE_SHIPPED        | PLATE   | RPT   | ORGANISM   | ISOLATE_NBR   | BI_NBR     | ORGANISM_ORIG   | SHIPMENT_FILE                      |\n|---:|:--------------|:------------|------------:|:--------------------|:--------|:------|:-----------|:--------------|:-----------|:----------------|:-----------------------------------|\n|  0 | EC001         | A           |          01 | 2021-11-15 00:00:00 | EC001   | R0    | EC         | ATCC_25922    |            | EC              | EC001_20211115_LSARP_Shipment.xlsx |\n|  1 | EC001         | A           |          02 | 2021-11-15 00:00:00 | EC001   | R0    | EC         | BI_10_0005    | BI_10_0005 | EC              | EC001_20211115_LSARP_Shipment.xlsx |\n|  2 | EC001         | A           |          03 | 2021-11-15 00:00:00 | EC001   | R0    | EC         | BI_10_0006    | BI_10_0006 | EC              | EC001_20211115_LSARP_Shipment.xlsx |\n|  3 | EC001         | A           |          04 | 2021-11-15 00:00:00 | EC001   | R0    | EC         | BI_10_0007    | BI_10_0007 | EC              | EC001_20211115_LSARP_Shipment.xlsx |\n|  4 | EC001         | A           |          05 | 2021-11-15 00:00:00 | EC001   | R0    | EC         | BI_10_0008    | BI_10_0008 | EC              | EC001_20211115_LSARP_Shipment.xlsx |\n|  5 | EC001         | A           |          06 | 2021-11-15 00:00:00 | EC001   | R0    | EC         | BI_10_0013    | BI_10_0013 | EC              | EC001_20211115_LSARP_Shipment.xlsx |\n|  6 | EC001         | A           |          07 | 2021-11-15 00:00:00 | EC001   | R0    | EC         | BI_10_0014    | BI_10_0014 | EC              | EC001_20211115_LSARP_Shipment.xlsx |\n|  7 | EC001         | A           |          08 | 2021-11-15 00:00:00 | EC001   | R0    | EC         | BI_10_0016    | BI_10_0016 | EC              | EC001_20211115_LSARP_Shipment.xlsx |\n|  8 | EC001         | A           |          09 | 2021-11-15 00:00:00 | EC001   | R0    | EC         | BI_10_0017    | BI_10_0017 | EC              | EC001_20211115_LSARP_Shipment.xlsx |\n|  9 | EC001         | A           |          10 | 2021-11-15 00:00:00 | EC001   | R0    | EC         | BI_10_0020    | BI_10_0020 | EC              | EC001_20211115_LSARP_Shipment.xlsx |
