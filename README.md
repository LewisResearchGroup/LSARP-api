# LSARP api


### Dependencies

Depends on `lrg-omics` package [link](https://github.com/LSARP/lrg-omics).


### Run the API (fast-API)
To start the api you have to serve the app e.g. with `uvicorn`:

    uvicorn LSARP.api.main:app --reload

Then go to http://localhost:8000/docs to test the API.

### In IPython/Jupyter

In the juypter notebook:

    from LSARP import LSARP

    lsarp = LSARP(path='/data/test-lsarp-api', engine='parquet')

Will save the ingested data to `/data/test-lsarp-api`. 



    lsarp.metabolomics_worklist.create(fn_worklist) 
    lsarp.metabolomics_mint.create(fn_mint_results)
    lsarp.protein_groups.create(fn_protein_groups)
    
    lsarp.metabolomics_worklist.get()
    lsarp.metabolomics_mint.get()
    lsarp.protein_groups.get()
    
    

### Notes

I am currently reimplementing the old code:


```
Init signature:
LSARP(
    path='/home/swacker/data/LSARP/DB/data/formated', 
    engine='csv'  # Currently accepts 'csv', and 'parquet'
    )
Docstring:      <no docstring>
Source:        
class LSARP():
    
    def __init__(self, path='/home/swacker/data/LSARP/DB/data/formated',
                       maxquant_results_src='/home/swacker/data/LSARP/DB/data/proteomics/MaxQuant/',
                       engine='csv'):

        self._path = P(path)
        self._sub_paths = {'shipment': 'shipments',
                           'morphology': 'morphology',
                           'tmt11-plex': 'tmt11-plex',
                           'maxquant': 'maxquant',
                           'apl': 'APL',
                           'metabolomics': 'metabolomics'
                          }
        
        self._maxquant_results_src = maxquant_results_src

        self.shipments  = Shipments( path=self.path/engine/'shipments', engine=engine )
        self.protein_groups = MaxQuant( path=self.path/engine/'maxquant'/'protein_groups', engine=engine)
        self.plex_data = PlexData( path=self.path/engine/'plates'/'proteomics', engine=engine)
        self.metabolomics_worklist = MetabolomicsWorklist( path=self.path/engine/'metabolomics', engine=engine)

### This is the new code, all below is outdated.

```


# Examples


### Get metabolomics data tables with dask.

```
import dask.dataframe as dd


df = lsarp.metabolomics_mint.crosstab('peak_max', kind='dask')
wl = lsarp.metabolomics_worklist.get(kind='dask')

metabolomics_data = wl.merge(df, left_on='MS_FILE', right_index=True)

%%time
metab_neg, metab_pos = dd.compute( metabolomics_data[metabolomics_data.MS_MODE == 'Neg'], 
                                   metabolomics_data[metabolomics_data.MS_MODE == 'Pos'] )
```                              
