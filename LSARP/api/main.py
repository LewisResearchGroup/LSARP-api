import io

from fastapi import FastAPI, File, UploadFile

from ..LSARP import LSARP

app = FastAPI()

lsarp = LSARP(path='/data/test-lsarp-api', engine='parquet')


def parse_content(fn, content):
    if fn.lower().endswith('.csv'):
        print('Parse as CSV file')
        content = io.StringIO(content.decode('utf-8'))
    return content


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/shipments/put")
async def shipments_put(file: UploadFile = File(...)):
    content = await file.read()
    fn = file.filename
    content = parse_content(fn, content)
    message = lsarp.shipments.create(content)
    return {"filename": fn, "message": message}


@app.post("/shipments/get")
async def shipments_get():
    json = lsarp.shipments.get().to_json()
    return json

@app.post("/proteomics/plates/put")
async def proteomics_plates_put(file: UploadFile = File(...)):
    content = await file.read()
    fn = file.filename
    content = parse_content(fn, content)
    message = lsarp.plex_data.create(content)
    return {"filename": file.filename, "message": message}


@app.post("/proteomics/plates/get")
async def proteomics_plates_get():
    json = lsarp.plex_data.get().to_json()
    return json

@app.post("/proteomics/maxquant/protein-groups/put")
async def protein_groups_put(file: UploadFile = File(...)):
    content = await file.read()
    fn = file.filename
    content = parse_content(fn, content)
    message = lsarp.protein_groups.create(content)
    return {"filename": file.filename, "message": message}


@app.post("/proteomics/maxquant/protein-groups/get")
async def protein_groups_get():
    json = lsarp.protein_groups.get().to_json()
    return json