import time
from typing import Optional


from fastapi import FastAPI, APIRouter, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import httpx
import yaml

from utils.csw import csw_search
from utils.parameter_mapping import map_params

# # Konfiguration: Default-Werte für Collection und Provider-URL
# DEFAULT_COLLECTION = "landsat-c2l2-sr"
# PROVIDER_SEARCH_URL = "https://landsatlook.usgs.gov/stac-server/search"
#
# DEFAULT_COLLECTION = "sentinel-2-l2a"
# PROVIDER_SEARCH_URL = "https://stac.dataspace.copernicus.eu/v1/search"


# FastAPI-App erstellen
app = FastAPI(
    title="Custom STAC Gateway",
    description="Ein minimaler STAC-Search-Gateway-Prototyp mit nur einem /search-Endpunkt",
    version="0.1.0"
)

# # CORS konfigurieren (optional)
# origins = ["*"]  # Anpassen auf erlaubte Domains
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# API-Router und Endpunkte
router = APIRouter()


# Konfiguration laden
with open("providers.yml", "r", encoding="utf-8") as f:
    providers_config = yaml.safe_load(f)
# with open("collections.yml", "r", encoding="utf-8") as f:
#     collections_config = yaml.safe_load(f)

# STAC Core: Root Catalog
@router.get("/", include_in_schema=True)
async def root(request: Request):
    base = str(request.base_url).rstrip("/")
    catalog = {
        "stac_version": "1.0.0",
        "type": "Catalog",
        "id": "custom-stac-gateway",
        "title": "Custom STAC Gateway",
        "description": "Ein minimaler STAC-Search-Gateway-Prototyp",
        "conformsTo": [
            "https://api.stacspec.org/v1.0.0/core",
            "https://api.stacspec.org/v1.0.0/item-search"
        ],
        "links": [
            {"rel": "self", "href": base, "type": "application/json"},
            {"rel": "conformance", "href": f"{base}/conformance", "type": "application/json"},
            {"rel": "data", "href": f"{base}/collections", "type": "application/json"},
            {"rel": "search", "href": f"{base}/search", "type": "application/geo+json", "method": "GET"},
            {"rel": "search", "href": f"{base}/search", "type": "application/geo+json", "method": "POST"},
        ]
    }
    return JSONResponse(content=catalog)

# STAC Core: Conformance
@router.get("/conformance", include_in_schema=True)
async def conformance():
    return JSONResponse(content={
        "conformsTo": [
            "https://api.stacspec.org/v1.0.0/core",
            "https://api.stacspec.org/v1.0.0/item-search"
        ]
    })

# # /collections aus YAML
# @app.get("/collections", tags=["Collections"])
# def get_collections():
#     """
#     Gibt alle Provider und ihre Collections zurück, wie in collections.yml definiert.
#     """
#     return JSONResponse(collections_config)

@router.get("/search", include_in_schema=True)
async def search(request: Request,
                provider: Optional[str] = None,
                collections: Optional[str] = None,
                ids: Optional[str] = None,
                bbox: Optional[str] = None,
                datetime: Optional[str] = None,
                limit: Optional[int] = None,
                query: Optional[str] = None,
                page: Optional[int] = None,
                sortby: Optional[str] = None,):

    # Parameter aus Anfrage extrahieren
    params = dict(request.query_params)
    print("GET")
    # print(params)

    # Hardcodiertes Mapping: Standard-Collection, falls nicht angegeben
    if collections is None:
        pass
    else:
        #for provider in providers_config:
        params["collections"] = collections

        # Anfrage an den externen STAC-Provider weiterleiten
        async with httpx.AsyncClient(timeout=10.0) as client:
            # hardcode provider
            provider_url = providers_config.get(provider).get("search_url")
            response = await client.get(provider_url, params=params)

    # Rückgabe der Antwort im STAC-JSON-Format
    content = None
    try:
        content = response.json()
    except ValueError:
        content = {"error": "Ungültige JSON-Antwort vom Provider"}
    return JSONResponse(status_code=response.status_code, content=content)


# STAC Item Search: POST /search -> forward JSON body
@router.post("/search", include_in_schema=True)
async def search_post(request: Request):
    payload = await request.json()
    print("POST")

    # if "collections" not in payload:
    #     payload["collections"] = [...]

    for prov in providers_config:
        print(payload["collections"][0])
        print(providers_config[prov].get("products"))
        if payload["collections"][0] in providers_config[prov].get("products"):
            provider = prov

    provider_url = providers_config.get(provider).get("search_url")
    print(provider_url)
    print(f"Vor: {payload}")
    # Umwandeln in Provider-Parameternamen
    provider_params = map_params(payload, providers_config.get(provider))
    print(f"Danach: {provider_params}")

    if providers_config.get(provider).get("type") == "csw":
        response = await csw_search(provider_url, provider_params)
        print("Test")
    else:
        async with httpx.AsyncClient(timeout=10.0) as client:
            #print(f"Provider-url: {provider_url}")
            #print(f"Payload: {payload}")
            response = await client.post(provider_url, json=provider_params)

    try:
        content = response.json()
    except ValueError:
        content = {"error": "Ungültige JSON-Antwort vom Provider"}
    return JSONResponse(status_code=response.status_code, content=content)


app.include_router(router, prefix="")

# Entrypoint für Entwicklung
# app = create_app()

if __name__ == "__main__":

    uvicorn.run(
        "main_gateway:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
