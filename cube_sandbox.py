import os
import webbrowser
from typing import Dict, Any

import rasterio._err

from creds import cdse, PROVIDERS
from creds import usgs as usgs_
import matplotlib
import matplotlib.pyplot as plt
import odc.stac as odc_stac
from odc.geo import geobox
from pystac_client import Client as pystacclient
# import dask
# from dask.distributed import Client
import folium
# import folium.plugins as folium_plugins
# import geopandas as gpd


######################################################################
def get_stac_items(catalog_url, collections, bbox=None):

    if bbox is None:
        bbox = [-105.0, 40.0, -104.0, 41.0]

    client = pystacclient.open(catalog_url)

    # Führe die Suche aus
    search = client.search(
        bbox=bbox,
        #bbox=[5.763545147601, 46.793670688196, 15.152753273727, 55.945635321533],
        max_items=2,
        #limit=1,
        collections=collections,
    )
    a = next(search.items())
    # print(a)
    # print(type(a))
    return list(search.items())

######################################################################

def get_odc(stac_items, bands):

    try:
        cube = odc_stac.load(
            stac_items,
            bands=bands,
            resolution=20,
            #stac_cfg=stac_cfg,
            #groupby='solar_day',
            #chunks={'x': 1024, 'y': 1024}, # spatial chunking
            anchor=geobox.AnchorEnum.FLOATING # preserve original pixel grid
        )
    except ValueError as e:
        cube = odc_stac.load(
            stac_items,
            bands=bands,
            crs="EPSG:4326",
            resolution=0.00036,
            #stac_cfg=stac_cfg,
            #groupby='solar_day',
            chunks={'x': 1024, 'y': 1024}, # spatial chunking
            anchor=geobox.AnchorEnum.FLOATING # preserve original pixel grid
        )



    return cube


def show_bboxes(items):
    # Erstelle Karte
    fmap = folium.Map(location=[0, 0], tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
                      attr="Esri", name="Esri Satellite", zoom_start=2)
    style = {'fillColor': '#00000000', "color": "#0000ff", "weight": 1}

    # Add GeoJSON layer direkt aus Items
    for item in items:
        #print(item.geometry)
        geometry = item.geometry  # GeoJSON dict
        folium.GeoJson(
            data=geometry,
            name=item.id,
            style_function=lambda x: style
        ).add_to(fmap)

    # Zusatzfunktionen
    # folium_plugins.Fullscreen().add_to(fmap)
    folium.LayerControl(position='topright', collapsed=True).add_to(fmap)

    fmap.save("map.html")

    # Dann manuell oder per Code öffnen:
    webbrowser.open("map.html")

    return True

def usgs_alternate_s3(stac_items):
    for item in stac_items:
        print("Test")
        for asset_key, asset in item.assets.items():
            # print(asset_key)
            if "alternate" in asset.extra_fields and "s3" in asset.extra_fields["alternate"]:
                print(asset.extra_fields["alternate"]["s3"]["href"])
                asset.href = asset.extra_fields["alternate"]["s3"]["href"]
    return stac_items



# Ein Beispiel für einen URL Mapping Helper
# def change_asset(stac_items):
#
#     for item in stac_items:
#         # Assets transformieren
#         # print(item)
#         print()
#         print("###############")
#         print()
#         # provider_id = None
#         provider_id = "usgs"
#
#         for asset_key, asset in item.assets.items():
#             #print(asset_key)
#             #print(asset)
#
#             original_url = asset.href
#             #print(original_url)
#
#             # URL-Transformation basierend auf dem Typ
#             if original_url.startswith("s3://"):
#                 # S3 URL extrahieren
#                 parts = original_url[5:].split("/", 1)
#                 # print(parts)
#                 if len(parts) == 2:
#                     bucket = parts[0]
#                     key = parts[1]
#
#                     # Provider identifizieren (vereinfacht)
#                     # provider_id = None
#                     for pid, pconfig in PROVIDERS.items():
#                         print(pconfig.get("type"))
#                         print(pconfig.get("bucket"))
#                         if pconfig.get("type") == "s3" and pconfig.get("bucket") == bucket:
#                             provider_id = "usgs" # pid
#                             break
#
#                     if provider_id:
#                         print(f">>>>>>>here")
#
#                         # Neue URL erstellen
#                         asset.href = f"{PROXY_BASE_URL}/{provider_id}/{key}"
#                         print(f">>>>>>>{asset.href}")
#
#             # elif original_url.startswith("http://") or original_url.startswith("https://"):
#             #     #if original_url.startswith("http://") or original_url.startswith("https://"):
#             #     print(original_url)
#             #
#             #     # Provider identifizieren (vereinfacht)
#             #     # provider_id = None
#             #     print(f"-----------{provider_id}-----------")
#             #     for pid, pconfig in PROVIDERS.items():
#             #
#             #         if (
#             #                 pconfig.get("base_url") and
#             #                 pconfig.get("base_url") in original_url):
#             #             provider_id = pid
#             #             break
#             #         # if pconfig.get("type") == "http":# and pconfig.get("bucket") == bucket:
#             #         #     provider_id = pid
#             #         #     #print(provider_id)
#             #         #     break
#             #     #
#             #     print(PROVIDERS.get(provider_id))
#             #     print(PROVIDERS.get(provider_id).get("base_url"))
#             #     try:
#             #         parts = original_url.split(PROVIDERS.get(provider_id)["base_url"], 1)
#             #         print(parts)
#             #         if len(parts) == 2:
#             #             key = parts[1]
#             #         else:
#             #             print("????")
#             #             #print(parts)
#             #
#             #     except Exception as e:
#             #         print(e)
#             #
#             #     if provider_id:
#             #         # Neue URL erstellen
#             #         asset.href = f"{PROXY_BASE_URL}/{provider_id}/{key}"
#             #         print(asset.href)
#
#     return stac_items


if __name__ == "__main__":

    # wegen IDE Problem
    matplotlib.use("TkAgg")
    PROXY_BASE_URL = "http://127.0.0.1:8080"

    provider = "dlr_dsda"
    #provider = False

    if provider == "cdse":

        # os.environ["AWS_ACCESS_KEY_ID"] = cdse["access_key"]
        # os.environ["AWS_SECRET_ACCESS_KEY"] = cdse["secret_key"]
        # os.environ["AWS_REGION"] = "default"
        # os.environ["AWS_S3_ENDPOINT"] = r"eodata.dataspace.copernicus.eu"
        #
        # os.environ["AWS_NO_SIGN_REQUEST"] = "NO"
        # os.environ["AWS_VIRTUAL_HOSTING"] = "FALSE"
        os.environ["GDAL_DRIVER_PATH"] = os.path.join(os.environ["CONDA_PREFIX"], "Library", "lib", "gdalplugins")
        #import os
        os.environ["GDAL_SKIP"] = "netCDF"

        # cdse
        catalog_url = "https://stac.dataspace.copernicus.eu/v1/"
        collections= "sentinel-1-grd"#"sentinel-3-sl-2-aod-nrt"#"sentinel-1-grd" #"sentinel-2-l2a",#"S2_L2A_MAJA"
        bands = None # ["B01_20m"]


        # Functions
        items = get_stac_items(catalog_url, collections)

        # for item in items:
        #     print("Test")
        #     for asset_key, asset in item.assets.items():
        #         print(asset_key)
        #         print(asset)

        #changed_items = change_asset(items)

        # odcube = get_odc(items[:1], bands)
        #odcube = get_odc(changed_items[:1], bands)

        #map = show_bboxes(items)


        # show result based on stac
        #print(odcube.vh)

        # load and show result
        # odcube.B01_20m.plot.imshow(col="time")
        #odcube.vh.plot.imshow(col="time")
        # odcube.blue.plot.imshow(col="time")
        # odcube.nrt_aod.plot.imshow(col="time")
        plt.show()

    if provider == "usgs":

        # os.environ["AWS_ACCESS_KEY_ID"] = usgs_["access_key"]
        # os.environ["AWS_SECRET_ACCESS_KEY"] = usgs_["secret_key"]
        # os.environ["AWS_REGION"] = "us-west-2"
        # os.environ["AWS_REQUEST_PAYER"] = "requester"

        # usgs
        catalog_url = "https://landsatlook.usgs.gov/stac-server"
        collections = "landsat-c2l1"#"landsat-c2ard-ta"#"landsat-c2l1"#"landsat-c2l2-sr"
        not_working_collections = "landsat-c2l3-dswe"
        bands = None # ["B01_20m"]

        # Functions
        items = get_stac_items(catalog_url, collections)

        for item in items:
            print("Test")
            for asset_key, asset in item.assets.items():
                print(asset_key)
                print(asset)

        altered_items = usgs_alternate_s3(items)

        #changed_items = change_asset(altered_items)

        #odcube = get_odc(altered_items[:1], bands)
        #odcube = get_odc(changed_items[:1], bands)

        #map = show_bboxes(altered_items)
        # map = show_bboxes(changed_items)


        #print(odcube.blue)

        # odcube.blue.plot.imshow(col="time")
        #print(odcube.blue[0:10])
        # subset = odcube.blue.isel(x=slice(0,200), y=slice(0,1))
        # arr = subset.compute().values
        # print(arr)
        #odcube.blue[0:10].plot.imshow(col="time")
        plt.show()
        #

        # gdal/rio data access
        # blue_uri = ("s3://usgs-landsat/collection02/level-2/standard/oli-tirs/2025/034/032/LC09_L2SP_034032_20250505_"
        #             "20250506_02_T1/LC09_L2SP_034032_20250505_20250506_02_T1_SR_B2.TIF")
        blue_uri = ("http://127.0.0.1:8080/usgs/collection02/level-2/standard/oli-tirs/2025/034/032/LC09_L2SP_034032_20250505_20250506_"
                    "02_T1/LC09_L2SP_034032_20250505_20250506_02_T1_SR_B2.TIF")
        #blue_uri = ("http://127.0.0.1:8080/usgs/collection02/oli-tirs/2025/CU/012/009/LC08_CU_012009_20250506_20250516_02/LC08_CU_012009_20250506_"
        #            "20250516_02_TOA_B2.TIF")
        with rasterio.open(blue_uri) as src:
            print(src.profile)
            subset = src.read(1, window=((0, 512), (0, 512)))  # Nur kleines Fenster lesen
            print(subset.shape)
            print(subset)
            print(len(subset))

        # s3://usgs-landsat-ard/collection02/oli-tirs/2025/CU/012/009/LC08_CU_012009_20250506_20250516_02/LC08_CU_012009_20250506_20250516_02_TOA_B1.TIF
        # collection02/level-2/standard/oli-tirs/2025/034/032/LC09_L2SP_034032_20250505_20250506_02_T1/LC09_L2SP_034032_20250505_20250506_02_T1_SR_B2.TIF

    if provider == "usgs_m2m":

        # from usgs import USGSAuthExpiredError, USGSError, api
        #
        # for i in range(2):
        #
        #     try:
        #         api.login(usgs_["username"], usgs_["application_token"])
        #     except USGSAuthExpiredError:
        #         api.logout()
        #         continue
        #     except USGSError as e:
        #         if i == 0 and os.path.isfile(api.TMPFILE):
        #             # `.usgs` API file key might be obsolete
        #             # Remove it and try again
        #             os.remove(api.TMPFILE)
        #             continue

        import requests

        LOGIN_URL = "https://m2m.cr.usgs.gov/api/api/json/stable/login-token"
        LOGOUT_URL = "https://m2m.cr.usgs.gov/api/api/json/stable/logout"
        Permissions_URL = "https://m2m.cr.usgs.gov/api/api/json/stable/permissions"
        dataset_URL = "https://m2m.cr.usgs.gov/api/api/json/stable/dataset-search"
        scene_search_URL = "https://m2m.cr.usgs.gov/api/api/json/stable/scene-search"
        download_option_URL = "https://m2m.cr.usgs.gov/api/api/json/stable/download-options"

        # Login: User credentials
        credentials = {
            "username": usgs_["username"],
            "token": usgs_["application_token"]
        }

        response = requests.post(LOGIN_URL, json=credentials)
        print(response.json())

        token = response.json().get("data")

        #response = requests.post(Permissions_URL, headers={"X-Auth-Token": token})
        #print(response.json())
        #
        #
        # response = requests.post(dataset_URL, headers={"X-Auth-Token": token})
        # print(response.json())
        #
        #
        # data = {
        #     "datasetName": "landsat_fsca_tile_c2",
        #     "maxResults": "1"
        # }
        # response = requests.post(scene_search_URL, headers={"X-Auth-Token": token}, json=data)
        # print(response.json())

        payload = {
            'datasetName': 'landsat_fsca_tile_c2',
            'entityIds': ['LC09_AK_000010_20250506_20250510_02_SNOW']
        }
        response = requests.post(download_option_URL, headers={"X-Auth-Token": token}, json=payload)
        print(response.json())


        response = requests.post(LOGOUT_URL, headers={"X-Auth-Token": token})
        print(response.json())

    if provider =="dlr_dsda":

        # todo: Zip Pfade in Items sind teilweise nicht korrekt

        # os.environ["CPL_CURL_VERBOSE"] = "YES"
        # os.environ["CPL_DEBUG"] = "YES"
        # os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "TRUE"

        # dlr
        catalog_url = "https://geoservice.dlr.de/eoc/ogc/stac/v1/?f=application%2Fjson"
        collections = "S2_L3A_WASP"#"S2_L2A_MAJA"#"S2_L3A_WASP"
        not_working_collections = ""
        bands = None # ["B01_20m"]

        # bbox in Deutschland
        box = [5.763545147601, 46.793670688196, 15.152753273727, 55.945635321533]

        # Functions
        items = get_stac_items(catalog_url, collections, box)

        # for item in items:
        #     print("Test")
        #     for asset_key, asset in item.assets.items():
        #         print(asset_key)
        #         print(asset)

        PROXY_BASE_URL = "http://127.0.0.1:8000"

        def change_asset(stac_items, provider):

            for item in stac_items:
                # Assets transformieren
                # print(item)
                print()
                print("###############")
                print()

                for asset_key, asset in item.assets.items():
                    print(asset_key)
                    # print(asset)

                    if provider == "dlr":

                        provider_id = provider

                        original_url = asset.href
                        print(original_url)

                        provider_base_url = PROVIDERS.get(provider).get("base_url")

                        if provider_base_url in original_url:
                            altered_url = original_url.replace(provider_base_url, PROXY_BASE_URL)
                            print(altered_url)
                            key = original_url.split(provider_base_url+"/")[1]
                            asset.href = f"{PROXY_BASE_URL}/{provider_id}/{key}"
                            print(f"{PROXY_BASE_URL}/{provider_id}/{key}")
                        else:
                            print(False)
                            continue

            return stac_items


            #                 print(PROVIDERS.get(provider_id))
        #                 print(PROVIDERS.get(provider_id).get("base_url"))
        #                 try:
        #                     parts = original_url.split(PROVIDERS.get(provider_id)["base_url"], 1)
        #                     print(parts)
        #                     if len(parts) == 2:
        #                         key = parts[1]
        #                     else:
        #                         print("????")
        #                         #print(parts)
        #
        #                 except Exception as e:
        #                     print(e)
        #
        #                 if provider_id:
        #                     # Neue URL erstellen
        #                     asset.href = f"{PROXY_BASE_URL}/{provider_id}/{key}"
        #                     print(asset.href)

        ##########################################################
        #             # URL-Transformation basierend auf dem Typ
        #             if original_url.startswith("s3://"):
        #                 # S3 URL extrahieren
        #                 parts = original_url[5:].split("/", 1)
        #                 # print(parts)
        #                 if len(parts) == 2:
        #                     bucket = parts[0]
        #                     key = parts[1]
        #
        #                     # Provider identifizieren (vereinfacht)
        #                     provider_id = None
        #                     for pid, pconfig in PROVIDERS.items():
        #                         if pconfig.get("type") == "s3":# and pconfig.get("bucket") == bucket:
        #                             provider_id = pid
        #                             break
        #
        #                     if provider_id:
        #                         # Neue URL erstellen
        #                         asset.href = f"{PROXY_BASE_URL}/{provider_id}/{key}"
        #
        #             elif original_url.startswith("http://") or original_url.startswith("https://"):
        #             #if original_url.startswith("http://") or original_url.startswith("https://"):
        #                 print(original_url)
        #
        #                 # Provider identifizieren (vereinfacht)
        #                 # provider_id = None
        #                 print(f"-----------{provider_id}-----------")
        #                 for pid, pconfig in PROVIDERS.items():
        #
        #                     if (
        #                             pconfig.get("base_url") and
        #                             pconfig.get("base_url") in original_url):
        #                         provider_id = pid
        #                         break
        #                     # if pconfig.get("type") == "http":# and pconfig.get("bucket") == bucket:
        #                     #     provider_id = pid
        #                     #     #print(provider_id)
        #                     #     break
        #     #
        #                 print(PROVIDERS.get(provider_id))
        #                 print(PROVIDERS.get(provider_id).get("base_url"))
        #                 try:
        #                     parts = original_url.split(PROVIDERS.get(provider_id)["base_url"], 1)
        #                     print(parts)
        #                     if len(parts) == 2:
        #                         key = parts[1]
        #                     else:
        #                         print("????")
        #                         #print(parts)
        #
        #                 except Exception as e:
        #                     print(e)
        #
        #                 if provider_id:
        #                     # Neue URL erstellen
        #                     asset.href = f"{PROXY_BASE_URL}/{provider_id}/{key}"
        #                     print(asset.href)
        #
        #     return stac_items

        changed_items = change_asset(items, "dlr")


        # map = show_bboxes(items)

        #odcube = get_odc(items[:1], bands)

        odcube = get_odc(changed_items[:1], "FRC_B2")

        print(odcube)
        #print(odcube.WGT_R2)

        #odcube.FRC_B2.plot.imshow(col="time")
        odcube.FRC_B2.plot.imshow(col="time")
        plt.show()


        # with rasterio.open("https://download.geoservice.dlr.de/S2_L3A_WASP/files/32/U/QU/2024/SENTINEL2X_20241215-000000-000_L3A_T32UQU_C_V1-2/SENTINEL2X_20241215-000000-000_L3A_T32UQU_C_V1-2_FRC_B7.tif") as src:
        #     print(src.profile)
        #     print(src.read(1))

        url_1 = ("http://127.0.0.1:8080/dlr/S2_L2A_MAJA/files/33/U/VS/2025/02/SENTINEL2B_20250203-100641-946_L2A_T33UVS_"
               "C_V1-3.zip#SENTINEL2B_20250203-100641-946_L2A_T33UVS_C/SENTINEL2B_20250203-100641-946_L2A_T33UVS_C_FRE_B2.tif:1")
        url_2 = ("/vsizip/vsicurl/https://download.geoservice.dlr.de/S2_L2A_MAJA/files/33/U/VS/2025/02/SENTINEL2B_20250203-100641-946_L2A_T33UVS_C_V1-3.zip#SENTINEL2B_20250203-100641-946_L2A_T33UVS_C/SENTINEL2B_20250203-100641-946_L2A_T33UVS_C_FRE_B2.tif:1")
        url_3 = ("https://download.geoservice.dlr.de/S2_L3A_WASP/files/32/U/QU/2024/SENTINEL2X_20241215-000000-000_L3A_T32UQU_C_V1-2/SENTINEL2X_20241215-000000-000_L3A_T32UQU_C_V1-2_FRC_B7.tif")
        url_3 = ("http://127.0.0.1:8000/dlr/S2_L3A_WASP/files/32/U/QV/2024/SENTINEL2X_20241215-000000-000_L3A_T32UQV"
                 "_C_V1-2/MASKS/SENTINEL2X_20241215-000000-000_L3A_T32UQV_C_V1-2_WGT_R2.tif")
        url_3 = ("http://127.0.0.1:8000/dlr/S2_L3A_WASP/files/32/U/QU/2024/SENTINEL2X_20241215-000000-"
                 "000_L3A_T32UQU_C_V1-2/SENTINEL2X_20241215-000000-000_L3A_T32UQU_C_V1-2_FRC_B2.tif")
        # url_3 = ("https://download.geoservice.dlr.de/S2_L3A_WASP/files/32/U/QV/2024/SENTINEL2X_20241215-000000-"
        #          "000_L3A_T32UQV_C_V1-2/MASKS/SENTINEL2X_20241215-000000-000_L3A_T32UQV_C_V1-2_WGT_R2.tif")
        # with rasterio.open(url_3) as src:
        #     print(src.profile)
        #     print(src.read())
            # subset = src.read(1, window=((0, 5012), (0, 5012)))  # Nur kleines Fenster lesen
            # print(subset.shape)
            # print(subset)
            # print(len(subset))

    if provider =="terrabyte":

        # terrabyte
        catalog_url = "https://stac.terrabyte.lrz.de/public/api"
        collections = "landsat-tm-c2-l2"
        not_working_collections = ""
        bands = None # ["B01_20m"]

        # bbox in Deutschland
        box = [5.763545147601, 46.793670688196, 15.152753273727, 55.945635321533]

        # Functions
        items = get_stac_items(catalog_url, collections, box)

        # for item in items:
        #     print("Test")
        #     for asset_key, asset in item.assets.items():
        #         print(asset_key)
        #         print(asset)

        map = show_bboxes(items)

        odcube = get_odc(items[:1], bands)

        print(odcube.B01)

        odcube.B01.plot.imshow(col="time")
        plt.show()




# landsat_fsca_tile_c2
# 618533b64eb7c770
# LC09_AK_000010_20250506_20250510_02_SNOW


# todo: Was ist Caching? -> Wieso ist (1.lädt Datei herunter 2. speichert im lokalen Cache/S3/Filesystem 3. liefert
#  STAC-Response mit lokalem Asset-Link) sinnvoller als die Daten einfach dort zu belassen und zu öffnen? Denn (Client
#  kann daraus mit GDAL (VSI or HTTP) oder direkt laden) sollte doch auch möglich sein, wenn Daten bei einem Provider liegen?
# todo: Was sind signierte URLs?
# todo: Was meinst du mit "Streaming-/Blockzugriff (GDAL, ODC) → nur indirekt oder mit Cache-Systemen"?

# CDSE -------------
# import rasterio
# import boto3
# from rasterio.session import AWSSession
#
# session = boto3.Session(
#     aws_access_key_id=cdse["access_key"],
#     aws_secret_access_key=cdse["secret_key"],
#     region_name="default"
# )
#
# # In Rasterio übergeben
# aws_session = AWSSession(session)
# # ENV Variablen für GDAL setzen
# os.environ["AWS_S3_ENDPOINT"] = r"eodata.dataspace.copernicus.eu"
# os.environ["AWS_NO_SIGN_REQUEST"] = "NO"
# os.environ["AWS_VIRTUAL_HOSTING"] = "FALSE"
# os.environ["GDAL_DRIVER_PATH"] = os.path.join(os.environ["CONDA_PREFIX"], "Library", "lib", "gdalplugins")
#
# with rasterio.Env(aws_session):
#     #with rasterio.open("s3://eodata/Sentinel-2/MSI/L2A/2025/05/11/S2B_MSIL2A_20250511T130709_N0511_R081_T39XVJ_20250511T140257.SAFE/GRANULE/L2A_T39XVJ_A042721_20250511T130709/IMG_DATA/R20m/T39XVJ_20250511T130709_B01_20m.jp2") as src:
#     with rasterio.open("/vsis3/eodata/Sentinel-2/MSI/L2A/2025/05/11/S2B_MSIL2A_20250511T130709_N0511_R081_T39XVJ_20250511T140257.SAFE/GRANULE/L2A_T39XVJ_A042721_20250511T130709/IMG_DATA/R20m/T39XVJ_20250511T130709_B01_20m.jp2") as src:
#         print(src.profile)
#
# # s3://eodata/Sentinel-3/SLSTR/SL_2_AOD___/2025/04/29/S3B_SL_2_AOD____20250429T051307_20250429T051805_20250429T074221_0299_106_048______MAR_O_NR_003.SEN3/NRT_AOD.nc