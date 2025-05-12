import os
import webbrowser

from creds import cdse
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
def get_stac_items(catalog_url, collections):

    client = pystacclient.open(catalog_url)

    # Führe die Suche aus
    search = client.search(
        bbox=[-105.0, 40.0, -104.0, 41.0],
        #bbox=[5.763545147601, 46.793670688196, 15.152753273727, 55.945635321533],
        max_items=1,
        #limit=1,
        collections=collections,
    )
    a = next(search.items())
    print(a)
    print(type(a))
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
            chunks={'x': 1024, 'y': 1024}, # spatial chunking
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


if __name__ == "__main__":

    # wegen IDE Problem
    matplotlib.use("TkAgg")

    #
    os.environ["AWS_ACCESS_KEY_ID"] = cdse["access_key"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = cdse["secret_key"]
    os.environ["AWS_REGION"] = "default"
    os.environ["AWS_S3_ENDPOINT"] = r"eodata.dataspace.copernicus.eu"

    os.environ["AWS_NO_SIGN_REQUEST"] = "NO"
    os.environ["AWS_VIRTUAL_HOSTING"] = "FALSE"
    os.environ["GDAL_DRIVER_PATH"] = os.path.join(os.environ["CONDA_PREFIX"], "Library", "lib", "gdalplugins")

    # # cdse
    # catalog_url = "https://stac.dataspace.copernicus.eu/v1/"
    # collections= "sentinel-3-sl-2-aod-nrt"#"sentinel-3-sl-2-aod-nrt"#"sentinel-1-grd" #"sentinel-2-l2a",#"S2_L2A_MAJA"
    # bands = None # ["B01_20m"]

    # usgs
    catalog_url = "https://landsatlook.usgs.gov/stac-server"
    collections= "landsat-c2l2-sr"
    bands = None # ["B01_20m"]

    # Functions
    items = get_stac_items(catalog_url, collections)

    odcube = get_odc(items[:1], bands)

    map = show_bboxes(items)


    # show result based on stac
    # print(odcube.B01_20m)
    print(odcube.blue)


    # load and show result
    # odcube.B01_20m.plot.imshow(col="time")
    # odcube.vh.plot.imshow(col="time")
    odcube.blue.plot.imshow(col="time")
    plt.show()







#
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
