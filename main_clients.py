from pystac_client import Client

# Erstelle einen STAC-Client für Deinen Gateway
# catalog_url = "http://localhost:8000"  # Dein Gateway
# catalog_url = "https://landsatlook.usgs.gov/stac-server/"
catalog_url = "https://stac.dataspace.copernicus.eu/v1/"
client = Client.open(catalog_url)

# Führe die Suche aus
search = client.search(
    bbox=[-105.0, 40.0, -104.0, 41.0],
    #bbox=[5.763545147601, 46.793670688196, 15.152753273727, 55.945635321533],
    max_items=10,
    # limit=1,
    collections="sentinel-2-l2a",#"S2_L2A_MAJA",
)

print(f"{search.matched()} items found")
print(search.items())
for item in search.items():
    print(item.id)
#     #print(item.assets.get("blue").href)
#     print(item.assets.get("blue").href)

# first_page = search.item_collection()         # gibt das rohe ItemCollection-Objekt der ersten Seite
# print(first_page)
# items = list(first_page)             # Features der ersten Seite
# print(len(items))  # -> bis zu 5

# pages = search.pages()                        # Iterator über Seiten
# page1 = next(pages)                           # nur die erste Seite
# items = list(page1)

# for item in items:
#     print(item.id)


# page = search.item_collection()      # Ein einziger POST /search
# features = page.items             # Liste der Features der ersten Seite
# print(len(features))
# print(features)
# # for item in features:
# #     print(item.assets)





#
# ###########################################################
# # data-download
# ############################################################
# import subprocess
#

# url_data = "https://landsatlook.usgs.gov/data/collection02/level-2/standard/oli-tirs/2025/033/032/LC09_L2SP_033032_20250428_20250430_02_T1/LC09_L2SP_033032_20250428_20250430_02_T1_SR_B2.TIF"
# #
# # cmd = [
# #     "gdalinfo",
# #     f"--config", "GDAL_HTTP_AUTH", "BASIC",
# #     f"--config", "GDAL_HTTP_USERPWD", f"[{username}]:[{password}]",
# #     url
# # ]
# #
# # result = subprocess.run(cmd, capture_output=True, text=True)
# #
# # if result.returncode != 0:
# #     print("Fehler:", result.stderr)
# # else:
# #     print(result.stdout)
#
# import requests
# from bs4 import BeautifulSoup
# url = 'https://ers.cr.usgs.gov/login' # this is end point for login
# with requests.Session() as s:
#
#     r = s.get(url)
#     #soup = BeautifulSoup(r.content, 'html5lib')
#     soup = BeautifulSoup(r.content, 'html.parser')
#     sval = soup.find('input', attrs={'name':'csrf'})['value']
#
#     data = {"username": username, # change your username and password
#             "password": password,
#             "csrf": sval}
#
#     bf= s.post(url, data = data)
#     print(bf.status_code)
#     respb = s.get(url_data,
#                   allow_redirects=True,
#                   headers = {'content-type': 'image/tiff'})
#     with open('output.tif', 'wb') as src:
#         src.write(respb.content)