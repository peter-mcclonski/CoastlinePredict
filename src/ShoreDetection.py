import datetime
import ee
import geojson
import threading


def print_status():
    stat_dict = {}
    for t in tlist:
        state = t.status()['state']
        if state not in stat_dict.keys():
            stat_dict[state] = 0
        stat_dict[state] += 1
        if state == 'FAILED':
            print(t.status())

    print(stat_dict)

    if 'COMPLETED' not in stat_dict.keys() or len(stat_dict.keys()) != 1:
        threading.Timer(5.0, print_status).start()


def ndvi(image: ee.Image):
    ndvi_band = image.expression("(NIR - R) / (NIR + R)", {
        'NIR': image.select('B8'),
        'R': image.select('B4')
    })

    moderate_mask = ndvi_band.gt(0.15)
    dense_mask = ndvi_band.gt(0.55)
    ndvi_band = moderate_mask.add(dense_mask)

    return ndvi_band


def mndwi(image: ee.Image):
    mndwi_band = image.expression("(GREEN - SWIR1) / (GREEN + SWIR1)", {
        'GREEN': image.select('B3'),
        'SWIR1': image.select('B11')
    })
    mndwi_band = mndwi_band.gt(0)

    return mndwi_band


def awei(image: ee.Image):
    awei_band = image.expression("4 * (GREEN - SWIR2) - (0.25 * NIR + 2.75 * SWIR1)", {
        'GREEN': image.select('B3'),
        'SWIR2': image.select('B12'),
        'NIR': image.select('B8'),
        'SWIR1': image.select('B11')
    })

    awei_band = awei_band.gt(0)
    return awei_band


def ndmi(image: ee.Image):
    ndmi_band = image.expression("(NIR - SWIR) / (NIR + SWIR)", {
        'NIR': image.select('B8'),
        'SWIR': image.select('B11')
    })

    low_moisture = ndmi_band.gt(-0.4)
    moderate_moisture = ndmi_band.gt(0)
    high_moisture = ndmi_band.gt(0.4)
    waterlogged = ndmi_band.gt(0.8)

    ndmi_band = low_moisture.add(moderate_moisture).add(high_moisture).add(waterlogged)

    return ndmi_band


def addIndexBands(image: ee.Image):
    return {
        'MNDWI': mndwi(image),
        'AWEI': awei(image),
        'NDVI': ndvi(image),
        'NDMI': ndmi(image)
    }


temp_res = datetime.timedelta(days=90)
today = datetime.date.today()
min_date = datetime.date(2015, 6, 23)


tlist = []
with open('../GeoJSON/Bounds_Set1.geojson') as aoi_file:
    aoi_set = geojson.load(aoi_file)['features']

aoi_id = 0

ee.Initialize()

for aoi in aoi_set:
    aoi_id += 1
    curr = min_date
    aoi = aoi['geometry']
    while curr < today:
        name = f"US_East_Coast_S2_AoI_{aoi_id}_{curr.strftime('%Y-%m-%d')}"

        sentinel_2 = ee.ImageCollection('COPERNICUS/S2')\
            .filterDate(curr.strftime("%Y-%m-%d"), (curr + temp_res).strftime("%Y-%m-%d"))\
            .filterBounds(aoi)

        curr += temp_res

        if sentinel_2.size().getInfo() == 0:
            print("No image found for " + name)
            continue

        sentinel_2 = sentinel_2.median().select("B.+")

        indexes = addIndexBands(sentinel_2)
        indexes['Base'] = sentinel_2

        for i in indexes.keys():
            new_task = ee.batch.Export.image.toDrive(indexes[i], description=name + f"_{i}", folder='CT_Coast_Sentinel_2_All_Bands', scale=20, region=ee.Geometry(aoi))

            new_task.start()
            tlist.append(new_task)

        print("Began export: " + name)

print_status()
