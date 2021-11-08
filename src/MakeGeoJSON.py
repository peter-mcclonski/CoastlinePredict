import geopandas as gpd
import matplotlib.pyplot as plt

pointSet = gpd.read_file('../CoastalPoints/Points_Set1.shp')
pointSet = pointSet.to_crs(4326)
buffer = pointSet.buffer(0.04, cap_style=3)

fig, ax = plt.subplots()
buffer.boundary.plot(ax=ax)
plt.show()

buffer.to_file('../GeoJSON/Bounds_Set1.geojson')
