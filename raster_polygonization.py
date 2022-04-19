import requests
import time
import os
import sys
from osgeo import gdal, osr, ogr
import urllib.parse as urlparse
from urllib.parse import parse_qs
import psycopg2

orig_url =  'https://geodaten.naturschutz.rlp.de/kartendienste_naturschutz/mod_mapserver/nquery_hilite.php?qx=418264.19235909&qy=5580395.2974272&activelayer=alkis_flur_nc,luftbilder_wms_ov&restrictlayer=&deactivatelayer=&FORMAT=image%2Fpng&TRANSPARENT=TRUE&SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&STYLES=&SRS=EPSG%3A25832&BBOX=417923.40920977,5580175.4287959,418660.00881203,5581050.1408237&WIDTH=696&HEIGHT=826'

max_width = 5000
max_height = 5000

try:
    con=psycopg2.connect("dbname='' user='' host='' port='' password=''")
    print ("Successfully connected to database")
except:
    print ("Not connected")

cursor = con.cursor()

epoch_time = int(time.time())

cursor.execute("""CREATE TEMP TABLE polygons(id serial, area float, geom geometry(Polygon,25832) )""")

input_png_name = 'input'+str(epoch_time)+'.png'
georef_png_name = 'georef'+str(epoch_time)+'.png'

parsed = urlparse.urlparse(orig_url)
bbox = str(parse_qs(parsed.query)['BBOX'][0]).split(',')

gt_org_lat = float(bbox[3])
gt_org_lng = float(bbox[0])
cell_size_x = abs(float(bbox[0])-float(bbox[2]))/(float(parse_qs(parsed.query)['WIDTH'][0]))
cell_size_y = abs(float(bbox[1])-float(bbox[3]))/float((parse_qs(parsed.query)['HEIGHT'][0]))

new_2 = float(bbox[0]) + (cell_size_x * max_width)
new_3 = float(bbox[1]) + (cell_size_y * max_height)

new_url = orig_url[:orig_url.find('BBOX')] + 'BBOX='+str(bbox[0])+','+str(bbox[1])+ ',' +str(new_2)+','+str(new_3)+'&WIDTH='+str(max_width)+'&HEIGHT='+str(max_height)
print ('orig_url: ',orig_url)
print ('new_url:  ',new_url)
parsed = urlparse.urlparse(new_url)
bbox = str(parse_qs(parsed.query)['BBOX'][0]).split(',')
print(str(parse_qs(parsed.query)['BBOX'][0]).split(','))

r = requests.get(new_url, allow_redirects=True)
open(input_png_name, 'wb').write(r.content)
dst_filename = (georef_png_name)

# Opens source dataset
src_ds = gdal.Open(input_png_name)
format = "PNG"
driver = gdal.GetDriverByName(format)

# Open destination dataset
dst_ds = driver.CreateCopy(dst_filename, src_ds, 0)

gt_org_lat = float(bbox[3])
gt_org_lng = float(bbox[0])
cell_size_x = abs(float(bbox[0])-float(bbox[2]))/(src_ds.RasterXSize)
cell_size_y = abs(float(bbox[1])-float(bbox[3]))/(src_ds.RasterYSize)

gt = [gt_org_lng, cell_size_x , 0, gt_org_lat, 0, -cell_size_y]
dst_ds.SetGeoTransform(gt)

# Get raster projection
epsg = 25832
srs = osr.SpatialReference()
srs.ImportFromEPSG(epsg)
dest_wkt = srs.ExportToWkt()

# Set projection
dst_ds.SetProjection(dest_wkt)

# Close files

dst_ds = None
src_ds = None

src_ds = gdal.Open(georef_png_name)

if src_ds is None:
    print('Unable to open %s' % src_fileName)
    sys.exit(1)
srcband = src_ds.GetRasterBand(1)
drv = ogr.GetDriverByName("ESRI Shapefile")

dst_ds = drv.CreateDataSource("/vsimem/vector_poly.shp")
dst_layer = dst_ds.CreateLayer("vector_poly.shp", srs)

id_field = ogr.FieldDefn('id', ogr.OFTInteger)
dst_layer.CreateField(id_field)
area_field = ogr.FieldDefn('area', ogr.OFTReal)
dst_layer.CreateField(area_field)

gdal.Polygonize(srcband, None, dst_layer, 0, [],callback=None )

poly_area = 0
for feat in dst_layer:
    if (feat.GetField("id") == 0):
        outer_poly = feat.GetGeometryRef()
        dst_layer.DeleteFeature(feat.GetFID())

dst_layer.ResetReading()
for feature in dst_layer:
    if(feature.GetGeometryRef().Touches(outer_poly) and feature.GetField("id") != 254 ):
        dst_layer.DeleteFeature(feature.GetFID())
    else:
        geom = feature.GetGeometryRef()
        area = geom.GetArea()
        feature.SetField("area", area)
        dst_layer.SetFeature(feature)
        poly_area = poly_area + area
        wkt = feature.GetGeometryRef().ExportToWkt()
        cursor.execute("INSERT INTO polygons (area,geom) " +"VALUES (%s, ST_GeometryFromText(%s, " +"25832))", (area, wkt))
        con.commit()

cursor.execute("select st_astext(st_union(geom)),st_area(st_union(geom)) from polygons")
poly_record = cursor.fetchall()
final_wkt_polygon = poly_record[0][0]
final_area = poly_record[0][1]

print('final_wkt_poly', final_wkt_polygon)
print('final_area', final_area)

dst_ds = None
src_ds = None

con.close()

os.remove(input_png_name)
os.remove(georef_png_name)
os.remove(georef_png_name+'.aux.xml')

