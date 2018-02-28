# python 3.5
import sys
import os.path
import traceback
import logging
import json
from ci_secrets.secrets import GIT_base_path, GEO_base_path, GEO_number_of_pyarmid_levels, GEO_user, GEO_password
import subprocess
import requests


# schemas
stat_schema = 'stat'  # 'stat' on production/dev database
geo_schema = 'geo'  # 'geo' on production/dev database

# current path
base_path = os.path.dirname(os.path.abspath(__file__))

# repository
repository_name = 'pop_tot_curr_density'
repository_path = os.path.join(GIT_base_path, repository_name)
print(repository_path)

# read datapackage.json (dp)
try:
    dp = json.load(open(repository_path + '/datapackage.json'))

    gis_data_type = dp['profile']
    gis_resources = dp['resources']
    dataset_version = dp['version']
    table_name = dp['name'].lower().replace("hotmaps", "")

    for r in gis_resources:
        format = r['format']
        name = r['name'].split('.')[0]
        path = r['path']
        # date = r['date']
        if gis_data_type == 'vector-data-resource':
            print('vector data resource')
            vector = r['vector']
            proj = vector['epsg']
            geom_type = vector['geometry_type']
            schema = vector['schema']

            # retrieve start and end date
            temp = r['temporal']
            start_date = temp['start']
            end_date = temp['end']

        elif gis_data_type == 'raster-data-resource':
            print('raster data resource')
            raster = r['raster']
            proj = raster['epsg']

            # retrieve start and end date
            temp = r['temporal']
            start_date = temp['start']
            end_date = temp['end']

            raster_path = os.path.join(repository_path, path)
            pyramid_path = os.path.join(GEO_base_path, name)

            # build pyramid using gdal_retile script
            # todo: could improve by enabling "COMPRESS=JPEG" option, but doing so raised error
            cmds = 'cd ' + os.path.join(repository_path, 'data') + ' ; rm -r ' + pyramid_path + ' ; mkdir ' + pyramid_path + ' ; gdal_retile.py -v -r bilinear ' + \
                   '-levels ' + str(GEO_number_of_pyarmid_levels) + ' ' + \
                   '-ps 2048 2048 -co "TILED=YES" ' + \
                   '-targetDir ' + pyramid_path + ' ' + raster_path
            print(cmds)
            #subprocess.call(cmds, shell=True)

            # add to geoserver
            workspace = 'hotmaps'
            # create data store
            headers = {
                'Content-type': 'text/xml',
            }
            data = '<coverageStore>' \
                   '<name>' + name + '</name>' \
                   '<workspace>hotmaps</workspace>' \
                   '<enabled>true</enabled>' \
                   '<type>ImagePyramid</type>' \
                   '<url>file:raster-layers/pop_tot_curr_density</url>' \
                   '</coverageStore>'

            response = requests.post(
                'http://localhost:9090/geoserver/rest/workspaces/' + workspace + '/coveragestores?configure=all',
                headers=headers,
                data=data,
                auth=(GEO_user, GEO_password)
            )
            #print(data)
            #print(response)

            # create layer
            data = '<coverage>' \
                   '<name>' + name + '</name>' \
                   '<title>' + name + '</title>' \
                   '<srs>EPSG:' + proj + '</srs>' \
                   '</coverage>'

            response = requests.post(
                'http://localhost:9090/geoserver/rest/workspaces/' + workspace + '/coveragestores/' + name + '/coverages',
                headers=headers,
                data=data,
                auth=(GEO_user, GEO_password)
            )
            #print(data)
            #print(response)

        elif gis_data_type == 'tabular-data-resource':
            print('tabular data resource')
            schema = r['schema']

            # retrieve start and end date
            temp = r['temporal']
            start_date = temp['start']
            end_date = temp['end']

            fields = schema['fields']

        else:
            print('Unknown GEO data type, only vector-package/raster-package/tabular-data-resource')

except Exception as e:
    logging.error(traceback.format_exc())
    sys.exit(1)
