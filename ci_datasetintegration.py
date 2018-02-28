# python 3.5
import sys
import git
import json
from pprint import pprint
import os.path
import psycopg2
import osgeo.ogr
import traceback
import logging
import subprocess
import csv
from ci_secrets.secrets import DB_password, DB_database, DB_host, DB_port, DB_user, GIT_base_path
from db import db_helper

# schemas
from db.db_helper import str_with_quotes, str_with_single_quotes

stat_schema = 'public'  # 'stat' on production/dev database
geo_schema = 'public'  # 'geo' on production/dev database

# current path
base_path = os.path.dirname(os.path.abspath(__file__))

# git repositories path
repositories_base_path = GIT_base_path
repository_name = 'HDD_CDD_curr'
git_path = os.path.join(repositories_base_path, repository_name)

# README
# To test this script localy, run the following script before
# to start postgis with the same options as DEV/PROD versions
"""
#!/bin/bash
sudo docker kill postgis-database
sudo docker rm postgis-database

sudo docker run \
        --name=postgis-database \
        -e POSTGRES_USER=hotmaps \
        -e POSTGRES_PASSWORD=password \
        -e POSTGRES_DB=toolboxdb \
        -e PGDATA=/var/lib/postgresql/data \
        -p 32768:5432 \
        -d hotmaps/postgis-database
"""


def import_shapefile(src_file, date):
    # import shp
    # src_file = os.path.join("git-repos", "HotmapsLAU", "data", "HotmapsLAU.shp")
    shapefile = osgeo.ogr.Open(src_file)
    layer = shapefile.GetLayer(0)
    for i in range(layer.GetFeatureCount()):
        feature = layer.GetFeature(i)
        values = []
        # get fields dynamically
        for att in attributes_names:
            values.append(feature.GetField(att))

        geom = feature.GetGeometryRef()

        # convert Polygon type to MultiPolygon
        if geom.GetGeometryType() == osgeo.ogr.wkbPolygon:
            geom = osgeo.ogr.ForceToMultiPolygon(geom)

        # export as WKT
        wkt = geom.ExportToWkt()

        # add date from datapackage.json
        values.append(date)

        db.query(commit=True,
                 query='INSERT INTO ' + geo_schema + '.' + table_name
                       + ' (' + ', '.join(
                     map(db_helper.str_with_quotes, [x.lower() for x in db_attributes_names])) + ')'
                       + ' VALUES ('
                       + ', '.join(map(db_helper.str_with_single_quotes, values))
                       + ', ST_GeomFromText(\'' + wkt + '\', ' + str(proj) + ')'
                       + ')'
                 )


# git pull
# g = git.cmd.Git(g_dir)
# g.pull()

# connect to database
db = db_helper.DB(host=DB_host, port=str(DB_port), database=DB_database, user=DB_user, password=DB_password)

# read datapackage.json (dp)
try:
    dp = json.load(open(git_path + '/datapackage.json'))

    gis_data_type = dp['profile']
    gis_resources = dp['resources']
    dataset_version = dp['version']
    table_name = dp['name'].lower().replace("hotmaps", "")

    pprint(dp)
    print(table_name)

    for r in gis_resources:
        format = r['format']
        name = r['name']
        path = r['path']
        # date = r['date']
        if gis_data_type == 'vector-package':
            vector = r['vector']
            proj = vector['epsg']
            geom_type = vector['geometry_type']
            schema = vector['schema']

            # retrieve start and end date
            temp = r['temporal']
            start_date = temp['start']
            end_date = temp['end']

            attributes_names = []
            attributes_types = []
            for att in schema:
                col_type = att['type']
                if col_type == 'string':
                    col_type = 'varchar(255)'
                elif col_type == 'integer':
                    col_type = 'bgint'
                elif col_type == 'float':
                    col_type = 'numeric(20,2)'
                elif col_type == 'boolean':
                    col_type = 'boolean'
                elif col_type == 'date':
                    col_type = 'date'
                elif col_type == 'datetime':
                    col_type = 'timestamp'
                elif col_type == 'timestamp':
                    col_type = 'timestamp'
                else:
                    print('Unhandled table type ', col_type)
                    break

                attributes_names.append(att['name'])
                attributes_types.append(col_type)

            # create a copy of lists (one for db[including date] one for shapefile)
            db_attributes_names = list(attributes_names)
            db_attributes_types = list(attributes_types)

            # add date and geometry columns from datapackage.json
            db_attributes_names.append('date')
            db_attributes_types.append('date')
            db_attributes_names.append('geom')

            # convert Polygon type to MultiPolygon
            if geom_type.lower() == 'polygon':
                geom_type = 'MultiPolygon'

            db_attributes_types.append('geometry(' + geom_type + ', ' + proj + ')')

            # drop table
            db.drop_table(table_name=geo_schema + '.' + table_name)
            # create table if not exists
            db.create_table(table_name=geo_schema + '.' + table_name, col_names=db_attributes_names,
                            col_types=db_attributes_types, id_col_name='gid')

            # import shapefile
            import_shapefile(os.path.join(base_path, 'git-repos', repository_name, path))

        elif gis_data_type == 'raster-data-resource':
            raster = r['raster']
            proj = raster['epsg']

            # retrieve start and end date
            temp = r['temporal']
            start_date = temp['start']
            end_date = temp['end']

            # number_of_bands = raster['number_of_bands']
            # band0 = raster['band0']
            raster_path = os.path.join(base_path, 'git-repos', repository_name, path)

            os.environ['PGHOST'] = DB_host
            os.environ['PGPORT'] = DB_port
            os.environ['PGUSER'] = DB_user
            os.environ['PGPASSWORD'] = DB_password
            os.environ['PGDATABASE'] = DB_database

            cmds = 'cd git-repos/' + repository_name + '/data ; raster2pgsql -d -s ' + proj + ' -t "auto" -I -C -Y "' + name + '" ' + stat_schema + '.' + name + ' | psql'
            print(cmds)
            subprocess.call(cmds, shell=True)
        elif gis_data_type == 'tabular-data-resource':
            schema = r['schema']

            # retrieve start and end date
            temp = r['temporal']
            start_date = temp['start']
            end_date = temp['end']

            fields = schema['fields']

            attributes_names = []
            attributes_types = []
            attributes_units = []

            for att in fields:
                col_type = att['type']
                if col_type == 'string':
                    col_type = 'varchar(255)'
                elif col_type == 'integer':
                    col_type = 'bigint'
                elif col_type == 'float':
                    col_type = 'numeric(20,2)'
                elif col_type == 'boolean':
                    col_type = 'boolean'
                elif col_type == 'date':
                    col_type = 'date'
                elif col_type == 'datetime':
                    col_type = 'timestamp'
                elif col_type == 'timestamp':
                    col_type = 'timestamp'
                else:
                    print('Unhandled table type ', col_type)
                    break

                attributes_names.append(att['name'])
                attributes_types.append(col_type)
                attributes_units.append(att['unit'])

            db_attributes_names = list(attributes_names)
            db_attributes_types = list(attributes_types)

            valuesUnit = []
            valuesUnitName = []
            valuesUnitType = []
            for i in range(0, len(attributes_names)):
                if attributes_units[i] and attributes_units[i] != "":
                    valuesUnitName.append(attributes_names[i] + "_unit")
                    valuesUnit.append(attributes_units[i])
                    valuesUnitType.append("varchar(255)")

            if valuesUnit and len(valuesUnit) > 0:
                db_attributes_names.extend(valuesUnitName)
                db_attributes_types.extend(valuesUnitType)

            db_attributes_names.append('start_date')
            db_attributes_types.append('timestamp')

            db_attributes_names.append('end_date')
            db_attributes_types.append('timestamp')

            db.drop_table(table_name=stat_schema + '.' + table_name)
            db.create_table(table_name=stat_schema + '.' + table_name, col_names=db_attributes_names,
                            col_types=db_attributes_types, id_col_name='id')

            finalPath = git_path + '/' + path

            file = open(finalPath, "r")
            reader = csv.DictReader(file)

            for row in reader:
                values = []
                for name in attributes_names:
                    att = row[name]
                    values.append(att)

                if valuesUnit and len(valuesUnit) > 0:
                    values.extend(valuesUnit)

                values.append(start_date)
                values.append(end_date)

                db.query(commit=True, query='INSERT INTO ' + stat_schema + '.' + table_name + ' (' + ', '.join(
                    map(str_with_quotes,
                        [x.lower() for x in db_attributes_names])) + ')' + ' VALUES ' + '(' + ', '.join(
                    map(str_with_single_quotes, values)) + ')')
        else:
            print('Unknown GEO data type, only vector-package/raster-package/tabular-data-resource')
except Exception as e:
    logging.error(traceback.format_exc())
    sys.exit(1)

db.close_connection()
