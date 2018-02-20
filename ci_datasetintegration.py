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
import datetime

# schemas
stat_schema = 'public'  # 'stat' on production/dev database
geo_schema = 'public'  # 'geo' on production/dev database

# current path
base_path = os.path.dirname(os.path.abspath(__file__))

# git directory
# g_dir = '/home/hud/hotmaps/ci-datasets/git-repos/HotmapsLAU'
# g_dir = '/home/hud/hotmaps/ci-datasets/git-repos/pop_tot_curr_density'
repository_name = 'electricity_emissions_hourly'
g_dir = base_path + '/git-repos/' + repository_name

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
        -e POSTGRES_PASSWORD=***REMOVED*** \
        -e POSTGRES_DB=toolboxdb \
        -e PGDATA=/var/lib/postgresql/data \
        -p 32768:5432 \
        -d hotmaps/postgis-database
"""


def import_shapefile(src_file):
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

        # static way of getting feature fields
        # comm_id = feature.GetField("comm_id")
        # shape_leng = feature.GetField("shape_leng")
        # shape_area = feature.GetField("shape_area")
        # year = feature.GetField("year")
        # stat_levl_ = feature.GetField("stat_levl")
        #    name = feature.GetField("NAME").decode("Latin-1")
        geom = feature.GetGeometryRef()

        # convert Polygon type to MultiPolygon
        if geom.GetGeometryType() == osgeo.ogr.wkbPolygon:
            geom = osgeo.ogr.ForceToMultiPolygon(geom)

        # export as WKT
        wkt = geom.ExportToWkt()

        # add date from datapackage.json
        values.append(date)

        db.query(commit=True, query='INSERT INTO ' + geo_schema + '.' + table_name \
                                    + ' (' + ', '.join(
            map(str_with_quotes, [x.lower() for x in db_attributes_names])) + ')' \
                                    + ' VALUES ' + '(' + ', '.join(
            map(str_with_single_quotes, values)) + ', ST_GeomFromText(\'' + wkt + '\', ' + str(proj) + ')' + ')')


def str_with_quotes(obj):
    return '"' + str(obj) + '"'


def str_with_single_quotes(obj):
    return '\'' + str(obj) + '\''


class DB(object):
    conn_string = ''
    conn = None

    """docstring for DB."""

    def __init__(self, conn_string):
        super(DB, self).__init__()
        self.conn_string = conn_string
        try:
            self.conn = psycopg2.connect(conn_string)
        except psycopg2.Error as e:
            print(e)
            conn.close()
            sys.exit(1)

    def query(self, query, commit=False):
        try:
            print("executing Query : ", query)
            cursor = self.conn.cursor()
            cursor.execute(query)
            if commit:
                self.conn.commit()
        except psycopg2.Error as e:
            print(e)

    def drop_table(self, table_name):
        try:
            print('Droping table ', table_name)
            cursor = self.conn.cursor()
            cursor.execute('DROP TABLE IF EXISTS ' + table_name)
            self.conn.commit()
        except Exception as e:
            print(e)

    def create_table(self, table_name, col_names, col_types, id_col_name='id'):
        try:
            print('Creating table ', table_name)
            # lower all
            col_names = [x.lower() for x in col_names]
            col_types = [x.lower() for x in col_types]

            query = 'CREATE TABLE IF NOT EXISTS ' + table_name + '(' + id_col_name + ' bigserial, ' + ', '.join(
                ' '.join(n) for n in zip(col_names, col_types)) + ')'
            print(query)
            cursor = self.conn.cursor()
            cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            print(e)

    def close_connection(self, ):
        self.conn.close()


# git pull
#g = git.cmd.Git(g_dir)
#g.pull()

# connect to database
db = DB(conn_string="host='localhost' port='32768' dbname='toolboxdb' user='hotmaps' password='***REMOVED***'")

# read datapackage.json (dp)
try:
    dp = json.load(open(g_dir + '/datapackage.json'))

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

        elif gis_data_type == 'raster-package':
            raster = r['raster']
            proj = raster['epsg']
            number_of_bands = raster['number_of_bands']
            band0 = raster['band0']
            raster_path = os.path.join(base_path, 'git-repos', repository_name, path)

            os.environ['PGHOST'] = 'localhost'
            os.environ['PGPORT'] = '32768'
            os.environ['PGUSER'] = 'hotmaps'
            os.environ['PGPASSWORD'] = '***REMOVED***'
            os.environ['PGDATABASE'] = 'toolboxdb'

            cmds = 'cd git-repos/' + repository_name + '/data ; raster2pgsql -d -s ' + proj + ' -t "auto" -I -C -Y "' + name + '" ' + stat_schema + '.' + name + ' | psql'
            print(cmds)
            subprocess.call(cmds, shell=True)
        elif gis_data_type == 'tabular-data-resource':
            schema = r['schema']

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

            finalPath = g_dir + '/' + path

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
