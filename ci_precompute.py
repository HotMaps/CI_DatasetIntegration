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
from rasterstats import zonal_stats, point_query
from .ci_secrets import secrets

# schemas
stat_schema = 'public'  # 'stat' on production/dev database
geo_schema = 'public'  # 'geo' on production/dev database

# current path
base_path = os.path.dirname(os.path.abspath(__file__))

# git directory
# g_dir = '/home/hud/hotmaps/ci-datasets/git-repos/HotmapsLAU'
# g_dir = '/home/hud/hotmaps/ci-datasets/git-repos/pop_tot_curr_density'
repository_name = 'pop_tot_curr_density'
#g_dir = base_path + '/git-repos/' + repository_name
g_dir = '/home/hud/git-repos/' + repository_name

# default lau and nuts shapefiles
lau_shp = '/home/hud/git-repos/HotmapsLAU/data/HotmapsLAU.shp'


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
db = DB(conn_string="host='localhost' port='32768' dbname='toolboxdb' user='hotmaps' password='" + secrets.DB_password + "'")

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

            # convert Polygon type to MultiPolygon
            if geom_type.lower() == 'polygon':
                geom_type = 'MultiPolygon'

        elif gis_data_type == 'raster-package':
            raster = r['raster']
            proj = raster['epsg']
            number_of_bands = raster['number_of_bands']
            band0 = raster['band0']
            raster_path = os.path.join(base_path, 'git-repos', repository_name, path)

            # compute zonal stats
            stats = zonal_stats(lau_shp, raster_path)
            pprint(stats)

        elif gis_data_type == 'tabular-data-resource':
            schema = r['schema']

            temp = r['temporal']
            start_date = temp['start']
            end_date = temp['end']

            fields = schema['fields']

        else:
            print('Unknown GEO data type, only vector-package/raster-package/tabular-data-resource')
except Exception as e:
    logging.error(traceback.format_exc())
    sys.exit(1)

db.close_connection()
