# python 3.5
import sys
import json
from pprint import pprint
import os.path
import git
from git import Repo
from datetime import date, datetime, timedelta
import osgeo.ogr
import traceback
import logging
import subprocess
import csv
from ci_secrets.secrets import DB_password, DB_database, DB_host, DB_port, DB_user, GIT_base_path, GEO_base_path, \
    GEO_number_of_pyarmid_levels, GEO_user, GEO_password, GEO_url, GEO_port
from db import db_helper
import validate_datapackage
from db.db_helper import str_with_quotes, str_with_single_quotes
import requests
import gitlab
import logging


logging.basicConfig(level=logging.INFO)

# schemas
stat_schema = 'stat'  # 'stat' on production/dev database
geo_schema = 'geo'  # 'geo' on production/dev database

# geo tables
lau_table_name = 'lau'
lau_table = 'public' + '.' + lau_table_name # change to geo_schema when lau table has been moved in db
nuts_table_name = 'nuts'
nuts_table = geo_schema + '.' + nuts_table_name
vector_SRID = "3035"
raster_SRID = "3035"

# time tables
time_table_name = 'time'
time_table = stat_schema + '.' + time_table_name

# current path
base_path = os.path.dirname(os.path.abspath(__file__))

# git repositories path
repositories_base_path = GIT_base_path
repository_name = ''
listOfRepositories = []
repository_path = os.path.join(repositories_base_path, repository_name)
print(repository_path)

# README
# To test this script localy, run the following script before
# to start postgis with the same options as DEV/PROD versions
"""
#!/bin/bash
sudo docker kill postgis-databaseprint(
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
def parse_date(str):
    for format in ('%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(str, format)
        except:
            pass
    raise ValueError('date format not supported! excpecting: ', '%Y/%m/%d %H:%M:%S', ' or ', '%Y-%m-%d %H:%M:%S')

def get_or_create_time_id(timestamp, granularity):
    fk_time_id = db.query(commit=True,
                          query="SELECT id FROM stat.time WHERE timestamp = '" + timestamp + "' AND granularity LIKE '" + granularity + "'")
    if fk_time_id == None:
        print("Error getting fk_time_id with psycopg2")
    elif len(fk_time_id) == 0:
        time_attributes = []
        timestamp_att = parse_date(timestamp)

        # timestamp
        time_attributes.append(timestamp)
        # year
        year = timestamp_att.strftime('%Y')
        time_attributes.append(year)
        # month
        time_attributes.append(timestamp_att.strftime('%m'))
        # day
        time_attributes.append(timestamp_att.strftime('%d'))
        #[0][0] weekday
        weekday_num = timestamp_att.strftime('%w')
        if weekday_num != 0 and weekday_num != 6:
            weekday = "Week"
        else:
            weekday = timestamp_att.strftime('%a')
        time_attributes.append(weekday)
        # season
        year = int(year)
        seasons = [('winter', (date(year, 1, 1),  date(year, 3, 20))),
                   ('spring', (date(year, 3, 21),  date(year, 6, 20))),
                   ('summer', (date(year, 6, 21),  date(year, 9, 22))),
                   ('autumn', (date(year, 9, 23),  date(year, 12, 20))),
                   ('winter', (date(year, 12, 21),  date(year, 12, 31)))]
        season = next(s for s, (start, end) in seasons if start <= timestamp_att.date() <= end)
        time_attributes.append(season)
        # hour
        hour = timestamp_att.strftime('%H')
        time_attributes.append(hour)
        # hour of yearprint(
        day_of_year = timestamp_att.strftime('%j')
        hour_of_year = int(day_of_year) * int(hour)
        time_attributes.append(hour_of_year)
        # date
        time_attributes.append(timestamp_att.strftime('%Y-%m-%d'))
        # granularity
        time_attributes.append(granularity)

        fk_time_id = db.query(commit=True,
                 query='INSERT INTO ' + time_table +
                       ' (timestamp, year, month, day, weekday, season, hour_of_day, hour_of_year, date, granularity) ' +
                       'VALUES (' + ', '.join(map(str_with_single_quotes, time_attributes)) + ') RETURNING id')

    if len(fk_time_id) > 0 and len(fk_time_id[0]) > 0:
        fk_time_id = fk_time_id[0][0]

    return fk_time_id


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
verbose = True

# check repository on gitlab

#repo_date = datetime.utcnow()-timedelta(days=1)
repo_date = datetime(2010, 1, 1, 0, 0, 0)
dateStr = repo_date.isoformat(sep='T')+'Z'
gl = gitlab.Gitlab('https://gitlab.com', private_token='f-JzjmRRnxzwqC5o3zsQ')

group = gl.groups.get('1354895')

projects = group.projects.list(all=True)

for project in projects:

    proj = gl.projects.get(id=project.id)
    
    commits = proj.commits.list(since=dateStr)
    if len(commits)==0:
        print('No commit')
    else:
		try:
			f = proj.files.get(file_path='datapackage.json', ref='master')			
			repository_name = proj.name
			listOfRepositories.append(repository_name)
			repository_path = os.path.join(repositories_base_path, repository_name)
			print(repository_name)
			if os.path.exists(repository_path):
				# git pull
				print('update repository')
				g = git.cmd.Git(repository_path)
				g.pull()
				print('successfuly updated repository')

			else:
				# git clone
				print('clone repository')
				url = proj.http_url_to_repo
				Repo.clone_from(url, repository_path)
				print('successfuly cloned repository')
		except:
			print('No datapackage.json or in a wrong place')


listOfRepositories.append('.git')

# Validate_Datapackage

try:
	list_dirs = [name for name in os.listdir(GIT_base_path) if os.path.isdir(os.path.join(GIT_base_path, name))]
	list_dirs = sorted(list_dirs)
	for d in list_dirs:
		d_file_path = os.path.join(repositories_base_path, d, 'datapackage.json')
		print(d_file_path)
		print()
		print("#########################")
		# validate_datapackage.print(d, bcolors.HEADER)
		print(d)
		print("#########################")
		if validate_datapackage.validate_datapackage(d_file_path) == False:
			print(d + ' has been removed')
			listOfRepositories.remove(d)
			
except Exception as e:
	logging.error(traceback.format_exc())
	sys.exit(1)

listOfRepositories.remove('temperature_profile_daily_avg_household_yearlong_2010')
listOfRepositories.remove('temperature_profile_daily_avg_industry_yearlong_2010')
listOfRepositories.remove('scen_ambitious_building_demand')

    # read datapackage.json (dp)
for name in listOfRepositories:
	repository_path = os.path.join(repositories_base_path, name)
	  
	try:
		print(repository_path)
		dp = json.load(open(repository_path + '/datapackage.json'))

		gis_data_type = dp['profile']
		gis_resources = dp['resources']
		dataset_version = dp['version']
		table_name = dp['name'].lower().replace("hotmaps", "")

		print(dp)
		print(table_name)

		for r in gis_resources:
			format = r['format']
			name = r['name']
			path = r['path']
			# date = r['date']
			raster_table_name = name.split('.')[0]
			precomputed_table_name_lau = raster_table_name + "_" + lau_table_name
			precomputed_table_name_nuts = raster_table_name + "_" + nuts_table_name

			if gis_data_type == 'raster-data-resource':
				raster = r['raster']
				proj = raster['epsg']

				# retrieve start and end date
				start_date = '1970-01-01 00:00:00'
				end_date = '1970-01-01 00:00:00'

				try:
					temp = r['temporal']
					start_date = temp['start']
					end_date = temp['end']
				except:
					# keep default data
					pass

				# number_of_bands = raster['number_of_bands']
				# band0 = raster['band0']
				raster_path = os.path.join(repository_path, path)  # (base_path, 'git-repos', repository_name, path)

				os.environ['PGHOST'] = DB_host
				os.environ['PGPORT'] = DB_port
				os.environ['PGUSER'] = DB_user
				os.environ['PGPASSWORD'] = DB_password
				os.environ['PGDATABASE'] = DB_database

				rast_tbl = geo_schema + '.' + raster_table_name

				#cmds = 'cd ' + repository_path + '/data ; raster2pgsql -d -s ' + proj + ' -t "auto" -I -C -Y "' + name + '" ' + rast_tbl + ' | psql'
				cmds = 'raster2pgsql -d -s ' + proj + ' -t "auto" -I -C -Y "' + raster_path + '" ' + rast_tbl + ' | psql'
				print(cmds)
				subprocess.call(cmds, shell=True)

				# Precompute layers for nuts and lau
				# LAU
				vect_tbl = "public." + lau_table_name
				vect_tbl_name = lau_table_name
				prec_tbl = stat_schema + '.' + precomputed_table_name_lau
				prec_tbl_name = precomputed_table_name_lau

				db.drop_table(table_name=prec_tbl, notices=verbose)

				attributes_names = (
					'count', 'sum', 'mean', 'stddev', 'min', 'max', 'comm_id', 'fk_' + vect_tbl_name + '_gid')
				constraints = "ALTER TABLE " + prec_tbl + " " \
							  + "ADD CONSTRAINT " + prec_tbl_name + "_" + vect_tbl_name + "_gid_fkey " \
							  + "FOREIGN KEY (fk_" + vect_tbl_name + "_gid) " \
							  + "REFERENCES " + vect_tbl + "(gid) " \
							  + "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION "

				db.create_table(table_name=prec_tbl,
								col_names=attributes_names,
								col_types=('bigint', 'numeric(20,2)', 'numeric(20,2)', 'numeric(20,2)', 'numeric(20,2)',
										   'numeric(20,2)', 'varchar(255)', 'bigint'),
								constraints_str=constraints,
								notices=verbose)

				query = "SELECT (" \
						+ "SELECT (ST_SummaryStatsAgg(ST_Clip(" + rast_tbl + ".rast, 1, ST_Transform(" + \
						vect_tbl + ".geom, " + raster_SRID + "), true), 1, true)) " \
						+ "FROM " + rast_tbl + " " \
						+ "WHERE ST_Intersects(" \
						+ rast_tbl + ".rast, ST_Transform(" + vect_tbl + ".geom, 3035) " \
						+ ") " \
						+ ").*, " + vect_tbl + ".comm_id, " + vect_tbl + ".gid " \
						+ "FROM " + vect_tbl + " "

				db.query(commit=True, notices=verbose, query='INSERT INTO ' + prec_tbl
															 + ' (' + ', '.join(
					map(db_helper.str_with_quotes, [x.lower() for x in attributes_names])) + ') '
															 + query + ' ;')

				# NUTS
				vect_tbl = geo_schema + '.' + nuts_table_name
				prec_tbl = stat_schema + '.' + precomputed_table_name_nuts
				vect_tbl_name = nuts_table_name
				prec_tbl_name = precomputed_table_name_nuts

				db.drop_table(table_name=prec_tbl, notices=verbose)

				attributes_names = (
					'count', 'sum', 'mean', 'stddev', 'min', 'max', 'nuts_id', 'fk_' + vect_tbl_name + '_gid')
				constraints = "ALTER TABLE " + prec_tbl + " " \
							  + "ADD CONSTRAINT " + prec_tbl_name + "_" + vect_tbl_name + "_gid_fkey " \
							  + "FOREIGN KEY (fk_" + vect_tbl_name + "_gid) " \
							  + "REFERENCES " + vect_tbl + "(gid) " \
							  + "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION "

				db.create_table(table_name=prec_tbl,
								col_names=attributes_names,
								col_types=('bigint', 'numeric(20,2)', 'numeric(20,2)', 'numeric(20,2)', 'numeric(20,2)',
										   'numeric(20,2)', 'varchar(255)', 'bigint'),
								constraints_str=constraints,
								notices=verbose)

				query = "SELECT (" \
						+ "SELECT (ST_SummaryStatsAgg( ST_Clip(" + rast_tbl + ".rast, 1, ST_Transform(" + \
						vect_tbl + ".geom, " + raster_SRID + "), true), 1, true)) " \
						+ "FROM " + rast_tbl + " " \
						+ "WHERE ST_Intersects(" \
						+ rast_tbl + ".rast, ST_Transform(" + vect_tbl + ".geom, 3035) " \
						+ ") " \
						+ ").*, " + vect_tbl + ".nuts_id, " + vect_tbl + ".gid " \
						+ "FROM " + vect_tbl + " "

				db.query(commit=True, notices=verbose, query='INSERT INTO ' + prec_tbl
															 + ' (' + ', '.join(
					map(db_helper.str_with_quotes, [x.lower() for x in attributes_names])) + ') '
															 + query + ' ;')

				# build pyramid and add layer to geoserver
				pyramid_path = os.path.join(GEO_base_path, raster_table_name)

				# build pyramid using gdal_retile script
				# todo: could improve by enabling "COMPRESS=JPEG" option, but doing so raised error
				cmds = 'cd ' + os.path.join(repository_path,
											'data') + ' ; rm -r ' + pyramid_path + ' ; mkdir ' + pyramid_path + ' ; gdal_retile.py -v -r bilinear ' + \
					   '-levels ' + str(GEO_number_of_pyarmid_levels) + ' ' + \
					   '-ps 2048 2048 -co "TILED=YES" ' + \
					   '-targetDir ' + pyramid_path + ' ' + raster_path
				print(cmds)
				subprocess.call(cmds, shell=True)

				# add to geoserver

				workspace = 'hotmaps'
				layer_name = raster_table_name

				# remove coverage store from geoserver
				response = requests.delete(
					'http://' + GEO_url + ':' + GEO_port + '/geoserver/rest/workspaces/' + workspace + '/coveragestores/' + layer_name + '?recurse=true',
					auth=(GEO_user, GEO_password),
				)
				print(response, response.content)
				# create data store
				headers = {
					'Content-type': 'text/xml',
				}
				data = '<coverageStore>' \
					   '<name>' + layer_name + '</name>' \
					   '<workspace>hotmaps</workspace>' \
					   '<enabled>true</enabled>' \
					   '<type>ImagePyramid</type>' \
					   '<url>file:raster-layers/' + layer_name + '</url>' \
					   '</coverageStore>'

				response = requests.post(
					'http://' + GEO_url + ':' + GEO_port + '/geoserver/rest/workspaces/' + workspace + '/coveragestores?configure=all',
					headers=headers,
					data=data,
					auth=(GEO_user, GEO_password),
				)
				# print(data)
				print(response, response.content)

				# create layer
				data = '<coverage>' \
					   '<name>' + layer_name + '</name>' \
					   '<title>' + layer_name + '</title>' \
					   '<srs>EPSG:' + proj + '</srs>' \
					   '</coverage>'

				response = requests.post(
					'http://' + GEO_url + ':' + GEO_port + '/geoserver/rest/workspaces/' + workspace + '/coveragestores/' + layer_name + '/coverages',
					headers=headers,
					data=data,
					auth=(GEO_user, GEO_password),
				)
				# print(data)
				print(response, response.content)
				print('INSERT DONE : ' + name) 
			
			else:
				print('Unknown GEO data type, only vector-data-resource/raster-data-resource/tabular-data-resource')
	except Exception as e:
		logging.error(traceback.format_exc())
		sys.exit(1)

db.close_connection()
