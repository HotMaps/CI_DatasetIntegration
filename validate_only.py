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
    GEO_number_of_pyarmid_levels, GEO_user, GEO_password, GEO_url, GEO_port, TAIGA_token, GIT_token
from db import db_helper
import validate_datapackage
from db.db_helper import str_with_quotes, str_with_single_quotes
import requests
import gitlab
from gitlab import GitlabError, GitlabAuthenticationError, GitlabConnectionError, GitlabHttpError
import logging
from time import time, strftime, gmtime
from taiga import TaigaAPI
from taiga.exceptions import TaigaException


log_start_time = time()
log_previous_time = log_start_time
print(strftime("Execution start time: %Y-%m-%d %H:%M:%S +0000", gmtime(log_start_time)))

taiga_api = TaigaAPI(token=TAIGA_token)
taiga_project = taiga_api.projects.get_by_slug('widmont-hotmaps')
logging.basicConfig(level=logging.INFO)
taiga_api = None
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

def log_print_step(text):
    print(text)
    log_end_time = time()
    global log_previous_time
    prev_time = log_previous_time
    print(strftime("%Y-%m-%d %H:%M:%S +0000", gmtime(log_end_time)))
    hours, rem = divmod(log_end_time-prev_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Current step time: {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))
    hours, rem = divmod(log_end_time-log_start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Ellapsed time: {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))
    log_previous_time = log_end_time

def post_issue(name, description, issue_type='Dataset integration'):
    print(name, description, issue_type)

def post_issue_repo(project, name, description):
    issue = project.issues.create({'title': name, 'description': description})

def get_property_datapackage(obj, property_name, repo_name, resource_name):
    try:
        vector = obj[property_name]
    except:
        post_issue(name='Integration of resource failed - repository ' + repo_name,
                   description='No vector attribute provided for resource "' + resource_name + '". The resource has been skipped.'
                             + 'Make sure that "' + property_name + '" attribute is correctly declared in the "datapackage.json" file')

def parse_date(str):
    for format in ('%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S',
                   '%Y/%m/%d %H:%M', '%Y-%m-%d %H:%M', '%d/%m/%Y %H:%M'):
        try:
            return datetime.strptime(str, format)
        except:
            pass
    raise ValueError('date format not supported! excpecting: ',
                     '%Y/%m/%d %H:%M:%S', ' or ', '%Y-%m-%d %H:%M:%S', ' or ',
                     '%d/%m/%Y %H:%M:%S', ' or ', '%Y/%m/%d %H:%M', ' or ',
                     '%Y-%m-%d %H:%M', ' or ', '%d/%m/%Y %H:%M')

def get_or_create_time_id(timestamp, granularity):
    t = timestamp
    d = None
    try:
        d = parse_date(timestamp)
    except ValueError:
        raise

    if date is not None:
        t = datetime.strftime(d, '%Y/%m/%d %H:%M:%S')

    fk_time_id = db.query(commit=True,
                          query="SELECT id FROM stat.time WHERE timestamp = '" + t + "' AND granularity LIKE '" + granularity + "'")
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

def update_or_create_repo(repo_name, git_id):
    r = repo_name
    d = datetime.now()
    d_str = d.strftime('%Y-%m-%d')

    repo_id = db.query( commit=True,
                        query="SELECT id FROM public.repo WHERE name LIKE '" + r + "' AND git_id = '" + git_id + "'")

    if repo_id == None:
        print("Error getting repo_id with psycopg2")
    elif len(repo_id) == 0:
        repo_attributes = [repo_name, git_id]
        timestamp_att = parse_date(timestamp)
        repo_attributes.append(d_str)
        repo_attributes.append(d_str)

        repo_id = db.query( commit=True,
                            query='INSERT INTO public.repo ' +
                            '(name, git_id, created, updated) ' +
                            'VALUES (' + ', '.join(map(str_with_single_quotes, repo_attributes)) + ') RETURNING id')

    if len(repo_id) > 0 and len(repo_id[0]) > 0:
        repo_id = repo_id[0][0]
        db.query(commit=True,
                 query="UPDATE public.repo SET updated = " + d_str + " WHERE id = " + repo_id)

    return repo_id

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

# connect to databaselistOfRepositories
db = db_helper.DB(host=DB_host, port=str(DB_port), database=DB_database, user=DB_user, password=DB_password)
verbose = False

# create table repo (integration status)
db.create_table(table_name='public' + '.' + 'repo', col_names=['name', 'git_id', 'created', 'updated'],
                col_types=['varchar(255)', 'bigint', 'timestamp', 'timestamp'], id_col_name='id')

# check repository on gitlab

#repo_date = datetime.utcnow()-timedelta(days=3)  #permet de récupérer les datasats des 24 dernières heures
repo_date = datetime(2010, 1, 1, 0, 0, 0) #permet de récupérer tous les datasets.
dateStr = repo_date.isoformat(sep='T')+'Z'
gl = gitlab.Gitlab('https://gitlab.com', private_token=GIT_token)

hotmapsGroups = []
listOfRepositories = []
listOfRepoIds = []

allGroups = gl.groups.list()

group = gl.groups.get('1354895')
hotmapsGroups.append(group)
#print('gitlab group #' + group.id)

subgroups = group.subgroups.list()

log_print_step("Clone/Update repositories")

# Add all subgroups in the groups list as groups
for subgroup in subgroups:
    hotmapsGroups.append(gl.groups.get(subgroup.id, lazy=True))


for group in hotmapsGroups:
    projects = group.projects.list(all=True)
    print(projects)

    for project in projects:
        proj = gl.projects.get(id=project.id)
        commits = proj.commits.list(since=dateStr)
        try:
           if len(commits) == 0:
               print('No recent commit for repository ' + proj.name)
           else:
               repository_name = proj.name
               repository_path = os.path.join(repositories_base_path, repository_name)
               listOfRepositories.append(proj.name)
               listOfRepoIds.append(proj.id)
               print('New commit found for repository ' + repository_name)

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
        except (GitlabAuthenticationError, GitlabConnectionError, GitlabHttpError) as e:
            print('Error while updating repository ' + proj.name + ' (#' + str(proj.id) + ')')
            post_issue(name='Gitlab error for repository ' + repository_name,
                       description='The integration script encountered an error (' + type(e).__name__ + ') while updating/cloning repositories. More info: ' + str(e),
                       issue_type='Integration script execution')
        except Exception as e:
            print('Error while updating repository ' + proj.name + ' (#' + str(proj.id) + ')')
            post_issue(name='Script error for repository ' + repository_name,
                       description='The integration script encountered an error (' + type(e).__name__ + ') while updating/cloning repositories. More info: ' + str(e),
                       issue_type='Integration script execution')

try:
    listOfRepositories.remove('.git')
except:
    pass

repo_index = 0
for repository_name in listOfRepositories:
    """
        VALIDATION
    """
    log_print_step("Validation of " + repository_name)

    # check that repository path is correct
    repo_path = os.path.join(repositories_base_path, repository_name)
    if not os.path.isdir(repo_path):
        print('repo_path is not a directory')

    content = os.listdir('.')

    # check that datapackage file is not missing
    dp_file_path = os.path.join(repo_path, 'datapackage.json')
    if not os.path.isfile(dp_file_path):
        print('datapackage.json file missing or not in correct directory')

    # check that data directory is present
    data_dir_path = os.path.join(repo_path, 'data')
    if not os.path.isdir(data_dir_path):
        print('data directory missing')

    # check datapackage file
    #is_valid = validate_datapackage.validate_datapackage(dp_file_path)
    #if is_valid is not True:
        #print(is_valid)
        #print(d + ' has been removed')
        #listOfRepositories.remove(d)
        #post_issue(name='Validation failed - repository ' + repository_name,
        #           description='The file "datapackage.json" file does not exist, is in the wrong place or contains a mistake.')

    # check properties
    missing_properties = []
    error_messages = []

    # open file
    try:
        dp = json.load(open(dp_file_path))
    except:
        print('can\'t open file datapackage.json')
        dp = {}
    # profile
    try:
        dp_profile = dp['profile']
    except:
        missing_properties.append('profile')
    # resources
    try:
        dp_resources = dp['resources']
    except:
        missing_properties.append('resources')
        dp_resources = None

    # check resources attributes
    if dp_resources:
        if dp_profile == 'vector-data-resource':
            for dp_r in dp_resources:
                print('vector-data-resource')
                props = ['name', 'path', 'format', 'unit', 'vector']
                for p in props:
                    try:
                        a = dp_r[p]
                    except:
                        missing_properties.append('resources/' + p)
                try:
                    dp_path = dp_r['path']
                    if not os.path.isfile(os.join(repo_path, dp_path)):
                        error_messages.append('attribute path does not link to an existing file')
                except:
                    pass
                try:
                    dp_vector = dp_r['vector']
                    dp_epsg = dp_vector['epsg']
                except:
                    missing_properties.append('vector/epsg')
                try:
                    dp_vector = dp_r['vector']
                    dp_geometry_type = dp_vector['geometry_type']
                    if dp_geometry_type.lower() == 'polygon':
                        dp_geometry_type = 'MultiPolygon'
                    elif dp_geometry_type.lower() == 'multipolygon':
                        dp_geometry_type = 'MultiPolygon'
                    elif dp_geometry_type.lower() == 'point':
                        dp_geometry_type = 'Point'
                    elif dp_geometry_type.lower() == 'multipoint':
                        dp_geometry_type = 'MultiPoint'
                    elif dp_geometry_type.lower() == 'multilinestring':
                        dp_geometry_type = 'MultiLinestring'
                    elif dp_geometry_type.lower() == 'linestring':
                        dp_geometry_type = 'Linestring'
                    else:
                        error_messages.append('geometry_type is not set correctly (must be either (multi)point, (multi)linestring or (multi)polygon)')
                except:
                    missing_properties.append('vector/geometry_type')
                try:
                    dp_schema = dp_r['schema']
                    if len(dp_schema) > 0:
                        for f in dp_schema:
                            f_name = f['name']
                            f_unit = f['unit']
                            f_type = f['type']
                except:
                    error_messages.append('errors in schema definition (schema: [{name, unit, type},...])')
        elif dp_profile == 'raster-data-resource':
            print('raster-data-resource')
            for dp_r in dp_resources:
                props = ['name', 'path', 'unit', 'format', 'raster']
                for p in props:
                    try:
                        a = dp_r[p]
                    except:
                        missing_properties.append('resources/' + p)
                try:
                    dp_path = dp_r['path']
                    if not os.path.exists(os.join(repo_path, dp_path)):
                        error_messages.append('attribute path does not link to an existing file')
                except:
                    pass
                try:
                    dp_raster = dp_r['raster']
                    dp_epsg = dp_raster['epsg']
                except:
                    missing_properties.append('raster/epsg')
        elif dp_profile == 'tabular-data-resource':
            print('tabular-data-resource')
            for dp_r in dp_resources:
                props = ['name', 'path', 'schema', 'encoding', 'format', 'dialect']
                for p in props:
                    try:
                        a = dp_r[p]
                    except:
                        missing_properties.append('resources/' + p)
                # fields
                has_geom = False
                f_col_names = []
                try:
                    dp_schema = dp_r['schema']
                    if 'fields' in dp_schema:
                        dp_fields = dp_schema['fields']
                        if len(dp_fields) > 0:
                            for f in dp_fields:
                                f_name = f['name']
                                f_unit = f['unit']
                                f_type = f['type']
                                if f_type == 'geometry':
                                    has_geom = True
                                f_col_names.append(f_name)
                    else:
                        missing_properties.append('fields')
                except:
                    error_messages.append('errors in schema definition (schema: fields: [{name, unit, type},...])')
                # geoms
                if 'spatial_resolution' in dp_r and 'spatial_key_field' in dp_r:
                    if dp_r['spatial_key_field'] not in f_col_names:
                        error_messages.append('spatial_key_field does not refer to an existing field name')
                else:
                    if not has_geom:
                        error_messages.append('no geometry provided (nuts/lau reference [attribute spatial_key_field and spatial_resolution] or geometry field)')
        else:
            err_msg = '\'profile\' contains an unsupported value! Use only vector-data-resource, raster-data-resource or tabular-data-resource'
            print(err_msg)
            error_messages.append(err_msg)

    if len(error_messages) + len(missing_properties) > 0:
        str_error_messages = ''
        if len(error_messages) > 0:
            str_error_messages = 'Errors: \n' + '\n'.join(error_messages)
            if len(missing_properties) > 0:
                str_error_messages = str_error_messages + '\n'
        if len(missing_properties) > 0:
            str_error_messages = 'Missing properties: \n' + '\n'.join(missing_properties)
        print('Validation error for repository ' + repository_name + '\n' + str_error_messages)
        post_issue(name='Validation error ' + repository_name,
                   description='The repository validation was not successful.\n' + str_error_messages)
        continue
    else:
        print('Validation OK')

db.close_connection()
log_end_time = time()
print(strftime("%Y-%m-%d %H:%M:%S +0000", gmtime(log_end_time)))
hours, rem = divmod(log_end_time-log_start_time, 3600)
minutes, seconds = divmod(rem, 60)
print("Script execution time: {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))
print("--- %s seconds ---" % (log_end_time - log_start_time))
