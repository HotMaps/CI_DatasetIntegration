# python 3.5
import sys
import re
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
    GEO_number_of_pyarmid_levels, GEO_user, GEO_password, GEO_url, GEO_port, GEO_workspace, GEO_db_store, TAIGA_token, GIT_token, SERVER, DEBUG
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
from config import STAT_SCHEMA, GEO_SCHEMA, LAU_TABLE, LAU_TABLE_NAME, NUTS_TABLE, NUTS_TABLE_NAME, VECTOR_SRID, RASTER_SRID, TIME_TABLE, TIME_TABLE_NAME


log_start_time = time()
log_previous_time = log_start_time
print(strftime("Execution start time: %Y-%m-%d %H:%M:%S +0000", gmtime(log_start_time)))

try:
    taiga_api = TaigaAPI(token=TAIGA_token)
    taiga_project = taiga_api.projects.get_by_slug('widmont-hotmaps')
except:
    print("Could not connect to Taiga. Token might be outdated")
    taiga_api = None
    taiga_project = None

logging.basicConfig(level=logging.INFO)


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

def post_issue(name, description, issue_type='Dataset integration', tags=[]):
    if DEBUG:
        print(name, description, issue_type, tags.append(SERVER))
        return
    tags.append(SERVER)
    if taiga_project is not None:
        issue = taiga_project.add_issue(
            name,
            taiga_project.priorities.get(name='Workaround possible - Low').id,
            taiga_project.issue_statuses.get(name='New').id,
            taiga_project.issue_types.get(name=issue_type).id,
            taiga_project.severities.get(name='Minor').id,
            description=description,
            tags=tags
        )

def post_issue_repo(project, name, description):
    issue = project.issues.create({'title': name, 'description': description})

def get_property_datapackage(obj, property_name, repo_name, resource_name):
    try:
        vector = obj[property_name]
    except:
        post_issue(name='Integration of resource failed - repository ' + repo_name,
                   description='No vector attribute provided for resource "' + resource_name + '". The resource has been skipped.'
                             + 'Make sure that "' + property_name + '" attribute is correctly declared in the "datapackage.json" file',
                   issue_type='Dataset Provider improvement needed')

def parse_date(str):
    for format in ('%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%d.%m.%Y %H:%M:%S',
                   '%Y/%m/%d %H:%M', '%Y-%m-%d %H:%M', '%d/%m/%Y %H:%M', '%d.%m.%Y %H:%M',
                   '%Y/%m/%d', '%Y-%m-%d', '%d/%m/%Y', '%d.%m.%Y',
                   '%Y/%m', '%Y-%m', '%m/%Y', '%m.%Y',
                   '%Y'):
        try:
            return datetime.strptime(str, format)
        except:
            pass
    raise ValueError('date format not supported! excpecting: ',
                     '%Y/%m/%d %H:%M:%S', ' or ', '%Y-%m-%d %H:%M:%S', ' or ', '%d.%m.%Y %H:%M:%S', ' or ',
                     '%d/%m/%Y %H:%M:%S', ' or ', '%Y/%m/%d %H:%M', ' or ', '%d.%m.%Y %H:%M', ' or ',
                     '%Y-%m-%d %H:%M', ' or ', '%d/%m/%Y %H:%M', ' or ',
                     '%Y/%m/%d', ' or ', '%Y-%m-%d', ' or ', '%d/%m/%Y', '%d.%m.%Y', ' or ',
                     '%Y/%m', ' or ', '%Y-%m', ' or ', '%m/%Y', ' or ', '%m.%Y', ' or ',
                     '%Y')

def get_or_create_time_id(timestamp, granularity):
    t = timestamp
    d = None
    try:
        d = parse_date(timestamp)
    except ValueError:
        raise

    if d is not None:
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
                 query='INSERT INTO ' + TIME_TABLE +
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
                        query="SELECT id FROM public.repo WHERE name LIKE '" + r + "' AND git_id = '" + str(git_id) + "'")

    if repo_id == None:
        print("Error getting repo_id with psycopg2")
    elif len(repo_id) == 0:
        repo_attributes = [repo_name, str(git_id)]
        repo_attributes.append(d_str)
        repo_attributes.append(d_str)

        repo_id = db.query( commit=True,
                            query='INSERT INTO public.repo ' +
                            '(name, git_id, created, updated) ' +
                            'VALUES (' + ', '.join(map(str_with_single_quotes, repo_attributes)) + ') RETURNING id')

    if len(repo_id) > 0 and len(repo_id[0]) > 0:
        repo_id = repo_id[0][0]
        db.query(commit=True,
                 query="UPDATE public.repo SET updated = '" + d_str + "' WHERE id = " + str(repo_id))

    return repo_id

def import_shapefile(src_file, date, attributes_names):
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
                 query='INSERT INTO ' + GEO_SCHEMA + '.' + table_name
                       + ' (' + ', '.join(
                     map(db_helper.str_with_quotes, [x.lower() for x in db_attributes_names])) + ')'
                       + ' VALUES ('
                       + ', '.join(map(db_helper.str_with_single_quotes, values))
                       + ', ST_GeomFromText(\'' + wkt + '\', ' + str(proj) + ')'
                       + ')'
                 )


# connect to databaselistOfRepositories
db = db_helper.DB(host=DB_host, port=str(DB_port), database=DB_database, user=DB_user, password=DB_password)
verbose = False

# create table repo (integration status)
db.create_table(table_name='public' + '.' + 'repo', col_names=['name', 'git_id', 'created', 'updated'],
                col_types=['varchar(255)', 'bigint', 'timestamp', 'timestamp'], id_col_name='id')

# check repository on gitlab

repo_date = datetime.utcnow()-timedelta(days=1)  # allows to retrieve the datasets from past 24h
#repo_date = datetime(2010, 1, 1, 0, 0, 0) # allows to retrieve datasets.
dateStr = repo_date.isoformat(sep='T')+'Z'
gl = gitlab.Gitlab('https://gitlab.com', private_token=GIT_token)

hotmapsGroups = []
listOfRepositories = []
listOfRepoIds = {}

if len(sys.argv) > 1:
    log_print_step('Manual integration process')
    # manual pull
    for arg in sys.argv[1:]:
        print(arg)
        p = gl.search('projects', arg)
        if len(p) > 0:
            proj = gl.projects.get(p[0]['id'])

            print(proj.name, proj.id)
            repository_name = proj.name
            repository_path = os.path.join(repositories_base_path, repository_name)
            if os.path.exists(repository_path):
                # git pull
                print('update repository')
                repo = Repo(repository_path)
                repo.git.execute(["git", "fetch", "--all"])
                repo.git.execute(["git", "reset", "--hard", "origin/master"])
                repo.git.execute(["git", "lfs", "pull"]) # force pull lfs files
                print('successfuly updated repository')
            else:
                # git clone
                print('clone repository')
                url = proj.http_url_to_repo
                repo = Repo.clone_from(url, repository_path)
                repo.git.execute(["git", "lfs", "pull"]) # force pull lfs files
                print('successfuly cloned repository')

            # add to list of repositories to process if clone/pull succeeds (only!)
            listOfRepositories.append(proj.name)
            listOfRepoIds[proj.name] = proj.id
        else:
            print('repository not found')
else :
    log_print_step('Automatic integration process')
    # automatic pull/clone
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

            # check if repo is private or not
            if proj.visibility != 'public':
                print('Repository', proj.name, 'is not public. Skipping...')
                post_issue(name='Visibility issue for ' + proj.name,
                    description='The repository is not public and has been skipped. Please set the repository to public in order to integrate it.',
                    issue_type='Integration script execution')

                continue

            commits = proj.commits.list(since=dateStr)
            try:
                if len(commits) == 0:
                    print('No recent commit for repository ' + proj.name)
                else:
                    repository_name = proj.name
                    repository_path = os.path.join(repositories_base_path, repository_name)
                    print('New commit found for repository ' + repository_name)

                    if os.path.exists(repository_path):
                        # git pull
                        print('update repository')
                        repo = Repo(repository_path)
                        repo.git.execute(["git", "fetch", "--all"])
                        repo.git.execute(["git", "reset", "--hard", "origin/master"])
                        repo.git.execute(["git", "lfs", "pull"]) # force pull lfs files
                        print('successfuly updated repository')
                    else:
                        # git clone
                        print('clone repository')
                        url = proj.http_url_to_repo
                        repo = Repo.clone_from(url, repository_path)
                        repo.git.execute(["git", "lfs", "pull"]) # force pull lfs files
                        print('successfuly cloned repository')

                    # add to list of repositories to process if clone/pull succeeds (only!)
                    listOfRepositories.append(proj.name)
                    listOfRepoIds[proj.name] = proj.id
            except (GitlabAuthenticationError, GitlabConnectionError, GitlabHttpError) as e:
                print('Error while updating repository ' + proj.name + ' (#' + str(proj.id) + ')')
                post_issue(name='Gitlab error for repository ' + repository_name,
                        description='The integration script encountered an error (' + type(e).__name__ + ') while updating/cloning repositories. More info: \n' + str(e),
                        issue_type='Integration script execution')
            except Exception as e:
                print('Error while updating repository ' + proj.name + ' (#' + str(proj.id) + ')')
                post_issue(name='Script error for repository ' + repository_name,
                        description='The integration script encountered an error (' + type(e).__name__ + ') while updating/cloning repositories. More info: \n' + str(e),
                        issue_type='Integration script execution')

try:
    listOfRepositories.remove('HotmapsLAU')
    listOfRepositories.remove('lau2')
    listOfRepositories.remove('NUTS')
    listOfRepositories.remove('potential_wind')
    listOfRepositories.remove('.git')
except:
    pass



for repository_name in listOfRepositories:
    if repository_name == 'HotmapsLAU' or repository_name == 'lau2' or repository_name == 'NUTS':
        continue
    """
        VALIDATION
    """
    log_print_step("Validation of " + repository_name)

    # check that repository path is correct
    repo_path = os.path.join(repositories_base_path, repository_name)
    print(repo_path)
    if not os.path.isdir(repo_path):
        print('repo_path is not a directory')
        msg = 'repository path is not a directory'
        post_issue(name='Validation error ' + repository_name,
                   description='The repository validation was not successful.\n' + msg,
                   issue_type='Dataset Provider improvement needed')
        continue

    content = os.listdir('.')

    # check that datapackage file is not missing
    dp_file_path = os.path.join(repo_path, 'datapackage.json')
    if not os.path.isfile(dp_file_path):
        print('datapackage.json file missing or not in correct directory')
        msg = 'datapackage.json file missing or not in correct directory'
        post_issue(name='Validation error ' + repository_name,
                   description='The repository validation was not successful.\n' + msg,
                   issue_type='Dataset Provider improvement needed')
        continue

    # check that data directory is present
    data_dir_path = os.path.join(repo_path, 'data')
    if not os.path.isdir(data_dir_path):
        print('data directory missing')
        msg = 'data directory missing'
        post_issue(name='Validation error ' + repository_name,
                   description='The repository validation was not successful.\n' + msg,
                   issue_type='Dataset Provider improvement needed')
        continue

    # check properties
    missing_properties = []
    error_messages = []

    # open file
    # check if file construction is valid
    try:
        with open(dp_file_path) as f:
            dp = json.load(f)
    except json.decoder.JSONDecodeError as e:
        msg = 'JSON decoding raised an exception.\n' + str(e)
        print(msg)
        post_issue(name='Validation error ' + repository_name,
                   description='The repository validation was not successful.\n' + msg,
                   issue_type='Dataset Provider improvement needed')
        continue

    # create tags from contributors (data providers)
    try:
        contributors = dp['contributors']
        tags = []
        for c in contributors:
            print(c['title'])
            tags.append(c['title'])
    except KeyError as e:
        tags = []


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
    has_geom = None # variable used to detect geometries in datapacakge

    if dp_resources:
        if dp_profile == 'vector-data-resource':
            for dp_r in dp_resources:
                print('vector-data-resource')
                props = ['name', 'path', 'format', 'unit', 'vector']
                for p in props:
                    try:
                        a = dp_r[p]
                        if p == 'name':
                            if len(a) > 50:
                                error_messages.append('resource/name length is too long (max 50 char.)')
                            if a.endswith(('.csv', '.tif', '.tiff', '.shp', '.geojson', '.txt')):
                                error_messages.append('resource/name should not contain a file extension (extension is in resource/path)')
                    except KeyError as e:
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
                    dp_schema = dp_vector['schema']
                except:
                    missing_properties.append('vector/schema')
                try:
                    dp_schema = dp_vector['schema']
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
                        if p == 'name':
                            if len(a) > 50:
                                error_messages.append('resource/name length is too long (max 50 char.)')
                            if a.endswith(('.csv', '.tif', '.tiff', '.shp', '.geojson', '.txt')):
                                error_messages.append('resource/name should not contain a file extension (extension is in resource/path)')
                    except KeyError as e:
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
                        if p == 'name':
                            if len(a) > 50:
                                error_messages.append('resource/name length is too long (max 50 char.)')
                            if a.endswith(('.csv', '.tif', '.tiff', '.shp', '.geojson', '.txt')):
                                error_messages.append('resource/name should not contain a file extension (extension is in resource/path)')
                    except KeyError as e:
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
                        error_messages.append('no geometry provided (nuts/lau reference [attribute spatial_key_field and spatial_resolution] or geometry field)\n'
                                              + '\tThe dataset will be integrated as is but make sure that no geometry is needed.')

        else:
            err_msg = '\'profile\' contains an unsupported value! Use only vector-data-resource, raster-data-resource or tabular-data-resource'
            print(err_msg)
            error_messages.append(err_msg)

    number_of_errors = len(error_messages) + len(missing_properties)
    print("number of errors found: ", number_of_errors)
    if number_of_errors > 0:
        str_error_messages = ''
        if len(error_messages) > 0:
            str_error_messages = 'Errors: \n' + '\n'.join(error_messages)
            if len(missing_properties) > 0:
                str_error_messages = str_error_messages + '\n'
        if len(missing_properties) > 0:
            str_error_messages = 'Missing properties: \n' + '\n'.join(missing_properties)
        print('Validation error for repository ' + repository_name + '\n' + str_error_messages)

        post_issue(name='Validation error ' + repository_name,
                   description='The repository validation was not successful.\n' + str_error_messages,
                   issue_type='Dataset Provider improvement needed',
                   tags=tags)

        if has_geom is not None and number_of_errors == 1 and has_geom is False:
            pass # allow datasets without geometry
            print('Resource integration continuing despite geom error.')
        else:
            print('Resource integration aborted.')
            continue # otherwise skip dataset
    else:
        print('Validation OK')

    log_print_step("Start integration of " + repository_name)
    log_start_repo_time = log_previous_time

    repository_path = os.path.join(repositories_base_path, repository_name)

    try:
        # read datapackage.json (dp)
        print(repository_path)
        dp = json.load(open(repository_path + '/datapackage.json'))

        gis_data_type = dp['profile']
        gis_resources = dp['resources']
        dataset_version = dp['version']
        table_name = re.sub('[^A-Za-z0-9]+', '_', dp['name'].lower().replace("hotmaps", ""))

        print(dp)

        for r in gis_resources:
            log_print_step("Start resource")
            format = r['format']
            name = r['name']
            
            if name == 'wind_200m' or name == 'wind_100m' or name == 'agricultural_residues' or name == 'livestock_effluents' or name == 'space_heating_cooling_dhw_top-down':
                print(name, ' ... skipping ...')
                continue

            path = r['path']
            table_name = re.sub('[^A-Za-z0-9]+', '_', name.lower().replace("hotmaps", ""))
            print('table_name =', table_name)

            # date = r['date']
            raster_table_name = table_name
            precomputed_table_name_lau = raster_table_name + "_" + LAU_TABLE_NAME
            precomputed_table_name_nuts = raster_table_name + "_" + NUTS_TABLE_NAME

            if gis_data_type == 'vector-data-resource':
                vector = r['vector']
                proj = vector['epsg']
                geom_type = vector['geometry_type']
                schema = vector['schema']

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

                attributes_names = []
                attributes_types = []
                for att in schema:
                    col_type = att['type']
                    if col_type == 'string':
                        col_type = 'varchar(255)'
                    elif col_type == 'integer':
                        col_type = 'bigint'
                    elif col_type == 'double':
                        col_type = 'numeric(20,2)'
                    elif col_type == 'number':
                        col_type = 'numeric(20,2)'
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
                        print('Unhandled table type', col_type)
                        post_issue(name='Integration warning - repository ' + repository_name,
                            description=col_type + ' column type not supported.\n',
                            issue_type='Dataset Provider improvement needed', tags=tags)
                        continue

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
                elif geom_type.lower() == 'multipolygon':
                    geom_type = 'MultiPolygon'
                elif geom_type.lower() == 'point':
                    geom_type = 'Point'
                elif geom_type.lower() == 'multipoint':
                    geom_type = 'MultiPoint'
                elif geom_type.lower() == 'multilinestring':
                    geom_type = 'MultiLinestring'
                elif geom_type.lower() == 'linestring':
                    geom_type = 'Linestring'
                else:
                    print('geometry_type is not set correctly')


                db_attributes_types.append('geometry(' + geom_type + ', ' + proj + ')')

                # drop table
                print(GEO_SCHEMA)
                db.drop_table(table_name=GEO_SCHEMA + '.' + table_name, cascade=True)
                # create table if not exists
                db.create_table(table_name=GEO_SCHEMA + '.' + table_name, col_names=db_attributes_names,
                                col_types=db_attributes_types, id_col_name='gid')

                log_print_step("Start shapefile importation")
                # import shapefile
                import_shapefile(os.path.join(repository_path, path), start_date, attributes_names)  # (base_path, 'git-repos', repository_name, path))

                log_print_step("Start geoserver integration")
                # add to geoserver
                workspace = GEO_workspace
                store = GEO_db_store
                layer_name = table_name

                # remove previous layer from geoserver
                # remove layer
                response = requests.delete(
                    GEO_url + ':' + GEO_port + '/geoserver/rest/layers/' + layer_name,
                    auth=(GEO_user, GEO_password),
                )
                print(response, response.content)

                # remove feature type
                response = requests.delete(
                    GEO_url + ':' + GEO_port + '/geoserver/rest/workspaces/' + workspace + '/datastores/' + store + '/featuretypes/' + layer_name,
                    auth=(GEO_user, GEO_password),
                )
                print(response, response.content)

                # create layer
                headers = {
                    'Content-type': 'text/xml',
                }
                data = '<featureType>' \
                       + '<name>' + layer_name + '</name>' \
                       + '<title>' + layer_name + '</title>' \
                       + '<srs>EPSG:' + proj + '</srs>' \
                       + '</featureType>'

                response = requests.post(
                    GEO_url + ':' + GEO_port + '/geoserver/rest/workspaces/' + workspace + '/datastores/' + store + '/featuretypes/',
                    headers=headers,
                    data=data,
                    auth=(GEO_user, GEO_password),
                )
                print(data)
                print(response, response.content)

            elif gis_data_type == 'raster-data-resource':
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
                    start_date = '1970-01-01 00:00:00'
                    end_date = '1970-01-01 00:00:00'
                    pass

                # temporal resolution
                temporal_resolution = ''
                try:
                    tr = r['temporal_resolution']
                except:
                    print('Missing attribute temporal_resolution in datapackage.json. Using year as default')
                    tr = 'year'
                if tr.lower().startswith('year'):
                    temporal_resolution = 'year'
                elif tr.lower().startswith('month'):
                    temporal_resolution = 'month'
                elif tr.lower().startswith('day'):
                    temporal_resolution = 'day'
                elif tr.lower().startswith('hour'):
                    temporal_resolution = 'hour'
                elif tr.lower().startswith('minute'):
                    temporal_resolution = 'minute'
                elif tr.lower().startswith('second'):
                    temporal_resolution = 'second'
                elif tr.lower().startswith('quarter'):
                    temporal_resolution = 'quarter'
                elif tr.lower().startswith('week'):
                    temporal_resolution = 'week'

                # number_of_bands = raster['number_of_bands']
                # band0 = raster['band0']
                raster_path = os.path.join(repository_path, path)  # (base_path, 'git-repos', repository_name, path)

                os.environ['PGHOST'] = DB_host
                os.environ['PGPORT'] = DB_port
                os.environ['PGUSER'] = DB_user
                os.environ['PGPASSWORD'] = DB_password
                os.environ['PGDATABASE'] = DB_database

                rast_tbl = GEO_SCHEMA + '.' + raster_table_name

                log_print_step("Start raster integration in database")
                #cmds = 'cd ' + repository_path + '/data ; raster2pgsql -d -s ' + proj + ' -t "auto" -I -C -Y "' + name + '" ' + rast_tbl + ' | psql'
                db.drop_table(table_name=rast_tbl, notices=verbose, cascade=True)
                # create table
                cmds = 'raster2pgsql -p -s ' + proj + ' -t "auto" -I -C -Y "' + raster_path + '" ' + rast_tbl + ' | psql'
                subprocess.call(cmds, shell=True)
                # customize autovacuum settings
                #db.query(commit=True, notices=verbose, query='ALTER TABLE ' + rast_tbl + ' SET (autovacuum_vacuum_scale_factor = 0.0); ALTER TABLE ' + rast_tbl + ' SET (autovacuum_vacuum_threshold = 5000); ALTER TABLE ' + rast_tbl + ' SET (autovacuum_analyze_scale_factor = 0.0); ALTER TABLE ' + rast_tbl + ' SET (autovacuum_analyze_threshold = 5000);')
                #db.query(commit=True, notices=verbose, query='ALTER TABLE ' + rast_tbl + ' SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);')
                # add time column
                constraints = "ALTER TABLE " + rast_tbl + " " \
                              + "ADD COLUMN IF NOT EXISTS fk_" + TIME_TABLE_NAME + "_id bigint; "
                constraints = constraints + "DO $$ BEGIN IF NOT EXISTS (" \
                              + "SELECT 1 FROM pg_constraint WHERE conname = \'" + raster_table_name + "_" + TIME_TABLE_NAME + "_id_fkey\') THEN " \
                              + "ALTER TABLE " + rast_tbl + " " \
                              + "ADD CONSTRAINT " + raster_table_name + "_" + TIME_TABLE_NAME + "_id_fkey " \
                              + "FOREIGN KEY (fk_" + TIME_TABLE_NAME + "_id) " \
                              + "REFERENCES " + TIME_TABLE + "(id) " \
                              + "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE SET NULL; " \
                              + "END IF; END; $$; "
                db.query(commit=True, notices=verbose, query=constraints)
                # insert data
                cmds = 'raster2pgsql -a -s ' + proj + ' -t "auto" -I -C -Y -e "' + raster_path + '" ' + rast_tbl + ' | psql'
                print(cmds)
                subprocess.call(cmds, shell=True)

                # add time relationship in raster table
                #constraints = "ALTER TABLE " + rast_tbl + " " \
                #              + "ADD COLUMN IF NOT EXISTS fk_" + TIME_TABLE_NAME + "_id bigint; "
                #constraints = constraints + "DO $$ BEGIN IF NOT EXISTS (" \
                #              + "SELECT 1 FROM pg_constraint WHERE conname = \'" + raster_table_name + "_" + TIME_TABLE_NAME + "_id_fkey\') THEN " \
                #              + "ALTER TABLE " + rast_tbl + " " \
                #              + "ADD CONSTRAINT " + raster_table_name + "_" + TIME_TABLE_NAME + "_id_fkey " \
                #              + "FOREIGN KEY (fk_" + TIME_TABLE_NAME + "_id) " \
                #              + "REFERENCES " + TIME_TABLE + "(id) " \
                #              + "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE SET NULL; " \
                #              + "END IF; END; $$; "

                fk_time_id = get_or_create_time_id(timestamp=start_date, granularity=temporal_resolution)
                print('fk_time_id=', fk_time_id)

                query = "UPDATE " + rast_tbl + " AS r " \
                        + "SET fk_" + TIME_TABLE_NAME + "_id = " + str(fk_time_id) + " " \
                        + "WHERE fk_" + TIME_TABLE_NAME + "_id IS NULL;"

                db.query(commit=True, notices=verbose, query=query)

                # Precompute layers for nuts and lau
                # LAU
                log_print_step("Precompute LAU")
                vect_tbl = LAU_TABLE
                vect_tbl_name = LAU_TABLE_NAME
                prec_tbl = STAT_SCHEMA + '.' + precomputed_table_name_lau
                prec_tbl_name = precomputed_table_name_lau

                db.drop_table(table_name=prec_tbl, notices=verbose, cascade=True)

                attributes_names = (
                    'count', 'sum', 'mean', 'stddev', 'min', 'max',
                    'comm_id',
                    'fk_' + TIME_TABLE_NAME + '_id', 'fk_' + vect_tbl_name + '_gid')
                attributes_types = (
                    'bigint', 'numeric(20,2)', 'numeric(20,2)', 'numeric(20,2)', 'numeric(20,2)', 'numeric(20,2)',
                    'varchar(255)',
                    'bigint', 'bigint')

                constraints = "ALTER TABLE " + prec_tbl + " " \
                              + "ADD CONSTRAINT " + prec_tbl_name + "fkey_" + vect_tbl_name + "_gid_fkey " \
                              + "FOREIGN KEY (fk_" + vect_tbl_name + "_gid) " \
                              + "REFERENCES " + vect_tbl + "(gid) " \
                              + "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION; "

                constraints = constraints + "ALTER TABLE " + prec_tbl + " " \
                              + "ADD CONSTRAINT " + prec_tbl_name + "_" + TIME_TABLE_NAME + "_id_fkey " \
                              + "FOREIGN KEY (fk_" + TIME_TABLE_NAME + "_id) " \
                              + "REFERENCES " + TIME_TABLE + "(id) " \
                              + "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE SET NULL "


                db.create_table(table_name=prec_tbl,
                                col_names=attributes_names,
                                col_types=attributes_types,
                                constraints_str=constraints,
                                notices=verbose)

                query = """
SELECT (
  SELECT (ST_SummaryStatsAgg(ST_Clip(rast, 1, ST_Transform({vect_tbl}.geom, {RASTER_SRID}), true), 1, true))
  FROM (
    SELECT ST_Union(rast) as rast 
    FROM (
      SELECT rast
      FROM {rast_tbl}
      WHERE ST_Intersects(
        {rast_tbl}.rast, ST_Transform({vect_tbl}.geom, {RASTER_SRID})
      )
      AND fk_{TIME_TABLE_NAME}_id = {fk_time_id}
    ) as rast
  ) as rast
).*, {vect_tbl}.comm_id, {fk_time_id} AS fk_{TIME_TABLE_NAME}_id, {vect_tbl}.gid
FROM public.lau
""".format(
    vect_tbl=vect_tbl,
    rast_tbl=rast_tbl,
    RASTER_SRID=str(RASTER_SRID),
    TIME_TABLE_NAME=TIME_TABLE_NAME,
    fk_time_id=str(fk_time_id)
)

                db.query(commit=True, notices=verbose, query='INSERT INTO ' + prec_tbl
                    + ' (' + ', '.join(
                        map(db_helper.str_with_quotes, [x.lower() for x in attributes_names])) + ') '
                        + query + ' ;')

                # NUTS
                log_print_step("Precompute NUTS 3")

                prec_lau_tbl = prec_tbl
                vect_tbl = GEO_SCHEMA + '.' + NUTS_TABLE_NAME
                prec_tbl = STAT_SCHEMA + '.' + precomputed_table_name_nuts
                vect_tbl_name = NUTS_TABLE_NAME
                prec_tbl_name = precomputed_table_name_nuts

                db.drop_table(table_name=prec_tbl, notices=verbose, cascade=True)

                attributes_names = (
                    'min', 'max', 'sum', 'count',
                    'mean', 'nuts_id', 'stat_levl_',
                    'fk_' + TIME_TABLE_NAME + '_id', 'fk_' + vect_tbl_name + '_gid')

                constraints = "ALTER TABLE " + prec_tbl + " " \
                              + "ADD CONSTRAINT " + prec_tbl_name + "_" + vect_tbl_name + "_gid_fkey " \
                              + "FOREIGN KEY (fk_" + vect_tbl_name + "_gid) " \
                              + "REFERENCES " + vect_tbl + "(gid) " \
                              + "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE SET NULL; "

                constraints = constraints + "ALTER TABLE " + prec_tbl + " " \
                              + "ADD CONSTRAINT " + prec_tbl_name + "_" + TIME_TABLE_NAME + "_id_fkey " \
                              + "FOREIGN KEY (fk_" + TIME_TABLE_NAME + "_id) " \
                              + "REFERENCES " + TIME_TABLE + "(id) " \
                              + "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE SET NULL "

                db.create_table(table_name=prec_tbl,
                                col_names=attributes_names,
                                col_types=('numeric(20,2)', 'numeric(20,2)', 'numeric(20,2)', 'bigint',
                                           'numeric(20,2)', 'varchar(255)', 'integer',
                                           'bigint', 'bigint'),
                                constraints_str=constraints,
                                notices=verbose)

                # Compute NUTS 3 from LAU
                query = """
with lau as (select s.count, s.sum, s.mean, s.stddev, s.min, s.max, s.comm_id, l.fk_{0}_gid, l.nuts_id, s.fk_{1}_id
from {2} s
left join {3} l on l.comm_id = s.comm_id
)
insert into {4} (min, max, sum, count, mean, nuts_id, stat_levl_, fk_{1}_id, fk_{0}_gid)
(select min(lau.min), max(lau.max), sum(lau.sum), sum(lau.count) as count, sum(lau.sum) / cast(sum(lau.count) as numeric(20,2)) as mean,
n.nuts_id, n.stat_levl_, lau.fk_{1}_id, lau.fk_{0}_gid
from {5} n
right join lau on lau.fk_{0}_gid = n.gid
where n.stat_levl_ = 3 and n.year = '2013-01-01'
group by n.gid, n.stat_levl_, lau.fk_{0}_gid, lau.fk_time_id)
;
""".format(
    vect_tbl_name,   # 0
    TIME_TABLE_NAME, # 1
    prec_lau_tbl,    # 2
    LAU_TABLE,       # 3
    prec_tbl,        # 4
    vect_tbl,        # 5
    )

                db.query(commit=True, notices=verbose, query=query)

                # compute NUTS 0-2 from NUTS 3
                log_print_step("Precompute NUTS 0-2")
                query = """
with nuts3 as (select s.count, s.sum, s.mean, s.min, s.max, s.nuts_id, s.stat_levl_, s.fk_{0}_gid, s.fk_{1}_id
from {4} s
where s.stat_levl_ = 3
)
insert into {4} (min, max, sum, count, mean, nuts_id, stat_levl_, fk_{1}_id, fk_{0}_gid)
(select min(nuts3.min), max(nuts3.max), sum(nuts3.sum), sum(nuts3.count) as count, sum(nuts3.sum) / cast(sum(nuts3.count) as numeric(20,2)) as mean,
n.nuts_id, n.stat_levl_, nuts3.fk_{1}_id, nuts3.fk_{0}_gid
from {5} n
right join nuts3 on nuts3.nuts_id LIKE n.nuts_id || '%'
where nuts3.count is not null and n.stat_levl_ < 3 and n.year = '2013-01-01'
group by n.gid, n.stat_levl_, nuts3.fk_{0}_gid, nuts3.fk_{1}_id)
;
""".format(
    vect_tbl_name,   # 0
    TIME_TABLE_NAME, # 1
    prec_lau_tbl,    # 2
    LAU_TABLE,       # 3
    prec_tbl,        # 4
    vect_tbl,        # 5
)
                # query = "INSERT INTO " + prec_tbl + " " \
                #         + "(min, max, sum, count, mean, nuts_id, stat_levl_, fk_" + TIME_TABLE_NAME + "_id, fk_" + vect_tbl_name + "_gid) " \
                #         + "(SELECT i.min, i.max, i.sum, i.count, i.sum / cast(i.count as numeric(20,2)) as mean, " \
                #         + "i.nuts_id, i.stat_levl_, i.fk_" + TIME_TABLE_NAME + "_id, i.gid " \
                #         + "FROM " \
                #         + "(SELECT min(intr.min), max(intr.max), sum(intr.sum), sum(intr.count) as count, " \
                # 	 	+ "n.nuts_id, n.stat_levl_, intr.fk_" + TIME_TABLE_NAME + "_id, n.gid " \
                # 		+ "FROM ( " \
                # 		+ "SELECT data_tbl.nuts_id, data_tbl.fk_" + vect_tbl_name + "_gid, data_tbl.stat_levl_, " \
                #         + "data_tbl.min, data_tbl.max, data_tbl.sum, data_tbl.count, data_tbl.mean, " \
                # 		+ "data_tbl.fk_" + TIME_TABLE_NAME + "_id " \
                # 		+ "FROM " + prec_tbl + " data_tbl " \
                # 		+ "WHERE data_tbl.stat_levl_ = 3 " \
                # 		+ ") as intr " \
                # 	 	+ "RIGHT JOIN " + vect_tbl + " n ON intr.nuts_id LIKE n.nuts_id || '%' " \
                # 		+ "WHERE intr.count IS NOT NULL  " \
                # 	 	+ "AND n.stat_levl_ < 3 " \
                # 	 	+ "AND n.year = '2013-01-01' " \
                #         + "GROUP BY n.gid, n.nuts_id, n.stat_levl_, intr.fk_" + TIME_TABLE_NAME + "_id " \
                # 	    + ") as i " \
                #         + "); "

                db.query(commit=True, notices=verbose, query=query)

                # db.query(commit=True, notices=verbose, query='INSERT INTO ' + prec_tbl
                #                                              + ' (' + ', '.join(
                #     map(db_helper.str_with_quotes, [x.lower() for x in attributes_names])) + ') '
                #                                              + query + ' ;')

                log_print_step("Generate image pyramid for geoserver")
                # build pyramid and add layer to geoserver
                pyramid_path = os.path.join(GEO_base_path, raster_table_name)

                # build pyramid using gdal_retile script
                # todo: could improve by enabling "COMPRESS=JPEG" option, but doing so raised error
                cmds = 'cd ' + os.path.join(repository_path,
                                            'data') + ' ; rm -r ' + pyramid_path + ' ; mkdir ' + pyramid_path + ' ; gdal_retile.py -v -r bilinear ' + \
                       '-levels ' + str(GEO_number_of_pyarmid_levels) + ' ' + \
                       '-ps 2048 2048 -co "TILED=YES" -co "COMPRESS=LZW" ' + \
                       '-targetDir ' + pyramid_path + ' ' + raster_path
                print(cmds)
                subprocess.call(cmds, shell=True)

                # add to geoserver
                log_print_step("Add to geoserver")
                workspace = GEO_workspace
                layer_name = raster_table_name

                # remove coverage store from geoserver
                response = requests.delete(
                    GEO_url + ':' + GEO_port + '/geoserver/rest/workspaces/' + workspace + '/coveragestores/' + layer_name + '?recurse=true',
                    auth=(GEO_user, GEO_password),
                )
                print(response, response.content)
                # create data store
                headers = {
                    'Content-type': 'text/xml',
                }
                data = '<coverageStore>' \
                       '<name>' + layer_name + '</name>' \
                       '<workspace>' + workspace + '</workspace>' \
                       '<enabled>true</enabled>' \
                       '<type>ImagePyramid</type>' \
                       '<url>file:raster-layers/' + layer_name + '</url>' \
                       '</coverageStore>'

                response = requests.post(
                    GEO_url + ':' + GEO_port + '/geoserver/rest/workspaces/' + workspace + '/coveragestores?configure=all',
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
                    GEO_url + ':' + GEO_port + '/geoserver/rest/workspaces/' + workspace + '/coveragestores/' + layer_name + '/coverages',
                    headers=headers,
                    data=data,
                    auth=(GEO_user, GEO_password),
                )
                # print(data)
                print(response, response.content)

                # apply style
                data = '<layer><defaultStyle><name>' + layer_name + '</name></defaultStyle></layer>'

                response = requests.put(
                    GEO_url + ':' + GEO_port + '/geoserver/rest/layers/' + workspace + ':' + layer_name,
                    headers=headers,
                    data=data,
                    auth=(GEO_user, GEO_password),
                )
                print(response, response.content)

            elif gis_data_type == 'tabular-data-resource':
                log_print_step("Read datapackage for CSV")

                schema = r['schema']

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

                # file path
                tabular_file_path = repository_path + '/' + path

                # retrieve csv dialect and encoding
                delimiter = ','
                double_quote = True
                encoding = 'utf-8'
                #lineterminator = '\r\n'
                try:
                    dialect = r['dialect']
                except:
                    print('No dialect provided for this resource')
                try:
                    delimiter = dialect['delimiter']
                except:
                    print('No delimiter provided for this dialect')
                try:
                    double_quote = dialect['doubleQuote']
                except:
                    print('No doubleQuote provided for this dialect')
                # lineterminator = dialect['lineterminator']
                try:
                    encoding = r['encoding']
                except:
                    print('No encoding provided for this resource')

                fields = schema['fields']

                attributes_names = []
                db_attributes_names = []
                attributes_types = []
                attributes_units = []

                count_geom_cols = 0

                for att in fields:
                    col_type = att['type'].lower()
                    if col_type == 'string':
                        col_type = 'varchar(255)'
                        att['unit'] = '' # prevent error if user set unit on geometry reference column
                    elif col_type == 'integer':
                        col_type = 'bigint'
                    elif col_type == 'number':
                        col_type = 'numeric(20,2)'
                    elif col_type == 'double':
                        col_type = 'numeric(20,2)'
                    elif col_type == 'float':
                        col_type = 'numeric(20,2)'
                    elif col_type == 'boolean':
                        col_type = 'boolean'
                        att['unit'] = '' # prevent error if user set unit on geometry reference column
                    elif col_type == 'date':
                        col_type = 'date'
                        att['unit'] = '' # prevent error if user set unit on geometry reference column
                    elif col_type == 'datetime':
                        col_type = 'timestamp'
                        att['unit'] = '' # prevent error if user set unit on geometry reference column
                    elif col_type == 'timestamp':
                        col_type = 'timestamp'
                        att['unit'] = '' # prevent error if user set unit on geometry reference column
                    elif col_type == 'geom' or col_type == 'geometry':
                        col_type = 'geometry'
                        count_geom_cols = count_geom_cols + 1
                        att['unit'] = '' # prevent error if user set unit on geometry reference column
                    else:
                        print('Unhandled table type', col_type)
                        post_issue(name='Integration warning - repository ' + repository_name,
                            description=col_type + ' column type not supported.\n',
                            issue_type='Dataset Provider improvement needed', tags=tags)
                        continue

                    col_name = re.sub('[^A-Za-z0-9]+', '_', att['name'].lower()) # prevent unwanted chars
                    while col_name in attributes_names: # append '_1' if duplicate
                        col_name = col_name + '_1'
                    attributes_names.append(att['name'])
                    db_attributes_names.append(col_name)
                    attributes_types.append(col_type)
                    attributes_units.append(att['unit'])

                #db_attributes_names = list(attributes_names)
                db_attributes_types = list(attributes_types)

                valuesUnit = []
                valuesUnitName = []
                valuesUnitType = []
                for i in range(0, len(attributes_names)):
                    if attributes_units[i] and attributes_units[i] != "":
                        valuesUnitName.append(db_attributes_names[i] + "_unit")
                        valuesUnit.append(attributes_units[i])
                        valuesUnitType.append("varchar(255)")

                if valuesUnit and len(valuesUnit) > 0:
                    db_attributes_names.extend(valuesUnitName)
                    db_attributes_types.extend(valuesUnitType)

                # db_attributes_names.append('start_date')
                # db_attributes_types.append('timestamp')
                #
                # db_attributes_names.append('end_date')
                # db_attributes_types.append('timestamp')

                log_print_step("Drop current table")
                db.drop_table(table_name=STAT_SCHEMA + '.' + table_name, cascade=True)

                # spatial resolution
                spatial_table = None
                spatial_resolution = None
                spatial_field_name = None

                missing_geometry = False
                try:
                    spatial_resolution = r['spatial_resolution']
                    spatial_field_name = r['spatial_key_field']

                    missing_geometry = spatial_resolution.lower() == 'none' and count_geom_cols <= 0

                except:
                    print('No spatial field/resolution specified in datapackage.json')
                    missing_geometry = count_geom_cols <= 0
                    # with open(tabular_file_path, "rb") as f:
                    #     reader = csv.reader(f)
                    #     column_names = next(reader)

                if missing_geometry is True:
                    print('No spatial reference or geometry provided. The dataset should contain at least one geolocalized data. The dataset will be integrated as is but make sure that no geometry is needed.')
                    # post_issue(name='Integration of resource failed - repository ' + repository_name,
                    #            description='No spatial reference or geometry provided for resource "' + name + '". The resource has been skipped. '
                    #                      + 'The dataset must contain at least one geolocalized data (geometry or reference to spatial resolutions (NUTS/LAU)). '
                    #                      + 'Make sure that the geometry column is of type "geometry" or that "spatial_resolution" and "spatial_key_field" attributes are correctly declared in the "datapackage.json" file. '
                    #                      + 'The dataset will be integrated as is but if you get this error and your dataset should contain a geometry, please make sure it is declared correctly. Otherwise you might delete this issue.')
                    # continue # turned off to allow the integration of datasets without geometry. uncomment to restrict
                else:
                    if spatial_resolution and spatial_resolution.lower().startswith("nuts"):
                        spatial_table_name = NUTS_TABLE_NAME
                        spatial_table = NUTS_TABLE
                        spatial_type = 'N'
                    elif spatial_resolution and spatial_resolution.lower().startswith("lau"):
                        spatial_table_name = LAU_TABLE_NAME
                        spatial_table = LAU_TABLE
                        spatial_type = 'L'

                # temporal resolution
                temporal_resolution = ''
                try:
                    tr = r['temporal_resolution']
                except:
                    print('Missing attribute temporal_resolution in datapackage.json. Using year as default')
                    tr = 'year'
                if tr.lower().startswith('year'):
                    temporal_resolution = 'year'
                elif tr.lower().startswith('month'):
                    temporal_resolution = 'month'
                elif tr.lower().startswith('day'):
                    temporal_resolution = 'day'
                elif tr.lower().startswith('hour'):
                    temporal_resolution = 'hour'
                elif tr.lower().startswith('minute'):
                    temporal_resolution = 'minute'
                elif tr.lower().startswith('second'):
                    temporal_resolution = 'second'
                elif tr.lower().startswith('quarter'):
                    temporal_resolution = 'quarter'
                elif tr.lower().startswith('week'):
                    temporal_resolution = 'week'

                constraints = ""
                if spatial_table is not None and missing_geometry is False:
                    # add spatial relationship in table
                    constraints = constraints + "DO $$ BEGIN IF NOT EXISTS (" \
                                  + "SELECT 1 FROM pg_constraint WHERE conname = \'" + table_name + "_" + spatial_table_name + "_gid_fkey\') THEN " \
                                  + "ALTER TABLE " + STAT_SCHEMA + '.' + table_name + " " \
                                  + "ADD CONSTRAINT " + table_name + "_" + spatial_table_name + "_gid_fkey " \
                                  + "FOREIGN KEY (fk_" + spatial_table_name + "_gid) " \
                                  + "REFERENCES " + spatial_table + "(gid) " \
                                  + "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE SET NULL; " \
                                  + "END IF; END; $$; "

                    db_attributes_names.append('fk_' + spatial_table_name + '_gid')
                    db_attributes_types.append('bigint')

                if temporal_resolution is not None and len(temporal_resolution) > 0:
                    # add temporal relationship in table
                    constraints = constraints + "DO $$ BEGIN IF NOT EXISTS (" \
                                  + "SELECT 1 FROM pg_constraint WHERE conname = \'" + table_name + "_" + TIME_TABLE_NAME + "_id_fkey\') THEN " \
                                  + "ALTER TABLE " + STAT_SCHEMA + '.' + table_name + " " \
                                  + "ADD CONSTRAINT " + table_name + "_" + TIME_TABLE_NAME + "_id_fkey " \
                                  + "FOREIGN KEY (fk_" + TIME_TABLE_NAME + "_id) " \
                                  + "REFERENCES " + TIME_TABLE + "(id) " \
                                  + "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE SET NULL; " \
                                  + "END IF; END; $$; "

                    db_attributes_names.append('fk_' + TIME_TABLE_NAME + '_id')
                    db_attributes_types.append('bigint')

                # generate table with constraints
                log_print_step("Generate table")
                db.create_table(table_name=STAT_SCHEMA + '.' + table_name, col_names=db_attributes_names,
                                col_types=db_attributes_types, id_col_name='id', constraints_str=constraints)

                log_print_step("Integrate CSV in database")
                if encoding == 'utf-8' or encoding == 'utf_8' or encoding == 'utf8' or encoding == 'u8':
                    encoding = 'utf-8-sig'
                file = open(tabular_file_path, "r", encoding=encoding)
                csv.register_dialect('custom', delimiter=delimiter, doublequote=double_quote) # lineterminator=lineterminator
                reader = csv.DictReader(f=file, dialect='custom')

                skip_repo = False

                for row in reader:
                    values = []
                    skip = False
                    i = 0  # index
                    fk_gid = None
                    fk_time_id = None

                    for name in attributes_names:
                        try:
                            att = row[name]
                        except:
                            print(name, ' attribute does not match a column name ', row)
                            post_issue(name='Integration failed - repository ' + repository_name,
                                       description=name + ' attribute does not match a column name ' + row + '\n' + traceback.format_exc(),
                                       issue_type='Dataset Provider improvement needed', tags=tags)
                            continue

                        # check type
                        type = db_attributes_types[i]
                        if type == 'bigint' or type.startswith('numeric'):
                            if isinstance(att, str):
                                #att = None
                                pass

                        # handle spatial column
                        if missing_geometry is False:
                            if name == spatial_field_name:
                                # nuts
                                if spatial_type == 'N':  # NUTS
                                    fk_gid = db.query(commit=True,
                                                      query="SELECT gid FROM " + spatial_table + " WHERE year = '2013-01-01' AND nuts_id LIKE '" + att.upper() + "'")
                                elif spatial_type == 'L':  # LAU
                                    fk_gid = db.query(commit=True,
                                                      query="SELECT gid FROM " + spatial_table + " WHERE comm_id LIKE '" + att.upper() + "'")

                                if fk_gid is not None and len(fk_gid) > 0 and len(fk_gid[0]) > 0:
                                    fk_gid = fk_gid[0][0]
                                else:
                                    print("No geometry found for reference: " + att + ". Skipping.")
                                    skip = True

                                print('fk_gid=', fk_gid)

                        # handle temporal column
                        fk_time_id = None
                        if name == 'datetime':
                            fk_time_id = get_or_create_time_id(timestamp=att, granularity=temporal_resolution)
                            print('fk_time_id=', fk_time_id)
                        elif name == 'timestamp':
                            name = 'timestamp'
                            timestamp = datetime.fromtimestamp(att).strftime('%Y/%m/%d %H:%M:%S')
                            fk_time_id = get_or_create_time_id(timestamp=timestamp, granularity=temporal_resolution)
                            print('fk_time_id=', fk_time_id)


                        if att == '':
                            att = None
                        values.append(att)

                        i = i + 1  # increment index

                    if skip:
                        continue

                    # get year if no timestamp specified
                    if fk_time_id is None:
                        fk_time_id = get_or_create_time_id(timestamp=start_date, granularity=temporal_resolution)
                        print('fk_time_id=', fk_time_id)

                    if valuesUnit and len(valuesUnit) > 0:
                        values.extend(valuesUnit)

                    if fk_gid:
                        values.append(fk_gid)

                    if fk_time_id:
                        values.append(fk_time_id)

                    #values.append(start_date)
                    #values.append(end_date)
                    # if fk_nuts_gid is not None and len(fk_nuts_gid) > 0:
                    #     values.append(fk_nuts_gid[0][0])

                    try:
                        db.insert(commit=True, table=STAT_SCHEMA + '.' + table_name, columns=db_attributes_names, types=db_attributes_types, values=values)
                    except Exception as e:
                        msg = 'DB Exception raised during insertion.\n' + str(e)
                        print(msg)
                        post_issue(name='Integration of ' + repository_name,
                                description='The repository integration was not successful.\n' + msg,
                                issue_type='Dataset Provider improvement needed')
                        skip_repo = True
                        break # break row iterations

                if skip_repo:
                    print('Skipping repository..')
                    continue

                    #db.query(commit=True, query='INSERT INTO ' + STAT_SCHEMA + '.' + table_name + ' (' + ', '.join(
                    #    map(str_with_quotes,
                    #        [x.lower() for x in db_attributes_names])) + ')' + ' VALUES ' + '(' + ', '.join(
                    #    map(str_with_single_quotes, values)) + ')')

                if missing_geometry is False:
                    # create view for Geoserver (if contains geometries / or refs to existing geometries)
                    log_print_step("Create view for Geoserver")
                    if fk_time_id:
                        time_cols = ', ' + TIME_TABLE_NAME + '.timestamp'
                        time_join = ' LEFT OUTER JOIN ' + TIME_TABLE + ' ' + \
                                    'ON (' + table_name + '.fk_time_id = ' + TIME_TABLE_NAME + '.id)'
                    else:
                        time_cols = ''
                        time_join = ''

                    if fk_gid:
                        geom_cols = ', ' + spatial_table_name + '.*'
                        geom_join = ' LEFT OUTER JOIN ' + spatial_table + ' ' + \
                                    'ON (' + table_name + '.fk_' + spatial_table_name + '_gid = ' + spatial_table_name + '.gid)'
                    else:
                        geom_cols = ''
                        geom_join = ''

                    # filter column names already present in spatial table
                    if spatial_table is not None:
                        split_spatial_tbl = spatial_table.split('.')
                        results = db.query(query='SELECT column_name FROM information_schema.columns WHERE table_schema=\'' + split_spatial_tbl[0] + '\' AND table_name=\'' + split_spatial_tbl[1] + '\';')
                        vect_col_names = [e[0] for e in results]
                        view_col_names = [table_name+'.'+e for e in db_attributes_names if e.lower() not in vect_col_names]
                        print(vect_col_names, view_col_names)
                    else:
                        view_col_names = [table_name+'.'+e for e in db_attributes_names]

                    query = 'CREATE VIEW ' + GEO_SCHEMA + '.' + table_name + '_view ' + \
                            'AS SELECT ' + ', '.join(view_col_names) + time_cols + geom_cols + ' ' + \
                            'FROM ' + STAT_SCHEMA + '.' + table_name + \
                            time_join + geom_join + \
                            ';'

                    # add to database
                    db.query(commit=True, query=query)

                    # add to geoserver
                    log_print_step("Add to Geoserver")
                    workspace = GEO_workspace
                    store = GEO_db_store
                    layer_name = table_name + '_view'

                    # remove previous layer from geoserver
                    # remove layer
                    response = requests.delete(
                        GEO_url + ':' + GEO_port + '/geoserver/rest/layers/' + layer_name,
                        auth=(GEO_user, GEO_password),
                    )
                    print(response, response.content)

                    # remove feature type
                    response = requests.delete(
                        GEO_url + ':' + GEO_port + '/geoserver/rest/workspaces/' + workspace + '/datastores/' + store + '/featuretypes/' + layer_name,
                        auth=(GEO_user, GEO_password),
                    )
                    print(response, response.content)

                    # create layer
                    headers = {
                        'Content-type': 'text/xml',
                    }
                    data = '<featureType>' \
                           + '<name>' + layer_name + '</name>' \
                           + '<title>' + layer_name + '</title>' \
                           + '</featureType>'

                    response = requests.post(
                        GEO_url + ':' + GEO_port + '/geoserver/rest/workspaces/' + workspace + '/datastores/' + store + '/featuretypes/',
                        headers=headers,
                        data=data,
                        auth=(GEO_user, GEO_password),
                    )
                    print(data)
                    print(response, response.content)

                    # special case for industrial sites database
                    print(repository_name, r['name'])
                    name = r['name']
                    views_schema = 'public' # set to "public" to match the mess in DB (should be "geo" like every other geo-related tables)
                    # views_schema = GEO_SCHEMA
                    if repository_name == 'industrial_sites_Industrial_Database' and name == 'Industrial_Database':
                        print('Industrial_Database repository. Adding custom views (Database) and layers (Geoserver).')
                        value_fields = {}
                        value_fields['industrial_database_excess_heat'] = ['id', 'siteid', 'companyname', 'sitename', 'address', 'citycode', 'city', 'country', 'geom',
                                                       'excess_heat_100-200c', 'excess_heat_200-500c', 'excess_heat_500c', 'excess_heat_total',
                                                       'excess_heat_100-200c_unit', 'excess_heat_200-500c_unit', 'excess_heat_500c_unit', 'excess_heat_total_unit']
                        value_fields['industrial_database_subsector'] = ['id', 'geom', 'subsector']
                        value_fields['industrial_database_emissions'] = ['id', 'siteid', 'companyname', 'address', 'citycode', 'city', 'country', 'geom', 'subsector', 'datasource',
                                                     'emissions_ets_2014', 'emissions_eprtr_2014', 'emissions_ets_2014_unit', 'emissions_eprtr_2014_unit']
                        value_fields['industrial_database_compagnyname'] = ['id', 'companyname', 'geom']

                        for table_name, view_col_names in value_fields.items():
                            log_print_step("Create view for Geoserver")
                            if fk_time_id:
                                time_cols = ', ' + TIME_TABLE_NAME + '.timestamp'
                                time_join = ' LEFT OUTER JOIN ' + TIME_TABLE + ' ' + \
                                            'ON (' + name + '.fk_time_id = ' + TIME_TABLE_NAME + '.id)'
                            else:
                                time_cols = ''
                                time_join = ''

                            if fk_gid:
                                geom_cols = ', ' + spatial_table_name + '.*'
                                geom_join = ' LEFT OUTER JOIN ' + spatial_table + ' ' + \
                                            'ON (' + name + '.fk_' + spatial_table_name + '_gid = ' + spatial_table_name + '.gid)'
                            else:
                                geom_cols = ''
                                geom_join = ''

                            # filter column names already present in spatial table
                            # if spatial_table is not None:
                            #     split_spatial_tbl = spatial_table.split('.')
                            #     results = db.query(query='SELECT column_name FROM information_schema.columns WHERE table_schema=\'' + split_spatial_tbl[0] + '\' AND table_name=\'' + split_spatial_tbl[1] + '\';')
                            #     vect_col_names = [e[0] for e in results]
                            #     view_col_names = [table_name+'.'+e for e in db_attributes_names if e.lower() not in vect_col_names]
                            #     print(vect_col_names, view_col_names)
                            # else:
                            #     view_col_names = [table_name+'.'+e for e in db_attributes_names]

                            query = 'CREATE VIEW ' + views_schema + '.' + table_name + ' ' + \
                                    'AS SELECT ' + ', '.join(['industrial_database.' + c.replace('-', '_') for c in view_col_names]) + time_cols + geom_cols + ' ' + \
                                    'FROM ' + STAT_SCHEMA + '.' + name + \
                                    time_join + geom_join + \
                                    ';'

                            # add to database
                            db.query(commit=True, query=query)

                            add_to_geoserver = False

                            if add_to_geoserver:
                                # add to geoserver
                                log_print_step("Add to Geoserver")
                                workspace = GEO_workspace
                                store = GEO_db_store
                                layer_name = table_name

                                # remove previous layer from geoserver
                                # remove layer
                                response = requests.delete(
                                    GEO_url + ':' + GEO_port + '/geoserver/rest/layers/' + layer_name,
                                    auth=(GEO_user, GEO_password),
                                )
                                print(response, response.content)

                                # remove feature type
                                response = requests.delete(
                                    GEO_url + ':' + GEO_port + '/geoserver/rest/workspaces/' + workspace + '/datastores/' + store + '/featuretypes/' + layer_name,
                                    auth=(GEO_user, GEO_password),
                                )
                                print(response, response.content)

                                # create layer
                                headers = {
                                    'Content-type': 'text/xml',
                                }
                                data = '<featureType>' \
                                       + '<name>' + layer_name + '</name>' \
                                       + '<title>' + layer_name + '</title>' \
                                       + '</featureType>'

                                response = requests.post(
                                    GEO_url + ':' + GEO_port + '/geoserver/rest/workspaces/' + workspace + '/datastores/' + store + '/featuretypes/',
                                    headers=headers,
                                    data=data,
                                    auth=(GEO_user, GEO_password),
                                )
                                print(data)
                                print(response, response.content)

            else:
                print('Unknown GEO data type, only vector-data-resource/raster-data-resource/tabular-data-resource')

            update_or_create_repo(repository_name, listOfRepoIds[repository_name])

            print("End of integration of repository ", repository_name)
            log_end_time = time()
            print(strftime("%Y-%m-%d %H:%M:%S +0000", gmtime(log_end_time)))
            hours, rem = divmod(log_end_time-log_start_repo_time, 3600)
            minutes, seconds = divmod(rem, 60)
            print("Duration of integration: {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))

    except Exception as e:
        print(str(e))
        logging.error(traceback.format_exc())
        post_issue(name='Integration failed - repository ' + repository_name,
                   description='A problem occurred during the integration process of the repository. Please contact the development team.\n' + traceback.format_exc(),
                   issue_type='Integration script execution')

db.close_connection()
log_end_time = time()
print(strftime("%Y-%m-%d %H:%M:%S +0000", gmtime(log_end_time)))
hours, rem = divmod(log_end_time-log_start_time, 3600)
minutes, seconds = divmod(rem, 60)
print("Script execution time: {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))
print("--- %s seconds ---" % (log_end_time - log_start_time))
