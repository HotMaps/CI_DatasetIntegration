import osgeo.ogr
from ci_secrets.secrets import DB_password, DB_database, DB_host, DB_port, DB_user, GIT_base_path, GEO_base_path, \
    GEO_number_of_pyarmid_levels, GEO_user, GEO_password, GEO_url, GEO_port, GEO_workspace, GEO_db_store, TAIGA_token, GIT_token, SERVER, DEBUG
from config import STAT_SCHEMA, GEO_SCHEMA, LAU_TABLE, LAU_TABLE_NAME, NUTS_TABLE, NUTS_TABLE_NAME, VECTOR_SRID, RASTER_SRID, TIME_TABLE, TIME_TABLE_NAME
from db import db_helper
import csv

def update_shapefile(src_file):
    # update shp
    # src_file = os.path.join("git-repos", "HotmapsLAU", "data", "HotmapsLAU.shp")

    # connect to atabaselistOfRepositories
    db = db_helper.DB(host=DB_host, port=str(DB_port), database=DB_database, user=DB_user, password=DB_password)

    shapefile = osgeo.ogr.Open(src_file)
    layer = shapefile.GetLayer(0)
    for i in range(layer.GetFeatureCount()):
        feature = layer.GetFeature(i)

        geom = feature.GetGeometryRef()

        # convert Polygon type to MultiPolygon
        if geom.GetGeometryType() == osgeo.ogr.wkbPolygon:
            geom = osgeo.ogr.ForceToMultiPolygon(geom)
        # export as WKT
        wkt = geom.ExportToWkt()
        query = 'UPDATE public.lau SET (geom = ST_GeomFromText(\'' + wkt + '\', ' + str(3035) + ') WHERE comm_id = '  + db_helper.str_with_single_quotes(feature.GetField('comm_id'))

        db.query(commit=True,
                 query=query
                )

    db.close_connection()


def compute(datasetname, srid):
    vect_tbl = LAU_TABLE
    rast_tbl = 'geo.' + datasetname

    query = """
WITH agg AS (
    SELECT (
        SELECT (ST_SummaryStatsAgg(ST_Clip(rast, 1, ST_Transform({vect_tbl}.geom, {rast_srid}), true), 1, true))
        FROM (
            SELECT ST_Union(rast) as rast 
            FROM (
                SELECT rast
                FROM {rast_tbl}
                WHERE ST_Intersects({rast_tbl}.rast, ST_Transform({vect_tbl}.geom, {rast_srid}))) as rast
            ) as rast
        ).*
    FROM {vect_tbl}
    WHERE {vect_tbl}.comm_id = 
)
""".format(
        vect_tbl=vect_tbl,
        rast_tbl=rast_tbl,
        rast_srid=srid
    )

    lau_rast_tbl = 'stat.' + datasetname + '_lau'
    
    queryUpdate = """
{subquery}
UPDATE {lau_rast_tbl} SET 
(count, mean, stddev, min, max) =
(agg.count, agg.mean, agg.stddev, agg.min, agg.max)
""".format(lau_rast_tbl=lau_rast_tbl, subquery=query)
    
    print(queryUpdate)
    exit()
                    
    # db.query(commit=True, notices=verbose, query='INSERT INTO ' + prec_tbl
    #     + ' (' + ', '.join(
    #         map(db_helper.str_with_quotes, [x.lower() for x in attributes_names])) + ') '
    #         + query + ' ;')



#update_shapefile("dk-lau-update/lau-update-dk.shp")

dataset = []

with open('dk-lau-update/list_of_datasets.txt', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        compute(row['dataset_name'], row['srid'])
