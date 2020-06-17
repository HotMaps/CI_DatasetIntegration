import osgeo.ogr
from ci_secrets.secrets import DB_password, DB_database, DB_host, DB_port, DB_user, GIT_base_path, GEO_base_path, \
    GEO_number_of_pyarmid_levels, GEO_user, GEO_password, GEO_url, GEO_port, GEO_workspace, GEO_db_store, TAIGA_token, GIT_token, SERVER, DEBUG
from config import STAT_SCHEMA, GEO_SCHEMA, LAU_TABLE, LAU_TABLE_NAME, NUTS_TABLE, NUTS_TABLE_NAME, VECTOR_SRID, RASTER_SRID, TIME_TABLE, TIME_TABLE_NAME
from db import db_helper
import csv

def update_shapefile(src_file):
    print('Update LAU dataset')
    # update shp

    # connect to database
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
        query = 'UPDATE public.lau SET geom = ST_GeomFromText(\'' + wkt + '\', ' + str(3035) + ') WHERE comm_id = '  + db_helper.str_with_single_quotes(feature.GetField('comm_id'))

        db.query(commit=True, query=query)

    db.close_connection()


def compute(datasetname, srid):
    vect_tbl = LAU_TABLE
    rast_tbl = 'geo.' + datasetname

    # connect to database
    db = db_helper.DB(host=DB_host, port=str(DB_port), database=DB_database, user=DB_user, password=DB_password)


    # Compute LAU
    print("Precompute LAU")

    precompute_query = """
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
        ).*, {vect_tbl}.comm_id, {vect_tbl}.gid
    FROM {vect_tbl}
    WHERE {vect_tbl}.comm_id LIKE 'DK%'
)""".format(
        vect_tbl=vect_tbl,
        rast_tbl=rast_tbl,
        rast_srid=srid
    )

    lau_rast_tbl = STAT_SCHEMA + '.' + datasetname + '_lau'
    
    update_query = """{subquery}
UPDATE {lau_rast_tbl} SET 
(count, mean, stddev, min, max) =
(agg.count, agg.mean, agg.stddev, agg.min, agg.max)
FROM agg
WHERE {lau_rast_tbl}.fk_lau_gid = agg.gid
;
""".format(lau_rast_tbl=lau_rast_tbl, subquery=precompute_query)
    
    db.query(commit=True, notices=True, query=update_query)                


    # Compute NUTS 3 from LAU
    print("Precompute NUTS 3")

    compute_query = """
with agg_lau as (select s.count, s.sum, s.mean, s.stddev, s.min, s.max, s.comm_id, l.fk_nuts_gid, l.nuts_id
from {prec_lau_tbl} s
left join {lau_table} l on l.comm_id = s.comm_id
WHERE s.comm_id LIKE 'DK%'
),
agg_nuts3 as (
    SELECT min(agg_lau.min), max(agg_lau.max), sum(agg_lau.sum), sum(agg_lau.count) as count, sum(agg_lau.sum) / cast(sum(agg_lau.count) as numeric(20,2)) as mean, agg_lau.nuts_id
    FROM agg_lau 
    GROUP BY agg_lau.nuts_id
)
UPDATE {prec_tbl} SET (min, max, sum, count, mean) =
(agg_nuts3.min, agg_nuts3.max, agg_nuts3.sum, agg_nuts3.count, agg_nuts3.mean)
FROM agg_nuts3
WHERE agg_nuts3.nuts_id = {prec_tbl}.nuts_id
;
""".format(
    prec_lau_tbl=lau_rast_tbl,
    lau_table=LAU_TABLE,
    prec_tbl=STAT_SCHEMA + '.' + datasetname + '_nuts',
    vect_tbl=GEO_SCHEMA + '.' + 'nuts',
    )

    db.query(commit=True, notices=False, query=compute_query)


    # compute NUTS 0-2 from NUTS 3
    print("Precompute NUTS 0-2")
    query = """
WITH agg_nuts3 as (SELECT s.count, s.sum, s.mean, s.min, s.max, s.nuts_id, s.stat_levl_, s.fk_nuts_gid
FROM {prec_tbl} s
WHERE s.stat_levl_ = 3
AND s.nuts_id LIKE 'DK%'
),
agg_nuts as (
    SELECT min(agg_nuts3.min), max(agg_nuts3.max), sum(agg_nuts3.sum), sum(agg_nuts3.count) as count, sum(agg_nuts3.sum) / cast(sum(agg_nuts3.count) as numeric(20,2)) as mean, agg_nuts3.fk_nuts_gid, n.nuts_id
    FROM geo.nuts n 
    RIGHT JOIN agg_nuts3 on agg_nuts3.nuts_id LIKE n.nuts_id || '%' /* || = concat (thank you IBM) */
    WHERE agg_nuts3.count IS NOT NULL
    AND n.stat_levl_ < 3 AND n.year = '2013-01-01'
    GROUP BY n.gid, n.stat_levl_, n.nuts_id, agg_nuts3.fk_nuts_gid
)

UPDATE {prec_tbl} SET (min, max, sum, count, mean) =
(agg_nuts.min, agg_nuts.max, agg_nuts.sum, agg_nuts.count, agg_nuts.mean)
FROM agg_nuts
WHERE agg_nuts.nuts_id = {prec_tbl}.nuts_id
AND agg_nuts.fk_nuts_gid = {prec_tbl}.fk_nuts_gid
;
""".format(
    prec_lau_tbl=lau_rast_tbl,
    lau_table=LAU_TABLE,
    prec_tbl=STAT_SCHEMA + '.' + datasetname + '_nuts',
    vect_tbl=GEO_SCHEMA + '.' + 'nuts',
)
    
    db.query(commit=True, notices=False, query=query)

    # connect to database
    db = db_helper.DB(host=DB_host, port=str(DB_port), database=DB_database, user=DB_user, password=DB_password)

    print('Success! Update of', datasetname, 'complete.')


# MAIN

# update shapefile
update_shapefile("dk-lau-update/lau-update-dk.shp")

dataset = []

# update precompute tables for each dataset in CSV file
with open('dk-lau-update/list_of_datasets.txt', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        print('Update dataset: ', row['dataset_name'], row['srid'])
        compute(row['dataset_name'], row['srid'])

print('Update script finished.')
print('Youpi')
