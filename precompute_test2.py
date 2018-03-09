# python 3.5
import sys
import os.path
import traceback
import logging
from ci_secrets.secrets import DB_password, DB_database, DB_host, DB_port, DB_user
from db import db_helper

# schemas
stat_schema = 'stat'  # 'stat' on production/dev database
geo_schema = 'geo'  # 'geo' on production/dev database

# current path
base_path = os.path.dirname(os.path.abspath(__file__))

lau_table_name = 'lau'
nuts_table_name = 'nuts'
raster_table_name = 'heat_tot_curr_density'
precomputed_table_name_lau = raster_table_name + "_" + lau_table_name + "_test1"
precomputed_table_name_nuts = raster_table_name + "_" + nuts_table_name + "_test1"
vector_SRID = "3035"
raster_SRID = "3035"

# connect to database
db = db_helper.DB(host=DB_host, port=DB_port, database=DB_database, user=DB_user, password=DB_password)
verbose = True

# read datapackage.json (dp)
try:
    # LAU
    #rast_tbl = geo_schema + "." + raster_table_name
    rast_tbl = geo_schema + '.' + raster_table_name
    vect_tbl = "public." + lau_table_name
    vect_tbl_name = lau_table_name
    prec_tbl = stat_schema + '.' + precomputed_table_name_lau
    prec_tbl_name = precomputed_table_name_lau

    # db.drop_table(table_name=prec_tbl, notices=verbose)
    #
    # attributes_names = ('count', 'sum', 'mean', 'stddev', 'min', 'max', 'comm_id', 'fk_' + vect_tbl_name + '_gid')
    # constraints = "ALTER TABLE " + prec_tbl  + " " \
    #             + "ADD CONSTRAINT " + prec_tbl_name + "_" + vect_tbl_name + "_gid_fkey " \
    #             + "FOREIGN KEY (fk_" + vect_tbl_name + "_gid) " \
    #             + "REFERENCES " + vect_tbl + "(gid) " \
    #             + "MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION "
    #
    # db.create_table(table_name=prec_tbl,
    #                 col_names=attributes_names,
    #                 col_types=('bigint', 'numeric(20,2)', 'numeric(20,2)', 'numeric(20,2)', 'numeric(20,2)',
    #                            'numeric(20,2)', 'varchar(255)', 'bigint'),
    #                 constraints_str=constraints,
    #                 notices=verbose)
    #
    # query = "SELECT (" \
    #             + "SELECT (ST_SummaryStatsAgg(ST_Clip(" + rast_tbl + ".rast, 1, ST_Transform(" + \
    #                 vect_tbl + ".geom, " + raster_SRID + "), true), 1, true)) " \
    #             + "FROM " + rast_tbl + " " \
    #             + "WHERE ST_Intersects(" \
    #                 + rast_tbl + ".rast, ST_Transform(" + vect_tbl + ".geom, 3035) " \
    #             + ") " \
    #         + ").*, " + vect_tbl + ".comm_id, " + vect_tbl + ".gid " \
    #         + "FROM " + vect_tbl + " "
    #
    # db.query(commit=True, notices=verbose, query='INSERT INTO ' + prec_tbl
    #     + ' (' + ', '.join(map(db_helper.str_with_quotes, [x.lower() for x in attributes_names])) + ') '
    #     + query + ' ;')
    #
    # NUTS
    vect_tbl = geo_schema + '.' + nuts_table_name
    prec_tbl = stat_schema + '.' + precomputed_table_name_nuts
    vect_tbl_name = nuts_table_name
    prec_tbl_name = precomputed_table_name_nuts

    db.drop_table(table_name=prec_tbl, notices=verbose)

    attributes_names = ('count', 'sum', 'mean', 'stddev', 'min', 'max', 'nuts_id', 'fk_'+vect_tbl_name+'_gid')
    constraints = "ALTER TABLE " + prec_tbl  + " " \
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
            + "FROM " + vect_tbl + " WHERE year='2013-01-01'"

    db.query(commit=True, notices=verbose, query='INSERT INTO ' + prec_tbl
        + ' (' + ', '.join(map(db_helper.str_with_quotes, [x.lower() for x in attributes_names])) + ') '
        + query + ' ;')

except Exception as e:
    logging.error(traceback.format_exc())
    sys.exit(1)

db.close_connection()
