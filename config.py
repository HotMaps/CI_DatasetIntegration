# schemas
STAT_SCHEMA = 'stat'  # 'stat' on production/dev database
GEO_SCHEMA = 'geo'  # 'geo' on production/dev database

# geo tables
LAU_TABLE_NAME = 'lau'
LAU_TABLE = 'public' + '.' + LAU_TABLE_NAME # change to geo_schema when lau table has been moved in db
NUTS_TABLE_NAME = 'nuts'
NUTS_TABLE = GEO_SCHEMA + '.' + NUTS_TABLE_NAME
VECTOR_SRID = "3035"
RASTER_SRID = "3035"

# time tables
TIME_TABLE_NAME = 'time'
TIME_TABLE = STAT_SCHEMA + '.' + TIME_TABLE_NAME