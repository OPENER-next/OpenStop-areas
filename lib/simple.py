"""
Streams a remote GeoParquet file (preprocessed OpenStreetMap planet data) over HTTP,
filters for public transport elements, merges nearby elements and saves buffered areas locally.
Writes the result to a FlatGeobuf file.
"""

import duckdb

URL = "https://download.openplanetdata.com/osm/planet/geoparquet/v1/planet-latest.osm.parquet"
OUTPUT_FILE = "output.fgb"
DISTANCE_THRESHOLD = 100.0
BUFFER_DISTANCE = 50.0

con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
con.sql("INSTALL spatial; LOAD spatial;")
# Can be used to manually limit memory usage, defaults to 80% of available RAM
con.sql("SET memory_limit = '4GB';")
con.sql("SET enable_progress_bar = true;")

con.sql(f"""
    COPY (
        SELECT
            geometry,
            tags['name'] AS name
        FROM read_parquet('{URL}')
        WHERE
            tags['highway'] IN ('platform', 'bus_stop')
            OR tags['railway'] IN ('platform', 'tram_stop', 'halt', 'station')
            OR tags['public_transport'] = 'platform'
            OR tags['amenity'] = 'bus_station'
    )
    TO '{OUTPUT_FILE}'
    WITH (
        FORMAT GDAL,
        DRIVER 'FlatGeobuf',
        LAYER_CREATION_OPTIONS 'SPATIAL_INDEX=NO'
    );
""")
