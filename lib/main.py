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

# 1. Read, filter, and project data to a metric coordinate system
con.sql(f"""
    CREATE OR REPLACE VIEW filtered_elements AS
    SELECT
        ROW_NUMBER() OVER () AS id, -- Generate sequential IDs for cluster identification
        ST_Transform(geometry, 'EPSG:3857') AS geometry,
        tags['name'] AS name
    FROM read_parquet('{URL}')
    WHERE
        tags['highway'] IN ('platform', 'bus_stop')
        OR tags['railway'] IN ('platform', 'tram_stop', 'halt', 'station')
        OR tags['public_transport'] = 'platform'
        OR tags['amenity'] = 'bus_station'
""")

# 2. Run graph-based proximity clustering using ST_DWithin and stream out to FlatGeobuf
# If ST_ClusterWithin is ever supported by DuckDB, this could be simplified
con.sql(f"""
    COPY (
        WITH RECURSIVE spatial_edges AS (
            SELECT DISTINCT
                a.id AS source_id,
                b.id AS target_id
            FROM filtered_elements a
            INNER JOIN filtered_elements b
                ON ST_DWithin(a.geometry, b.geometry, {DISTANCE_THRESHOLD})
        ),

        -- Recursively traverse chains to find the absolute minimum ID for each network cluster
        graph_traversal AS (
            -- Anchor member: Start by pointing every node to its direct neighbor
            SELECT
                source_id,
                target_id AS cluster_anchor
            FROM spatial_edges

            -- Union will be applied as long as the recursive select output changes
            UNION

            -- Recursive member: Propagate the lowest ID through the network chains
            SELECT
                gt.source_id,
                se.target_id AS cluster_anchor
            FROM graph_traversal gt
            JOIN spatial_edges se
                ON gt.cluster_anchor = se.source_id
            WHERE se.target_id < gt.cluster_anchor -- Only keep traversing if we find a lower ID (prevents infinite loops)
        ),

        -- Group by each node and find its absolute final structural root component
        final_clusters AS (
            SELECT
                source_id AS original_id,
                MIN(cluster_anchor) AS global_cluster_id
            FROM graph_traversal
            GROUP BY source_id
        ),

        spatial_clusters AS (
            SELECT
                MODE(g.name) AS name,
                ST_Buffer(ST_ConvexHull(ST_Collect(list(g.geometry))), {BUFFER_DISTANCE}, 2) AS geometry
            FROM final_clusters c
            JOIN filtered_elements g
            ON c.original_id = g.id
            GROUP BY c.global_cluster_id
        )

        SELECT
            ST_Transform(geometry, 'EPSG:3857', 'EPSG:4326') AS geometry,
            name
        FROM spatial_clusters
    )
    TO '{OUTPUT_FILE}'
    WITH (
        FORMAT GDAL,
        DRIVER 'FlatGeobuf',
        LAYER_CREATION_OPTIONS 'SPATIAL_INDEX=NO'
    );
""")
