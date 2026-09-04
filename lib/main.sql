/**
 * Streams a remote GeoParquet file (preprocessed OpenStreetMap planet data) over HTTP,
 * filters for public transport elements, merges nearby elements (and elements that share
 * the same name) and saves buffered areas locally.
 * Writes the result to a FlatGeobuf file.
 *
 * As DuckDB methods mostly require planar geometries each element is projected
 * into its own local best-fit UTM zone up front. Distance comparisons for clustering
 * are done in that projected (planar/metric) system wherefore only elements
 * that share the same UTM zone can be clustered.
 * Therefore elements won't be merged across zone boundaries.
 * Finally the resulting geometry is projected back to WGS84.
 **/

INSTALL httpfs;
LOAD httpfs;
INSTALL spatial;
LOAD spatial;

-- Can be used to manually limit memory usage, defaults to 80% of available RAM
SET memory_limit = '4GB';
-- Displays a progress bar in the terminal
SET enable_progress_bar = true;
SET enable_progress_bar_print = true;
SET geometry_always_xy = true;
-- Custom variables
SET VARIABLE INPUT_FILE = "https://download.openplanetdata.com/osm/planet/geoparquet/v1/planet-latest.osm.parquet";
SET VARIABLE OUTPUT_FILE = "output.fgb";
-- Elements are merged if they are within this distance
SET VARIABLE RAW_DISTANCE_THRESHOLD = 100.0;
-- Elements that share the same (non-null) name are merged if they are within this distance
SET VARIABLE NAME_DISTANCE_THRESHOLD = 300.0;
SET VARIABLE BUFFER_DISTANCE = 50.0;
SET VARIABLE IGNORE_AREA_LIMIT = 200000;

-- Determines the best-fit UTM zone for a given geometry by using its centroid
CREATE OR REPLACE FUNCTION get_utm_epsg_from_geom(geom) AS (
    SELECT
        'EPSG:' || CAST(
            32601 + ((ST_Y(centroid) < 0)::INT * 100) + FLOOR((ST_X(centroid) + 180) / 6)
            AS INT
        )
    FROM (SELECT ST_Centroid(geom) AS centroid)
);

-- Read and filter to public transport elements
CREATE OR REPLACE VIEW filtered_elements AS
SELECT
    -- Generate sequential IDs for cluster identification
    ROW_NUMBER() OVER () AS id,
    geometry::GEOMETRY('OGC:CRS84') AS geometry,
    tags['name'] AS name,
    get_utm_epsg_from_geom(geometry) AS utm_epsg
FROM read_parquet(getvariable('INPUT_FILE'))
WHERE
    tags['highway'] IN ('platform', 'bus_stop')
    OR tags['railway'] IN ('platform', 'tram_stop', 'halt', 'station')
    OR tags['public_transport'] = 'platform'
    OR tags['amenity'] = 'bus_station';

-- Project to metric coordinate system
CREATE OR REPLACE VIEW projected_elements AS
SELECT
    *,
    ST_Transform(geometry, 'OGC:CRS84', utm_epsg) AS proj_geometry,
FROM filtered_elements;

-- Ignores overly large elements
CREATE OR REPLACE VIEW filtered_by_size AS
SELECT *
FROM projected_elements
WHERE ST_Area(proj_geometry) < getvariable('IGNORE_AREA_LIMIT');

-- Run graph-based proximity clustering using ST_DWithin and stream out to FlatGeobuf
-- If ST_ClusterWithin is ever supported by DuckDB, this could be simplified

COPY (
    WITH RECURSIVE spatial_edges AS (
        SELECT DISTINCT
            a.id AS source_id,
            b.id AS target_id
        FROM filtered_by_size a
        INNER JOIN filtered_by_size b
            ON
            -- only ever compare elements that were projected into the same UTM zone
            a.utm_epsg = b.utm_epsg
            AND (
                -- link by max distance
                ST_DWithin(a.proj_geometry, b.proj_geometry, getvariable('RAW_DISTANCE_THRESHOLD'))
                -- link by name and separate max distance
                OR (
                    a.name IS NOT NULL
                    AND a.name = b.name
                    AND ST_DWithin(a.proj_geometry, b.proj_geometry, getvariable('NAME_DISTANCE_THRESHOLD'))
                )
            )
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

    -- Build the cluster geometry in the shared local projection
    -- every member of a cluster is guaranteed to share the same utm_epsg,
    -- since spatial_edges only ever linked elements within the same zone
    spatial_clusters AS (
        SELECT
            MODE(g.name) AS name,
            FIRST(g.utm_epsg) AS utm_epsg,
            ST_Buffer(ST_ConvexHull(ST_Collect(list(g.proj_geometry))), getvariable('BUFFER_DISTANCE'), 2) AS proj_geometry
        FROM final_clusters c
        JOIN filtered_by_size g
        ON c.original_id = g.id
        GROUP BY c.global_cluster_id
    )

    -- Transform back to WGS84
    SELECT
        ST_Transform(proj_geometry, utm_epsg, 'OGC:CRS84') AS geometry,
        name
    FROM spatial_clusters
)
TO (getvariable('OUTPUT_FILE'))
WITH (
    FORMAT GDAL,
    DRIVER 'FlatGeobuf',
    LAYER_CREATION_OPTIONS 'SPATIAL_INDEX=YES'
);
