"""
Streams a remote GeoParquet file (Preprocessed OpenStreetMap planet data) over HTTP,
filters for public transport elements, merges nearby elements and saves buffered areas locally.
Can write the result to GeoPackage or FlatGeobuf.
"""

import geopandas as gp
import pyarrow as pa
import pyarrow.fs as pf
import pyarrow.compute as pc
import pyarrow.dataset as ds
import geoarrow.pyarrow as ga
import fsspec
import fiona
import fiona.crs as fcrs

# GeoParquet globals
url = "https://download.openplanetdata.com/osm/planet/geoparquet/v1/planet-latest.osm.parquet"

required_columns = ["geometry", "tags"]

tags_field = pc.field("tags")
highway_vals = pc.map_lookup(tags_field, "highway", "first")
railway_vals = pc.map_lookup(tags_field, "railway", "first")
pt_vals = pc.map_lookup(tags_field, "public_transport", "first")
amenity_vals = pc.map_lookup(tags_field, "amenity", "first")

f_highway = pc.is_in(highway_vals, value_set=pa.array(["platform", "bus_stop"]))
f_railway = pc.is_in(railway_vals, value_set=pa.array(["platform", "tram_stop", "halt", "station"]))
f_pt = (pt_vals == "platform")
f_amenity = (amenity_vals == "bus_station")

tags_filter = f_highway | f_railway | f_pt | f_amenity

# Output file structure
schema = {
    'geometry': 'Polygon',
    'properties': {'name': 'str'}
}

OUTPUT_FILE = "output.gpkg"
BUFFER_RADIUS = 50.0
GEOMETRY_RESOLUTION = 2

# Open output file context and stream features
# Using FlatGeobuf as driver would work but the file cannot be written to disk until finished processing due to the RTree
with fiona.open(OUTPUT_FILE, "w", driver="GPKG", crs=fcrs.from_epsg(4326), schema=schema) as fgb:
    # Make PyArrow compatible file system
    pa_fs = pf.PyFileSystem(pf.FSSpecHandler(fsspec.filesystem("http")))
    # Open the file as a PyArrow Dataset
    # This reads only the metadata footer from the HTTP server
    dataset = ds.dataset(url, filesystem=pa_fs, format="parquet")
    # Read filtered GeoParquet batches
    for batch in dataset.to_batches(filter=tags_filter, columns=required_columns):
        # Extract name column from tags and geometry
        name_vals = pc.map_lookup(batch["tags"], "name", "first")
        geom_vals = ga.as_geoarrow(batch["geometry"], type=ga.wkb().with_crs("EPSG:4326"))
        # PyArrow Table
        pa_table = pa.Table.from_arrays(
            [name_vals, geom_vals],
            names=["name", "geometry"]
        )
        # Create GeoDataFrame
        gdf = gp.GeoDataFrame.from_arrow(pa_table)
        gdf.set_crs("EPSG:4326", inplace=True)
        # Project to metric system (Web Mercator / EPSG:3857)
        gdf.to_crs(epsg=3857, inplace=True)
        # Buffer geometry with resolution
        gdf['geometry'] = gdf.geometry.buffer(BUFFER_RADIUS, GEOMETRY_RESOLUTION)

        # START merging intersecting geometries
        def most_common(name_series):
            most_names = name_series.mode()
            return None if most_names.empty else most_names.iloc[0]
        # Cluster overlapping geometries
        merged_geom = gdf.geometry.union_all()
        clusters = (
            gp.GeoDataFrame(geometry=[merged_geom], crs=gdf.crs)
            .explode(index_parts=False)
            .reset_index(drop=True)
        )
        clusters['cluster_id'] = clusters.index
        clusters['geometry'] = clusters.geometry.convex_hull
        # Spatial join clusters with un-merged geometries so we can group by later
        joined = gp.sjoin(gdf, clusters, how='left', predicate='intersects')
        # Group by clusters and keep most occurring name
        aggregated_names = (
            joined.groupby('cluster_id')['name']
            .agg(most_common)
            .reset_index()
        )
        # Merge back the names to the clusters
        gdf = clusters.merge(aggregated_names, on='cluster_id', how='left')
        gdf.drop(columns=['cluster_id'], inplace=True)
        # END merging intersecting geometries

        # Un-project to original WGS84-System
        gdf.to_crs(epsg=4326, inplace=True)

        geojson_features = gdf.iterfeatures()
        fgb.writerecords(geojson_features)
        # Has no effect when using FlatGeobuf as fiona (more precisely GDAL)
        # keeps the file in memory because it must compute the R-Tree spatial index
        fgb.flush()
