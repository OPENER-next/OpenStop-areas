# OpenStop Areas of Interest

This repository contains a script to generate public transport areas of interest from OpenStreetMap data.

## Workflow
1. Use GeoParquet planet file as a source, because it contains pre-built geometries.
2. Stream data over HTTP and filter for public transport elements.
3. Buffer and merge overlapping geometries.
4. Export as GeoPackage as an intermediate format because it can be streamed to disk.
5. Convert to FlatGeobuf for fast random spatial access.

## Notes
Directly working with the osm.pbf planet file like so:
```bash
curl -sL https://...planet.osm.pbf | \
osmium tags-filter -F pbf -f pbf -o - \
    w/highway=platform,bus_stop \
    w/railway=platform,tram_stop,halt,station \
    ...
osmium export -F pbf -f geojsonseq - | \
python3 script.py
```
is not possible because pbf files are ordered by nodes, ways relations.
In order to build the geometry we have to read a pbf file twice (i.a. keep the file in memory or on disk)
which is what we want to avoid with streaming.
A GeoParquet on the other hand already has the geometry prebuilt wherefore we can stream and filter it directly.