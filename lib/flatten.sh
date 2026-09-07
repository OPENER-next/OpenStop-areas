# Flattens the output folder structure (hive) created by DuckDB partitioning.
# Moves all files out into the output folder and renames them to their geohash_cell.

for f in output/h3_id=*/data_0.csv; do
    id=$(echo "$f" | grep -oP "h3_id=\K[0-9]+")
    mv "$f" "output/${id}.csv"
done
