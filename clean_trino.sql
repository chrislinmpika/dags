-- 1. Force drop the staging table
DROP TABLE IF EXISTS iceberg.silver.staging_biological_results;

-- 2. Check if the file tracking table is locked (optional but good)
SELECT * FROM iceberg.silver._file_tracking LIMIT 1;