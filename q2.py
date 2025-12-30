
# Load Locations (needed for Q2 JOIN)
locations_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("simpsons/simpsons_locations.csv")

locations_df.createOrReplaceTempView("locations")

spark.sql("""
SELECT
  l.raw_text,
  loc.name AS location_name
FROM lines l
JOIN locations loc
  ON CAST(l.location_id AS INT) = loc.id
WHERE loc.name IS NOT NULL
  AND LOWER(loc.name) LIKE '%jerusalem%'
  AND l.raw_text IS NOT NULL
LIMIT 20
""").show(20, truncate=False)
