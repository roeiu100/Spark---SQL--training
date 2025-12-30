spark.sql("""
SELECT DISTINCT raw_location_text
FROM lines
WHERE raw_location_text IS NOT NULL
  AND LOWER(raw_location_text) LIKE '%springfield%'
LIMIT 50
""").show(50, truncate=False)
