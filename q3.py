spark.sql("""
SELECT
  raw_location_text AS location,
  COUNT(*) AS lines_count
FROM lines
WHERE raw_location_text IS NOT NULL
GROUP BY raw_location_text
ORDER BY lines_count DESC
LIMIT 20
""").show(20, truncate=False)
