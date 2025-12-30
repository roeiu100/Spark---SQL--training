spark.sql("""
SELECT
  raw_character_text AS character,
  COUNT(*) AS lines_spoken
FROM lines
WHERE raw_character_text IS NOT NULL
GROUP BY raw_character_text
ORDER BY lines_spoken DESC
LIMIT 10
""").show(10, truncate=False)
