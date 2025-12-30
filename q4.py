# Load Episodes (needed for Q4)
episodes_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("simpsons/simpsons_episodes.csv")

episodes_df.createOrReplaceTempView("episodes")

spark.sql("""
SELECT
  season AS season_num,
  COUNT(*) AS episode_count,
  ROUND(AVG(imdb_rating), 2) AS avg_imdb_rating
FROM episodes
WHERE season IS NOT NULL
  AND imdb_rating IS NOT NULL
GROUP BY season
ORDER BY avg_imdb_rating DESC
""").show(truncate=False)
