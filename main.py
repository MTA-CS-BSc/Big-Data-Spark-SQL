import sys

from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Bucket name is missing!!", file=sys.stderr)
        sys.exit(-1)

spark = SparkSession.builder.appName("My Python Spark SQL App").getOrCreate()
bucket = sys.argv[1]

simpsons_locations_lines = spark.read.format("com.databricks.spark.csv").options(header='true') \
        .load(f"s3://mayara2-105555623101/simpsons/simpsons_locations.csv")

simpsons_script_lines = spark.read.format("com.databricks.spark.csv").options(header='true') \
        .load(f"s3://mayara2-105555623101/simpsons/simpsons_script_lines.csv")

simpsons_episodes_lines = spark.read.format("com.databricks.spark.csv").options(header='true') \
        .load(f"s3://mayara2-105555623101/simpsons/simpsons_episodes.csv")

simpsons_locations_lines.createOrReplaceTempView("simpsons_locations")
simpsons_script_lines.createOrReplaceTempView("simpsons_script_lines")
simpsons_episodes_lines.createOrReplaceTempView("simpsons_episodes")

## print lines file schema
simpsons_locations_lines.printSchema()
simpsons_script_lines.printSchema()
simpsons_episodes_lines.printSchema()

## print the first 50 locations that has the word "Springfield" in them, ignoring letters case.
spark.sql("SELECT DISTINCT name " \
        "FROM simpsons_locations " \
        "WHERE normalized_name LIKE '%springfield%'") \
        .show(50, False)

## print 20 quotes that are located in any place that has Jerusalem in its name. 
## Note that Jerusalem may appear in any case and in any part of the location name.
## Use JOIN for this query on another table
spark.sql("SELECT spoken_words AS quote, loc.name as location " \
        "FROM simpsons_script_lines AS quotes " \
        "INNER JOIN simpsons_locations AS loc " \
        "ON loc.id = quotes.location_id " \
        "WHERE loc.normalized_name LIKE '%jerusalem%' " \
        "AND spoken_words IS NOT NULL") \
        .show(20, False)

## print first 20 most used locations with the count of lines spoken in them.
## use GROUP BY for that
spark.sql("SELECT raw_location_text, COUNT(*) AS lines_spoken " \
          "FROM simpsons_script_lines " \
          "GROUP BY raw_location_text " \
          "ORDER by lines_spoken DESC") \
        .show(20, False)

## find the seasons in which the average imdb rating was the highest. 
## Print the seasons number, the number of episodes in each one and the average rating 
## in a descending order from highest average rating to lowest.
spark.sql("SELECT season, COUNT(*) AS episodes, AVG(imdb_rating) AS season_imdb_rating " \
        "FROM simpsons_episodes " \
        "GROUP BY season " \
        "ORDER BY season_imdb_rating DESC") \
        .show()


## find character names that said lines that are greater than 30 characters in any place that has Jerusalem in its name
## Print character name, location name, the line and it's length in a descending order from the longest line
spark.sql("SELECT DISTINCT chars.name, loc.name, spoken_words, LENGTH(spoken_words) " \
        "FROM simpsons_script_lines " \
        "INNER JOIN simpsons_characters AS chars ON character_id = chars.id " \
        "INNER JOIN simpsons_locations AS loc ON location_id = loc.id " \
        "WHERE LENGTH(spoken_words) > 30 AND loc.normalized_name LIKE '%jerusalem%' " \
        "ORDER BY LENGTH(spoken_words) DESC") \
        .show()

