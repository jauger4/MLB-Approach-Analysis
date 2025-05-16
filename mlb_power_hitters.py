import argparse

from pyspark.sql import SparkSession

def mlb_power_hitters(data_source, output_uri):
    """
    Calculating the top power hitters from the 2025 MLB Season (as of 5/14/2025).

    :param data_source: The URI where the food establishment data CSV is saved, typically
			  an Amazon S3 bucket, such as 's3://DOC-EXAMPLE-BUCKET/mlb_stats.csv'.
    :param output_uri: The URI where the output is written, typically an Amazon S3
                       bucket, such as 's3://DOC-EXAMPLE-BUCKET/power_hitter_results'.
    """
    with SparkSession.builder.appName("Power Hitters").getOrCreate() as spark:
        # Load the MLB Hitter CSV data
        if data_source is not None:
            hitters_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        hitters_df.createOrReplaceTempView("hitters_stats")

        # Create a DataFrame of the top 10 power Hitters
        top_power_hitters = spark.sql("SELECT player_name, ROUND((k_percent + bb_percent + hard_hit_percent + whiff_percent - oz_swing_percent), 2) as power " +
          "FROM hitters_stats " +
          "ORDER BY power DESC LIMIT 10 ")

        # Write the results to the specified output URI
        top_power_hitters.write.option("header", "true").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI where the CSV hitter data is saved, typically an S3 bucket.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, typically an S3 bucket.")
    args = parser.parse_args()

    mlb_power_hitters(args.data_source, args.output_uri)
		