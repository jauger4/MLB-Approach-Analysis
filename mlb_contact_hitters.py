import argparse

from pyspark.sql import SparkSession

def mlb_contact_hitters(data_source, output_uri):
    """
    Calculating the top contact hitters from the 2025 MLB Season (as of 5/14/2025).

    :param data_source: The URI where the food establishment data CSV is saved, typically
			  an Amazon S3 bucket, such as 's3://DOC-EXAMPLE-BUCKET/mlb_stats.csv'.
    :param output_uri: The URI where the output is written, typically an Amazon S3
                       bucket, such as 's3://DOC-EXAMPLE-BUCKET/contact_hitter_results'.
    """
    with SparkSession.builder.appName("Contact Hitters").getOrCreate() as spark:
        # Load the MLB Hitter CSV data
        if data_source is not None:
            hitters_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        hitters_df.createOrReplaceTempView("hitters_stats")

        # Create a DataFrame of the top 10 Contact Hitters
        top_contact_hitters = spark.sql("SELECT player_name, ROUND(((iz_contact_percent - oz_contact_percent) + (z_swing_percent - oz_swing_percent) + k_percent + whiff_percent + bb_percent),2) AS contact " +
          "FROM hitters_stats " +
          "ORDER BY contact ASC LIMIT 10 ")

        # Write the results to the specified output URI
        top_contact_hitters.write.option("header", "true").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI where the CSV hitter data is saved, typically an S3 bucket.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, typically an S3 bucket.")
    args = parser.parse_args()

    mlb_contact_hitters(args.data_source, args.output_uri)
		