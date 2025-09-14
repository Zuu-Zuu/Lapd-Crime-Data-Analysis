from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count
import logging

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    spark = SparkSession.builder \
        .appName("LAPD - Crime Trend Analysis v2") \
        .getOrCreate()

    input_path = "hdfs://localhost:9000/lapd_project/processed/cleaned_parquet_v2"  # cleaned data
    output_path = "hdfs://localhost:9000/lapd_project/analysis/trend_v2"  # trend output

    df = spark.read.parquet(input_path)  # read dataset from HDFS

    # extract year/month, count crimes per month, sort chronologically
    result_df = df.withColumn("year", year("datetime_occurrence")) \
                  .withColumn("month", month("datetime_occurrence")) \
                  .groupBy("year", "month") \
                  .agg(count("*").alias("crime_count")) \
                  .orderBy("year", "month")

    result_df.write.mode("overwrite").parquet(output_path)  # save results to HDFS

    logging.info("Crime trend analysis completed successfully.")

    spark.stop()

if __name__ == "__main__":
    main()


