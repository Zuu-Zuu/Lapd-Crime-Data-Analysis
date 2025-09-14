from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import logging

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    spark = SparkSession.builder \
        .appName("LAPD - Crime by Area Analysis v2") \
        .getOrCreate()

    input_path = "hdfs://localhost:9000/lapd_project/processed/cleaned_parquet_v2"  # cleaned data
    output_path = "hdfs://localhost:9000/lapd_project/analysis/crime_by_area_v2"  # area analysis output

    df = spark.read.parquet(input_path)  # read dataset from HDFS

    # group crimes by area, count occurrences, sort highest first
    result_df = df.groupBy("area_name") \
                  .agg(count("*").alias("crime_count")) \
                  .orderBy("crime_count", ascending=False)

    result_df.write.mode("overwrite").parquet(output_path)  # save results to HDFS

    logging.info("Crime by Area analysis completed.")

    spark.stop()

if __name__ == "__main__":
    main()

