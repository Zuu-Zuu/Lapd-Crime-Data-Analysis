from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
import logging

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    spark = SparkSession.builder.appName("LAPD - Crime by Victim Gender & Descent v2").getOrCreate()

    input_path = "hdfs://localhost:9000/lapd_project/processed/cleaned_parquet_v2"  # cleaned data
    output_path = "hdfs://localhost:9000/lapd_project/analysis/crime_by_victim_v2"  # victim profile output

    df = spark.read.parquet(input_path)  # read dataset from HDFS

    df = df.withColumn("vict_age", col("vict_age").cast("int"))  # ensure age is numeric

    # bucket victims into age groups
    df = df.withColumn(
        "victim_age_group",
        when(col("vict_age") < 13, "Child (0-12)")
        .when(col("vict_age") < 18, "Teen (13-17)")
        .when(col("vict_age") < 25, "Young Adult (18-24)")
        .when(col("vict_age") < 45, "Adult (25-44)")
        .when(col("vict_age") < 65, "Middle Aged (45-64)")
        .when(col("vict_age") >= 65, "Senior (65+)")
        .otherwise("UNKNOWN")
    )

    # group by gender, descent, and age group
    result_df = df.groupBy("vict_sex", "vict_descent", "victim_age_group") \
                  .agg(count("*").alias("crime_count")) \
                  .orderBy("crime_count", ascending=False)

    result_df.write.mode("overwrite").parquet(output_path)  # save results to HDFS

    logging.info("Crime by Victim analysis completed.")
    spark.stop()

if __name__ == "__main__":
    main()


