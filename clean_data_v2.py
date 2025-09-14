from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, lpad, concat_ws, regexp_replace,
    trim, upper
)

def main():
    # Start Spark session
    spark = SparkSession.builder \
        .appName("LAPD Crime Data Cleaning v2") \
        .getOrCreate()

    # HDFS paths
    INPUT_PATH = "hdfs://localhost:9000/lapd_project/raw/lapd_crime_2020_2024.json"
    OUTPUT_PATH = "hdfs://localhost:9000/lapd_project/processed/cleaned_parquet_v2"

    df = spark.read.option("multiline", "true").json(INPUT_PATH)

    # Select relevant columns
    df_clean = df.select(
        col("dr_no"),
        col("date_occ"),
        col("time_occ"),
        col("area_name"),
        col("crm_cd_desc"),
        col("vict_age"),
        col("vict_sex"),
        col("vict_descent"),
        col("weapon_desc"),
        col("status_desc"),
        col("premis_desc"),
        col("lat"),
        col("lon")
    ).filter(
        col("dr_no").isNotNull() &
        col("date_occ").isNotNull() &
        col("time_occ").isNotNull()
    )

    # Clean and combine date + time fields
    df_clean = df_clean \
        .withColumn("time_occ_str", lpad(col("time_occ").cast("string"), 4, "0")) \
        .withColumn("date_str", regexp_replace(col("date_occ"), "T.*", "")) \
        .withColumn("datetime_str", concat_ws(" ", col("date_str"), col("time_occ_str"))) \
        .withColumn("datetime_occurrence", to_timestamp("datetime_str", "yyyy-MM-dd HHmm"))

    # Drop helper columns
    df_clean = df_clean.drop("date_str", "time_occ_str", "datetime_str")

    # Normalize text fields
    df_clean = df_clean \
        .withColumn("area_name", trim(upper(col("area_name")))) \
        .withColumn("crm_cd_desc", trim(upper(col("crm_cd_desc")))) \
        .withColumn("vict_sex", trim(upper(col("vict_sex")))) \
        .withColumn("vict_descent", trim(upper(col("vict_descent")))) \
        .withColumn("weapon_desc", trim(upper(col("weapon_desc")))) \
        .withColumn("status_desc", trim(upper(col("status_desc")))) \
        .withColumn("premis_desc", trim(upper(col("premis_desc"))))

    # Add year/month fields
    df_clean = df_clean \
        .withColumn("year", year(col("datetime_occurrence"))) \
        .withColumn("month", month(col("datetime_occurrence")))

    # Replace known nulls with defaults
    df_clean = df_clean.fillna({
        "vict_sex": "UNKNOWN",
        "vict_descent": "UNKNOWN",
        "weapon_desc": "NO WEAPON",
        "premis_desc": "UNKNOWN"
    })

    df_clean.write.mode("overwrite").partitionBy("year").parquet(OUTPUT_PATH)

    print(f"Cleaned_parquet_v2 saved to {OUTPUT_PATH}")
    spark.stop()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error during cleaning: {e}")

