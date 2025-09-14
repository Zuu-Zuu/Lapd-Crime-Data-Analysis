from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder \
    .appName("Export All LAPD Analyses to PostgreSQL") \
    .config("spark.jars", "/home/zuuzuu/lapd-crime-analysis/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

# PostgreSQL connection
jdbc_url = "jdbc:postgresql://localhost:5432/lapd_db"
properties = {
    "user": "crimeuser",
    "password": "crimepass",
    "driver": "org.postgresql.Driver"
}

# Define export function
def export_analysis(parquet_path, table_name):
    df = spark.read.parquet(parquet_path)
    df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)

# Analysis exports
export_analysis("hdfs://localhost:9000/lapd_project/analysis/trend_v2", "lapd_trend_analysis_v2")
export_analysis("hdfs://localhost:9000/lapd_project/analysis/crime_type_v2", "lapd_crime_type_analysis_v2")
export_analysis("hdfs://localhost:9000/lapd_project/analysis/crime_by_area_v2", "lapd_crime_area_analysis_v2")
export_analysis("hdfs://localhost:9000/lapd_project/analysis/crime_by_victim_v2", "lapd_victim_profile_analysis_v2")
export_analysis("hdfs://localhost:9000/lapd_project/analysis/crime_by_weapon_v2", "lapd_weapon_type_analysis_v2")

print("All analyses exported successfully.")
spark.stop()

