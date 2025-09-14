#!/bin/bash

# Timestamp
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
echo "[$TIMESTAMP] Starting LAPD Data Pipeline..."

# 1. Fetch LAPD data from API
echo "Fetching data from API..."
python3 ~/Desktop/lapd_project/scripts/fetch_data.py

# 2. Upload raw JSON to HDFS
echo "Uploading raw JSON to HDFS..."
hdfs dfs -put -f ~/Desktop/lapd_project/data/raw/lapd_crime_2020_2024.json /lapd_project/raw/

# 3. Clean data using PySpark
echo "Cleaning data with Spark..."
spark-submit ~/Desktop/lapd_project/scripts/clean_data_v2.py

# 4. Run analysis scripts
echo "Running analysis scripts..."

spark-submit ~/Desktop/lapd_project/scripts/analyze_crime_trend_v2.py
spark-submit ~/Desktop/lapd_project/scripts/analyze_crime_by_type_v2.py
spark-submit ~/Desktop/lapd_project/scripts/analyze_crime_by_area_v2.py
spark-submit ~/Desktop/lapd_project/scripts/analyze_crime_by_victim_v2.py
spark-submit ~/Desktop/lapd_project/scripts/analyze_crime_by_weapon_v2.py

# 5. Export to PostgreSQL
echo "Exporting analysis to PostgreSQL..."
spark-submit ~/Desktop/lapd_project/scripts/push_all_analysis_to_postgre.py

echo "[$(date "+%Y-%m-%d %H:%M:%S")] LAPD Data Pipeline completed successfully!"
