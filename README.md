LAPD Crime Data Analytics Pipeline (2020–2024)
==============================================

This project implements an end-to-end, scalable data processing pipeline for analyzing Los Angeles Police Department (LAPD) crime data from 2020 to 2024.
It uses Apache Spark, PostgreSQL, and Tableau to fetch, clean, process, analyze, and visualize crime data at scale.

----------------------------------------------
FEATURES
----------------------------------------------
- Automated ETL: Fetch → Clean → Analyze → Store → Visualize
- Scalable PySpark processing for 1M+ records
- Modular scripts for trends, types, areas, victims, and weapons
- PostgreSQL export for queryable results
- Tableau-ready datasets

----------------------------------------------
PROJECT STRUCTURE
----------------------------------------------
scripts/
    fetch_data.py                         - Download LAPD data from API
    clean_data_v2.py                      - Clean and transform raw JSON
    analyze_crime_trend_v2.py             - Monthly/yearly trend analysis
    analyze_crime_by_type_v2.py           - Crime type frequency analysis
    analyze_crime_by_area_v2.py           - Crime distribution by LAPD area
    analyze_crime_by_victim_v2.py         - Victim demographics analysis
    analyze_crime_by_weapon_v2.py         - Weapon usage analysis
    push_all_analysis_to_postgre.py       - Export all analysis results to PostgreSQL
    run_lapd_pipeline.sh                  - Automate entire pipeline

data/
    raw/                                  - Raw JSON data from API
    processed_hdfs/                       - Cleaned parquet data extracted from HDFS
    analysis_hdfs/                        - Analysis outputs extracted from HDFS

report/                                   - Documentation, write-up, and final report

----------------------------------------------
TECHNOLOGY STACK
----------------------------------------------
Python, Apache Spark (PySpark), HDFS, PostgreSQL, Tableau
Ubuntu environment

----------------------------------------------
DATASET
----------------------------------------------
Source: LA Open Data Portal – LAPD Crime Data
Period: Jan 2020 – Dec 2024 (partial)
Size: 1M+ records
Fields: timestamp, area, crime type, weapon, victim age/sex, status, etc.,

----------------------------------------------
HOW TO RUN THE PIPELINE
----------------------------------------------
1) PREREQUISITES
   - Spark, HDFS, PostgreSQL installed
   - Python deps:  pip install pyspark psycopg2 requests pandas
   - PostgreSQL JDBC driver:
       download postgresql-42.7.3.jar and place it at:
       /home/user/lapd-crime-analysis/jars/postgresql-42.7.3.jar

2) START REQUIRED SERVICES
   # HDFS
   start-dfs.sh
   # PostgreSQL (service name may vary)
   sudo systemctl start postgresql

3) DATABASE SETUP (run once)
   sudo -u postgres psql -c "CREATE DATABASE lapd_db;"
   sudo -u postgres psql -c "CREATE USER user WITH PASSWORD '*****';"
   sudo -u postgres psql -d lapd_db -c "GRANT ALL PRIVILEGES ON DATABASE lapd_db TO user;"

4) STEP-BY-STEP EXECUTION

   STEP 1: Fetch Data
       python scripts/fetch_data.py
       # (produces data/raw/lapd_crime_2020_2024.json)

   STEP 1.5: Upload raw JSON to HDFS (if not using the shell pipeline)
       hdfs dfs -mkdir -p /lapd_project/raw
       hdfs dfs -put -f data/raw/lapd_crime_2020_2024.json /lapd_project/raw/

   STEP 2: Clean & Transform Data
       spark-submit \
         --driver-memory 4G \
         --executor-memory 4G \
         ~/Desktop/lapd_project/scripts/clean_data_v2.py
       # writes to: /lapd_project/processed/cleaned_parquet_v2

   STEP 3: Run Analyses
       spark-submit scripts/analyze_crime_trend_v2.py
       spark-submit scripts/analyze_crime_by_type_v2.py
       spark-submit scripts/analyze_crime_by_area_v2.py
       spark-submit scripts/analyze_crime_by_victim_v2.py
       spark-submit scripts/analyze_crime_by_weapon_v2.py
       # outputs under: /lapd_project/analysis/*

   STEP 4: Export Results to PostgreSQL
       spark-submit \
         --jars /home/user/lapd-crime-analysis/jars/postgresql-42.7.3.jar \
         ~/Desktop/lapd_project/scripts/push_all_analysis_to_postgre.py
       # tables created in lapd_db as *_v2

   STEP 5: Visualize in Tableau
       # connect to PostgreSQL (lapd_db) and build charts

   NOTE: To connect Tableau to PostgreSQL running inside the VM:
      - Switch your VM network mode to "Bridged Adapter" (or configure NAT port forwarding).
      - Find your VM's IP address using:
            hostname -I
      - In Tableau, choose the PostgreSQL connector and use:
            Host: <VM_IP>
            Port: 5432
            Database: lapd_db
            Username: user
            Password: *****
      - Ensure PostgreSQL is configured to accept remote connections 
        (update `pg_hba.conf` and `postgresql.conf` if necessary).

5) RUN EVERYTHING AUTOMATICALLY
   bash scripts/run_lapd_pipeline.sh

----------------------------------------------
ANALYSES PERFORMED
----------------------------------------------
1. Crime trends (monthly/yearly)
2. Crime type frequency
3. Area-based distribution
4. Victim demographics
5. Weapon usage

----------------------------------------------
LIMITATIONS
----------------------------------------------
- Partial data for 2024
- Some missing demographic values
- Descriptive analytics only (no predictions)

----------------------------------------------
LICENSE
----------------------------------------------
MIT License – Public data

----------------------------------------------
ACKNOWLEDGEMENTS
----------------------------------------------
LA Open Data Portal, Apache Spark, PostgreSQL, Tableau
