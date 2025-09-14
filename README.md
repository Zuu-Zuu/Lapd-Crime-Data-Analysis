# LAPD Crime Data Analytics Pipeline (2020–2024)

This project implements an **end-to-end, scalable data processing pipeline** for analyzing Los Angeles Police Department (LAPD) crime data from 2020 to 2024.  
It uses **Apache Spark, PostgreSQL, and Tableau** to fetch, clean, process, analyze, and visualize crime data at scale.

---

## Features
- **Automated ETL:** Fetch → Clean → Analyze → Store → Visualize  
- **Scalable Processing:** PySpark pipeline handling 1M+ records  
- **Modular Analysis:** Scripts for trends, types, areas, victims, and weapons  
- **PostgreSQL Export:** Results stored for easy querying  
- **Tableau-ready:** Aggregated datasets ready for dashboarding  

---

## Project Structure
```text
scripts/
    fetch_data.py                         # Download LAPD data from API
    clean_data_v2.py                      # Clean and transform raw JSON
    analyze_crime_trend_v2.py             # Monthly/yearly trend analysis
    analyze_crime_by_type_v2.py           # Crime type frequency analysis
    analyze_crime_by_area_v2.py           # Crime distribution by LAPD area
    analyze_crime_by_victim_v2.py         # Victim demographics analysis
    analyze_crime_by_weapon_v2.py         # Weapon usage analysis
    push_all_analysis_to_postgre.py       # Export analysis results to PostgreSQL
    run_lapd_pipeline.sh                  # Automate entire pipeline

data/
    raw/                                  # Raw JSON data from API
    processed_hdfs/                       # Cleaned parquet data extracted from HDFS
    analysis_hdfs/                        # Analysis outputs extracted from HDFS

report/                                   # Documentation, write-up, and final report
```

---

## Technology Stack
- **Languages:** Python (PySpark)  
- **Frameworks:** Apache Spark (distributed processing)  
- **Storage:** HDFS (raw & processed data), PostgreSQL (aggregated results)  
- **Visualization:** Tableau  
- **Environment:** Ubuntu  

---

## Dataset
- **Source:** [LA Open Data Portal – LAPD Crime Data](https://data.lacity.org/)  
- **Period:** Jan 2020 – Dec 2024 (partial)  
- **Size:** 1M+ records  
- **Fields:** Timestamp, Area, Crime Type, Weapon, Victim Age/Sex, Status, etc.  

---

## How to Run the Pipeline

### Prerequisites
- **Install Spark, HDFS, PostgreSQL**
- Python dependencies:
```bash
pip install pyspark psycopg2 requests pandas
```
- PostgreSQL JDBC driver:
  - Download `postgresql-42.7.3.jar`
  - Place it in:  
    `/home/user/lapd-crime-analysis/jars/postgresql-42.7.3.jar`

---

### Start Required Services
```bash
# HDFS
start-dfs.sh

# PostgreSQL (service name may vary)
sudo systemctl start postgresql
```

---

### Database Setup (Run Once)
```bash
sudo -u postgres psql -c "CREATE DATABASE lapd_db;"
sudo -u postgres psql -c "CREATE USER user WITH PASSWORD '*****';"
sudo -u postgres psql -d lapd_db -c "GRANT ALL PRIVILEGES ON DATABASE lapd_db TO user;"
```

---

### Step-by-Step Execution

#### **Step 1: Fetch Data**
```bash
python scripts/fetch_data.py
# Produces data/raw/lapd_crime_2020_2024.json
```

#### **Step 1.5: Upload Raw JSON to HDFS (Optional)**
```bash
hdfs dfs -mkdir -p /lapd_project/raw
hdfs dfs -put -f data/raw/lapd_crime_2020_2024.json /lapd_project/raw/
```

#### **Step 2: Clean & Transform Data**
```bash
spark-submit   --driver-memory 4G   --executor-memory 4G   ~/Desktop/lapd_project/scripts/clean_data_v2.py
# Writes to: /lapd_project/processed/cleaned_parquet_v2
```

#### **Step 3: Run Analyses**
```bash
spark-submit scripts/analyze_crime_trend_v2.py
spark-submit scripts/analyze_crime_by_type_v2.py
spark-submit scripts/analyze_crime_by_area_v2.py
spark-submit scripts/analyze_crime_by_victim_v2.py
spark-submit scripts/analyze_crime_by_weapon_v2.py
# Outputs under: /lapd_project/analysis/*
```

#### **Step 4: Export Results to PostgreSQL**
```bash
spark-submit   --jars /home/user/lapd-crime-analysis/jars/postgresql-42.7.3.jar   ~/Desktop/lapd_project/scripts/push_all_analysis_to_postgre.py
# Creates tables in lapd_db as *_v2
```

#### **Step 5: Visualize in Tableau**
1. Connect to PostgreSQL (lapd_db)  
2. Build interactive charts & dashboards  

> **Tip:** If Tableau is outside the VM, set your VM to *Bridged Adapter* mode or configure NAT port forwarding.  
> Find VM IP: `hostname -I`  
> Configure PostgreSQL to allow remote access (`pg_hba.conf` + `postgresql.conf`).

---

### Run Everything Automatically
```bash
bash scripts/run_lapd_pipeline.sh
```

---

## Analyses Performed
1. Monthly & yearly crime trends  
2. Crime type frequency analysis  
3. Area-wise crime distribution  
4. Victim demographic analysis  
5. Weapon usage breakdown  

---

## Limitations
- Partial data available for 2024  
- Missing or inconsistent demographic fields  
- Focused on descriptive analytics (no ML predictions yet)  

---

## License
MIT License – Public data  

---

## Acknowledgements
- LA Open Data Portal  
- Apache Spark  
- PostgreSQL  
- Tableau  

