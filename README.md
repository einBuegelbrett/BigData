# BigDataMTG Project Documentation

## Project Overview
The **BigDataMTG** project aims to extract, transform, and load (ETL) data about Magic: The Gathering (MTG) cards from a public API into a big data environment and a relational database for analysis and reporting. The system leverages Apache Airflow for orchestration, PySpark for data processing, and PostgreSQL for database storage.

---

## Docker Setup
The system uses a **docker-compose** configuration to manage services:
- Airflow
- HDFS
- PySpark
- PostgreSQL
- Frontend and Backend applications

Just run this docker-compose configuration to run the whole system. Once the containers are running, start the hadoop cluster:
```
docker exec -it hadoop bash
sudo su hadoop
cd
start-all.sh
```

---

## ETL Workflow

### 1. **Extraction**
- **Task:** `download_mtg_cards`
    - **Description:** Fetches card data from the MTG API and stores it in local raw directories.
    - **Business Rule:** Downloaded data must be stored in JSON format with unique filenames using the Airflow execution date.

### 2. **Transformation**
- **Task:** `pyspark_mtg_important_data_to_final`
    - **Description:** Processes raw card data using PySpark to extract important fields (e.g., card name, mana cost, type).
    - **Business Rule:** Only cards with complete data are retained. The processed data is saved in HDFS in JSON format.

### 3. **Load**
- **Task:** `hdfs_put_mtg_data`
    - **Description:** Uploads raw JSON data to HDFS for long-term storage.
- **Task:** `pyspark_export_cards_to_postgresql`
    - **Description:** Transfers the final processed card data to PostgreSQL for analysis.
    - **Business Rule:** Each card name is stored in a normalized table for deduplication and quick lookups.

---

## Airflow DAGs and Tasks

### DAG: **MTG**
**Description:** Orchestrates the entire ETL pipeline for fetching, processing, and storing MTG card data.

#### Tasks:
1. **Create Local Directories**
    - `create_mtg_dir`, `create_raw_dir`, `create_final_dir`
    - Ensure required local directories exist for raw and final data storage.
2. **Clear Old Data**
    - `clear_raw_dir`, `clear_final_dir`
    - Remove old files to prepare for new data.
3. **Download Data**
    - `download_mtg_cards`
    - Fetch card data from the MTG API.
4. **HDFS Directory Setup**
    - `hdfs_mkdir_raw_cards`, `hdfs_mkdir_final_cards`
    - Create HDFS directories for raw and processed data.
5. **Upload to HDFS**
    - `upload_mtg_data_to_hdfs`
    - Upload raw JSON data to HDFS.
6. **Process Data**
    - `pyspark_mtg_important_data_to_final`
    - Use PySpark to process and filter card data.
7. **Export to Database**
    - `pyspark_export_cards_to_postgresql`
    - Save the processed data to a PostgreSQL database.

---

## Scripts and Applications

### 1. **Python Scripts**
- **Path:** `airflow/python/`
- **Scripts:**
    - `pyspark_mtg_important_data.py`
        - Processes raw card data to extract relevant fields.
    - `pyspark_export_cards_db.py`
        - Exports processed card data to PostgreSQL.

### 2. **DDL Scripts**
- **DDL for MTG Raw Data (Hive):**
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS mtg_raw_data (
    id STRING, 
    name STRING, 
    manaCost STRING, 
    colors ARRAY<STRING>, 
    type STRING, 
    rarity STRING, 
    setName STRING, 
    text STRING
)
STORED AS TEXTFILE
LOCATION '/user/hadoop/raw/mtg_cards';
```

---

## Backend
- **Folder:** `Backend/src/`
- **Contains:**
    - `Dockerfile`: Environment setup for the backend.
    - `package.json`, `tsconfig.json`: Node.js configuration files for API or backend services.

## Frontend
- **Folder:** `Frontend/`
- **Contains:** Source code for the web application to visualize MTG card data and calculated KPIs.
