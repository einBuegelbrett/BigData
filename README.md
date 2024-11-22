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

**Due to the limited resources set by Docker, you can use the `LIMIT` variable in `pyspark_get_cards.py` to set the number of pages read from the API.**

---

## ETL Workflow

### 1. **Extraction**
- **Task:** `pyspark_get_cards`
    - **Description:** Fetches card data from the MTG API and uploads it in hdfs.
    - **Business Rule:** Downloaded data must be stored in JSON format with unique filenames using the Airflow execution date.

### 2. **Transformation**
- **Task:** `pyspark_mtg_important_data_to_final`
    - **Description:** Processes raw card data using PySpark to extract important fields (card name and image url).
    - **Business Rule:** Only cards with complete data are retained. The processed data is saved in HDFS in JSON format.

### 3. **Load**
- **Task:** `hdfs_put_mtg_data`
    - **Description:** Uploads raw JSON data to HDFS for long-term storage.
- **Task:** `pyspark_export_cards_to_postgresql`
    - **Description:** Transfers the final processed card data to PostgreSQL for analysis.
  
---

## Airflow DAGs and Tasks

### DAG: **MTG**
**Description:** Orchestrates the entire ETL pipeline for fetching, processing, and storing MTG card data.

#### Tasks:
1. **HDFS Directory Setup**
    - `hdfs_mkdir_raw_cards`, `hdfs_mkdir_final_cards`
    - Create HDFS directories for raw and processed data.
2. **Download data**
    - `pyspark_get_cards`
    - Gets the cards from the API
3. **Process Data**
    - `pyspark_mtg_important_data_to_final`
    - Use PySpark to process and filter card data.
4. **Export to Database**
    - `pyspark_export_cards_to_postgresql`
    - Save the processed data to a PostgreSQL database.

---

## Scripts and Applications

### 1. **Python Scripts**
- **Path:** `airflow/python/`
- **Scripts:**
    - `pyspark_get_cards.py`
      - Gets the cards from the API
    - `pyspark_mtg_important_data.py`
      - Processes raw card data to extract relevant fields.
    - `pyspark_export_cards_db.py`
      - Exports processed card data to PostgreSQL.

### 2. **DDL Scripts**
- **DDL for creating the final data table**
```sql
CREATE TABLE IF NOT EXISTS cards (
    name VARCHAR(255),
    imageUrl VARCHAR(255)
);
```

---

## Backend
- **Folder:** `Backend/`
- **Contains:**
    - `index.ts`: Entry point for the backend application.
    - `Dockerfile`: Environment setup for the backend.
    - `package.json`, `tsconfig.json`: Node.js configuration files for API or backend services.

## Frontend
- **Folder:** `Frontend/`
- **Contains:** Source code for the web application to visualize MTG card data.
