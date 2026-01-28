## Setup Instructions

## Install Visual Studio or any other sutiable IDE

## Clone the Repository from GitHub
My Github repo : https://github.com/navyasriram777/listenbrainz-etl.git

git clone https://github.com/<your-username>/listenbrainz-etl.git
cd listenbrainz-etl

# Create a Python Virtual Environment named venv

# In Command Prompt

# Windows 
python -m venv venv
venv\Scripts\activate

# Mac/Linux
python3 -m venv venv
source venv/bin/activate

## In the Visual Studio : Windows Activate
In Visual Studio Right click on listenbrainz-etl
choose option : Open In Integrated Terminal then run cmd :
 .\venv\Scripts\Activate.ps1

## Install Dependencies
pip install -r requirements.txt

## Add Input JSON Files in DATA FOLDER  
data/listens_spotify.json : Download the dataset from given link of Google Drive folder: Test Assignment dataset
data/sample.json : This file has 5 duplicate records and 5 new records , this file is added to test idempotent features like 
    Already Ingested Data
    Duplicate Data

## Run the Project in the below path
(venv) PS C:\Users\Public\ScalableCapital\listenbrainz-etl> python .\etl\main.py

## Project Layers and Analysis Tasks

### 1. Raw Layer
The Raw Layer ingests ListenBrainz JSON files into DuckDB in their original form and addes house keeping columns like load_id,load_date,load_ts 
- Stores raw data in table : "raw_music_data"  and tracks ingested files in table : "etl_load_control" .  
- Handles already ingested , duplicate and corrupted data  
- Generates a unique "load_id" for traceability of each file.  
- Functions: Drop tables, create tables, ingest single/multiple files, get raw table count.  

---

### 2. Curated Layer
The Curated Layer transforms raw data into a clean, analysis-ready format in table : "curated_music_data".  
- Flattens nested JSON fields (artist_msid, release_msid, artist_name).  
- Converts integer timestamps to readable formats.  
- Deduplicates records using listen_id created by concatenation of : (listened_at,recording_msid,listen_id)
- Exports curated data to CSV for downstream analysis.  

---

### 3. Analysis Tasks

**Task A – User Listening Analysis**  
- Top N users by number of listens.  
- Count of users on a specific date (e.g., 2019-03-01).  
- Each user’s first listened song.  

**Task B – Top Listening Days per User**  
- For each user, identifies the top 3 days with the most listens.   

**Task C – Active Users Analysis**  
- Calculates daily active users and  active users % based on a 7-day rolling window.  

### 4. Config Files
- Runtime parameters are provided respective config files
### 5. OUTPUT folder
- Output files has been stored for transformation & analysis tasks results
- `Note`:  for user_count_for_date function of TASK A : output is displayed on screen , since outputis single value file is not created 