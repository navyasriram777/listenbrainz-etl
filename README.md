## Setup Instructions

#### Install Visual Studio/ other IDE

-git clone https://github.com/navyasriram777/listenbrainz-etl.git
-cd listenbrainz-etl

#### Create a Python Virtual Environment named venv

##### In Command Prompt

#### Windows 
- python -m venv venv
- venv\Scripts\activate

#### Mac/Linux
- python3 -m venv venv
- source venv/bin/activate

#### In the Visual Studio : Windows Activate
In Visual Studio Right click on listenbrainz-etl
choose option : Open In Integrated Terminal then run cmd :
 .\venv\Scripts\Activate.ps1

#### Install Dependencies
- pip install -r requirements.txt

#### Add Input JSON Files in DATA FOLDER  
- data/listens_spotify.json : Download the dataset from given link of Google Drive folder: Test Assignment dataset
- data/sample.json : This file has 5 duplicate records and 5 new records , this file is added to test idempotent features like 
    Already Ingested Data
    Duplicate Data

#### Run the Project in the below path
- (venv) PS C:\Users\Public\ScalableCapital\listenbrainz-etl> python .\etl\main.py


### Config Files
- Runtime parameters are provided respective config files
### OUTPUT folder
- Output files has been stored for transformation & analysis tasks results
- `Note`:  for user_count_for_date function of TASK A : output is displayed on screen , since outputis single value file is not created 