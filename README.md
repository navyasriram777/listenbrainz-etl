### Prerequisites

#### Before running the project, make sure you have:

- Python 3.10+ installed
- Download from [python.org]( https://www.python.org/downloads/)
- During installation, check “Add Python to PATH”

#### Git installed (for cloning the repo)

- Download from [git-scm.com](https://git-scm.com/install/)

### VS Code installed
- [Download](https://code.visualstudio.com/download)
- Install Python extension in VS Code

## Setup Instructions

#### Clone the Repo 

- git clone https://github.com/navyasriram777/listenbrainz-etl.git
- cd listenbrainz-etl

#### Create a Python Virtual Environment

##### Windows :In Command Prompt
- python -m venv venv
##### Mac/Linux :
- python3 -m venv venv

#### Select the Python interpreter in VS Code

- Press Ctrl + Shift + P
- Search Python: Select Interpreter
- Choose:<project-path>\venv\Scripts\python.exe

#### Activate : In the Visual Studio
In Visual Studio Right click on listenbrainz-etl
choose option : Open In Integrated Terminal then run cmd :
 - .\venv\Scripts\Activate.ps1

 #### Activate : In Windows
- venv\Scripts\activate
 #### Activate : Mac/Linux
- source venv/bin/activate

#### Install Dependencies
- pip install -r requirements.txt
- List all the installed packages  : pip list

#### Add Input JSON Files in DATA FOLDER : listenbrainz-etl\data
- data/listens_spotify.json : Download the dataset from given link [of Google Drive folder: Test Assignment dataset](https://drive.google.com/drive/folders/1wnAXYL4BtchW6J8C8YaqOOo9ba6NFOva?usp=sharing)
- data/sample.json : This file has 5 duplicate records and 5 new records , this file is added to test idempotent features like 
    Already Ingested Data,
    Duplicate Data

#### Config Files
- Runtime parameters are provided respective config files

#### Run the Project in the below path
- (venv) PS C:\Users\Public\ScalableCapital\listenbrainz-etl> python .\etl\main.py

#### OUTPUT folder
- Output files has been stored for transformation & analysis tasks results
- `Note`:  for user_count_for_date function of TASK A : output is displayed on screen , since outputis single value file is not created 