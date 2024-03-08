# YouTube Data Harvesting and Warehousing


This is a Python Application 

Utilizes Google API for fetching YouTube channel data.

Stores data in MongoDB and PostgreSQL databases.

Streamlit interface for user interaction and data visualization.

## Installation

The application uses psycopg2, pymongo, streamlit and google-client-api libraries.

The following commands installs the required libraries to run the application

```bash
pip install pycopg2

pip install pymongo

pip install streamlit

pip install google-api-python-client
```

## Running the Application

Update the API key from Google to comsume the YouTube API. Insert the connection strings for MongoDB and Postgres Databases.

Run the application code 

```bash
streamlit run /path/to/capstone-project.py
```      

The application is accessble from browser at

```bash
http://localhost:8501/

or 

http://127.0.0.1:8501/
```  
