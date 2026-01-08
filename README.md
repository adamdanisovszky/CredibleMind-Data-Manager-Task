# CredibleMind-Data-Manager-Task
Repository for the Data Manager Take-Home Assessment with CredibleMind


# 1) GitHub Repository


# 2) Python Ingestion Script
○ See "ingest.py" fole for the code.  
○ This code was performed entirely with the help of AI (Claude).  
○ First pass completed the data extraction via API, but an error occurred with the SERVICE_ACCOUNT_KEY.  
  • Second, Third, and Fourth passes failed due to timeouts before the extraction portion completed.  
  • I have instead gone the route of downloading the data directly as a CSV in order to move forward with the project.  

# 3) BigQuery Storage
○ BRFSS data extracted in CSV form directly from the CDC site, saved to Google Drive, then uploaded into BigQuery
○ The core of that work took about 20m, with about 40m spent setting up and getting reacclimated with BigQuery

# 4) dbt Project
○ Due to time and knowledge constraints, I have skipped this step. I have not yet worked with dbt directly, but I am very eager to learn more.

# 5) BI Dashboard
○ There is no fully free version of Sigma available, so I have opted to use Looker Studio since it is free and can tap straight into BigQuery data. 
○ I will also be including an example built in Sigma based on their included data sources they provide for their "Workout Wednesday" exercises. 

# 6) AI Usage


# 7) Time Spent
○ GitHub Repository... 15m  
○ Python Ingestion Script... 120m  
○ BigQuery Storage... 60m  
○ dbt Project... 0m  
○ BI Dashboard...   
○ README... 20m  
