# nashville-crime-data-platform
An end-to-end data engineering pipeline that ingests, processes, and analyzes public crime data from Nashville, Tennessee. The goal of this project is to transform raw public safety data into a structured analytics platform capable of supporting crime trend analysis and data-driven decision making.

## Project Overview 
The Nashville Crime Data Platform demonstrates how publicly available crime datasets can be transformed into an analytics-ready data warehouse using modern data engineering practices.

The pipeline performs the following steps:
1. Extract raw crime datasets from public data portals
2. Clean and standardize the data using Python
3. Stage data in cloud storage
4. Transform data into a dimensional warehouse model
5. Load data into a SQL analytics environment
6. Run analytical queries to identify crime trends

## Business Problem 
Public crime datasets are often published as raw files that are difficult to analyze at scale. Analysts must manually clean and structure this data before meaningful insights can be generated. This project builds a structured data pipeline that transforms raw Nashville crime datasets into a centralized analytics platform capable of supporting trend analysis, reporting, and future machine learning applications.

## Data Sources
Full datasets are available through the Nashville Open Data Portal.

Example datasets used:
- Metro Nashville Police Deptartment Crime Incidents (https://data.nashville.gov/datasets/Nashville::metro-nashville-police-department-incidents/about)
- 911 Calls for Service (https://data.nashville.gov/datasets/Nashville::metro-nashville-police-department-calls-for-service-2025/about)
- Police dispatch activity (https://datanashvillegov-nashville.hub.arcgis.com/datasets/e8eface241d34e5c8ff1fd4c28ebd93e_0/explore)

## Architecture 
The platform follows a modern data pipeline architecture:

```
Data Source
    Nashville Open Data Portal
            ↓
Raw Data Layer
    AWS S3 Raw Data
            ↓
Processing Layer
    AWS Glue ETL Jobs (Apache Spark)
            ↓
Curated Data Layer
    AWS S3 Curated Data
            ↓
Metadata Layer
    AWS Glue Data Catalog
            ↓
Query Layer
    Amazon Athena SQL
            ↓
Analytics Layer
    Data Analysis & Visualization
```
## Technology Used
Python  
AWS Glue  
AWS S3  
Amazon Redshift  
SQL  
Apache Spark (Glue)  
Jupyter Notebook  
Git / GitHub

## Data Model
The curated data layer follows a dimensional star schema design.

Fact Tables
- fact_incidents
- fact_calls_for_service

Dimension Tables
- dim_date
- dim_time
- dim_location
- dim_offense
- dim_call_type
- dim_call_disposition

## Analysis
This platform allows analysts to explore questions such as:
- How does crime vary by time of day?
  <img width="1206" height="559" alt="image" src="https://github.com/user-attachments/assets/c75f244d-7937-4447-9bca-8228cf0e11cb" />
- Are there seasonal patterns in crime activity?
  <img width="1128" height="558" alt="image" src="https://github.com/user-attachments/assets/e5991011-fc32-4e7c-a795-0dd41285ad65" />

## Key Findings
- Crime activity increases during evening hours
- Crime incidents occur more frequently on weekends
- 911 call volume was a statistically significant predictor of crime incidents

## Implications
-  911 call activity can act as an indicator of crime trends
- Monitoring service demand may help agencies identify emerging crime patterns
- Data can support data-driven policing and resource allocation
- Data platforms can improve situational awareness for public safety agencies

## Future Improvements
- Automate ingestion with scheduled workflows
- Add data quality validation checks
- Integrate real-time crime feeds
- Develop dashboards for visualization
- Expand the platform to support predictive analytics





