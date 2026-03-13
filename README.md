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
Raw Public Data
      ↓
Python Data Ingestion
      ↓
AWS S3 Storage
      ↓
Amazon Redshift Warehouse
      ↓
SQL Analytics Layer
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


## Analysis
This platform allows analysts to explore questions such as:
- Which areas of Nashville experience the highest crime frequency?
- How does crime vary by time of day?
- Which offense types are increasing year over year?
- Are there seasonal patterns in crime activity?

## Future Improvements
- Automate ingestion with scheduled workflows
- Add data quality validation checks
- Integrate real-time crime feeds
- Develop dashboards for visualization
- Expand the platform to support predictive analytics






