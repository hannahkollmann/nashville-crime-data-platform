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

## Data Sources
Metro Nashville Police Department (MNPD) Open Data Portal

Example datasets used:
- MNPD Crime Incidents
- 911 Calls for Service
- Police dispatch activity

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



