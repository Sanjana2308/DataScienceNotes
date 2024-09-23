# Week 4

## Azure Databricks for Data Engineering
- Introduction to databricks
- Setting up Azure Databricks Workspace
- Setting up Azure Databricks workspace and configuring clusters
- Hands-on Exercise: Creating a Databricks notebook
- Hands-on Exercise: Implementing Databricks for real-time data processing
<br><br>

- Data Exploration and Visualization in Databricks
- Exploratory data analysis (EDA) in Databricks
- Hands-on Exercise: Visualizing data in Databricks
<br><br>

- Mounting ADLS to Databricks , Mount s3 to databricks
<br><br>

- Reading and Writing Data  - 
- Big Data Formats
- Run one notebook from another
- Databricks Ingestion
- Read Data from CSV, JSON,Parquet, table, delta table
- Write Data to CSV, JSON, Parquet, delta, 
- Writing dataframe to Delta Table
<br><br>

- Hands-on Exercise: Analyzing and visualizing streaming data with Databricks

## Azure Databricks Features
- Introduction to Databricks Delta Lake
- Database vs datawareshouse vs data lake vs Delta Lake
- Creating Delta table using 3 methods 
- Merge and Upsert (SCD)
- Internals of delta table
- Optimize delta table
- Time Travel, Z ordering, Vacuum
- Hands-on Exercise: Using Delta Lake for data versioning
- Hands-on Exercise: Building a reliable data lake with Databricks Delta Lake

## Databases and tables on Databricks
- Managed and Unmanaged Table

## Views
- View, Temporary view, Global temporary view

## Incremental Data Processing 
- Streaming Data
- Structured Streaming
- Structured Streaming in Action
- Transformations on Streams

## Introduction to Jobs/Workflow
- Scheduling the Workflow Jobs
- Alerts configurations
- Submitting Jobs using Job Cluster, Creating Job on Databricks using - Notebook
- Cluster attached to Pool

## Build ETL pipelines
- Extracting data from ADLS to Databricks
- Transformations in Databricks using python or SQL
- Writing Data back to Database

## Delta Live Tables
- Introduction Delta Live tables
- Creating Complete ETL using DLT 
- DLT using SQL 
- DLT using Python
- Read, Write, Update, Delete and Merge to delta lake using both PySpark as well as SQL 
- History, Time Travel and Vacuum
- Converting Parquet files to Delta files
- Implementing incremental load pattern using delta lake
- DLT in development and production.
- Run a pipeline

## Unity Catalog
- Overview of Data Governance and Unity Catalog
- Create Unity Catalog Metastore and enable a Databricks workspace with Unity Catalog
- Overview of 3 level namespace and creating Unity Catalog objects
- Configuring and accessing external data lakes via Unity Catalog
- Development of mini project using unity catalog and seeing the key data governance capabilities offered by Unity Catalog such as Data Discovery, Data Audit, Data Lineage and Data Access Control. 

## Databricks SQL
- Introduction, Warehouse, SQL editior, Queries, COPY INTO

## Azure Data Factory
- Creating pipelines to execute Databricks notebooks
- Designing robust pipelines to deal with unexpected scenarios such as missing files
- Creating dependencies between activities as well as pipelines
- Scheduling the pipelines using data factory triggers to execute at regular intervals
- Monitor the triggers/ pipelines to check for errors/ outputs.

## Assignment

## Azure Databricks Coding Assessment