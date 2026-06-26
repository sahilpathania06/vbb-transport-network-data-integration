# VBB Transport Network Data Integration Using Apache Spark

## Overview

This repository contains an individual project developed for the B142 Data Integration module at GISMA University of Applied Sciences.
The project showcases the design and implementation of a complete ETL (Extract, Transform, Load) pipeline powered by Apache Spark and Databricks. 
It utilizes the Verkehrsverbund Berlin-Brandenburg (VBB) GTFS public transit dataset.The developed pipeline manages the entire data lifecycle, including ingestion, quality validation, integration, analytical processing, and exporting the final dataset in Parquet format.

---

## Project Objectives

The objectives of this project are :-

a. Loading several GTFS datasets into Apache Spark.
b. Performing data quality validation using row count verification, null value analysis, duplicate detection, and referential integrity checks.
c. Integrating multiple GTFS datasets using Spark DataFrame join operations.
d. Generating analytical insights from the integrated transportation dataset.
e. xporting the final integrated dataset in Parquet format for efficient storage and future analysis.

---

## Dataset

The project uses the "VBB GTFS (General Transit Feed Specification)" dataset provided by "Verkehrsverbund Berlin-Brandenburg (VBB)".

Datasets used in this project:

a. agency.txt
b. routes.txt
c. trips.txt
d. stops.txt
e. stop_times.txt

Dataset Source:

https://daten.berlin.de/datensaetze/vbb-fahrplandaten-via-gtfs

---

## Technology Stack

a. Apache Spark
b. PySpark
c. Databricks
d. GTFS Dataset
e. Parquet

---

## ETL Pipeline

```

GTFS Files - Data Ingestion - Data Quality Validation - Data Integration - Analytics and Insights - Parquet Export

---



## Project Workflow

### 1. Data Ingestion

GTFS datasets are loaded into Apache Spark using a reusable PySpark function load_gtfs_file().

### 2. Data Quality Validation

The datasets are validated using:

a. Row Count Verification
b. Null Value Analysis
c. Duplicate Detection
d. Referential Integrity Validation

### 3. Data Integration

The datasets are integrated using Spark DataFrame operations, including:

a.  Inner Join
b. Left Anti Join
c. Left Semi Join

### 4. Analytics

The integrated dataset is analysed to answer the following questions:

a.  Which agencies operate the highest number of routes?
b. Which routes operate the highest number of trips?
c. Which stops are visited most frequently?
d. How are transport types distributed?
e. Which stops serve as the most important transport hubs?

### 5. Parquet Export

The final integrated dataset is exported in Parquet format for efficient storage and analytical processing.



## How to Run

1. Import the `.dbc` archive into Databricks or use the Python notebook source files.
2. Execute the notebooks in the following order:

```
1. ingestion_vbb
2. examination_vbb
3. integration_vbb
4. Analytics_vbb
5. parquet_export_vbb
```

---

## Project Report

The final report is available in the `report` folder.

---

## Video Demonstration

Google Drive:

https://drive.google.com/file/d/1skajSb0o4EENd7dsBR7Oq1OiIL1TLQ7E/view

---

## Author

Sahil Pathania

Student ID: GH1041137

GISMA University of Applied Sciences

Module: B142 Data Integration
