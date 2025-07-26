
# 🧱 U.S. Crime Report Data Engineering Project

**Author**: Sunny Samson Badam  
**Type**: Data Engineering  
**Tools Used**: Apache Airflow · MySQL · Python · Pandas · AWS S3 · dbt

---

## 📌 Project Overview

This project builds an end-to-end data pipeline to process, clean, and analyze U.S. Crime Report data. The pipeline extracts raw CSV files, transforms the data into a normalized structure, and loads it into a MySQL database. The result supports efficient querying and downstream analytics.

---

## 🗃️ Dataset

- **Source**: [U.S. Crime Report Dataset by Arpit Singh](https://www.kaggle.com/datasets/arpitsingh/us-crime-report)
- **Format**: CSV
- **Size**: 250MB
- **Features**:

*Core Crime Information*

DR_NO: Unique identifier for each reported crime.
Date Rptd: Date the crime was reported to law enforcement.
DATE OCC: Date the crime occurred.
TIME OCC: Time the crime occurred.
AREA: Geographic area or precinct where the crime took place.
AREA NAME: Descriptive name of the area.
Rpt Dist No: Reporting district number.

*Crime Classification*

Part 1-2: Indicates whether the crime is a Part 1 (serious) or Part 2 (less serious) offense.
Crm Cd: Crime code or classification number.
Crm Cd Desc: Description of the crime code.
Mocodes: Motivations or circumstances related to the crime.

*Victim Information*

Vict Age: Age of the victim.
Vict Sex: Sex of the victim.
Vict Descent: Racial or ethnic background of the victim.

*Location and Context*

Premis Cd: Premises code (e.g., residential, commercial).
Premis Desc: Description of the premises.
Weapon Used Cd: Code for the weapon used (if any).
Weapon Desc: Description of the weapon.

*Additional Information*

Status: Current status of the case (e.g., open, closed).
Status Desc: Description of the case status.
Crm Cd 1, 2, 3, 4: Additional crime codes if applicable.
LOCATION: General location of the crime.
Cross Street: Intersection or nearby street.
LAT, LON: Latitude and longitude coordinates of the crime location.

---

## 🏗️ Architecture


          ┌──────────────┐
          │   Raw Data   │
          └──────┬───────┘
                 ↓
       ┌──────────────────┐
       │   Data Ingestion │  ← (Python Script / Airflow DAG)
       └──────────────────┘
                 ↓
       ┌──────────────────┐
       │ Data Transformation │  ← (Pandas / Spark / dbt)
       └──────────────────┘
                 ↓
       ┌──────────────────┐
       │ MySQL / S3  │  ← Final tables (fact, dimension)
       └──────────────────┘
                 ↓
       ┌──────────────────┐
       │  Analysis / Viz  │  ← Jupyter, BI Tool, Tableau
       └──────────────────┘
