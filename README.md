# Spotlight on the Windy City
![img](https://github.com/aldomontinaroam/_lds24_files/blob/main/DATA/cover.png)
This repository contains all the files and scripts created and used for the project developed as part of the **Laboratory of Data Science** module of the **Decision Support Systems** course within the **Master Programme in Data Science and Business Informatics** at the **Department of Computer Science** of Università di Pisa. The project focuses on analyzing traffic incidents in **Chicago** using crashes data, aiming to simulate a **Decision Support System** for an insurance company. The dataset contains information on crashes, people involved, and vehicles involved, spanning from January 2014 to January 2019. The project consists of two main parts:
1. **Database Design, Cleaning, and Population using Python and SQL Server Integration Services (SSIS)**
2. **Analysis and Business Intelligence using SQL Server Analysis Services (SSAS), MDX Queries, and Power BI Dashboards**
---
## Repository Structure
The repository is organized as follows:

```
├── DATA/                      # Raw datasets provided by the lecturer
│
├── Part1/
│   ├── DataCleaning_scripts/  # Python scripts for data cleaning and preprocessing
│   ├── SSIS/                  # SSIS package and files
│   ├── EDA_scripts/           # Python scripts for exploratory data analysis
│
├── Part2/
│   ├── GROUP_8_CUBE/          # SSAS cube project files
│   ├── PowerBI Dashboards/    # Graphs and dashboards from Power BI
│
├── README.md                  # Project overview and instructions
└── LDS2425_Group8_FullReport.pdf   # Project report
```

---
## Project Breakdown
### **Part 1: Data Preparation and Warehouse Design**
#### 1. Data Understanding and Cleaning
- Cleaning and preprocessing datasets using Python.
- Handling missing values using techniques such as reverse geocoding and external data sources.
- Scripts:
  - `crashesDataCleaning.py` and `utils_crashes.py`
  - `peopleDataCleaning.py`
  - `vehiclesDataCleaning.py`

#### 2. Data Warehouse Schema
- Designed using a **star schema** with one fact table (`DamageToUser`) and eight dimension tables: ![schema](https://github.com/aldomontinaroam/_lds24_files/blob/main/DATA/sql_server_dw_schema.png)

#### 3. Data Upload and SSIS Implementation
- **ETL Process:** Data transformation and upload using Python and **SQL Server Integration Services (SSIS)**.
- Implemented sampling and controlled flow for **incremental data uploads**.
- Scripts:
  - `duplicateTables_SSIS.py`: duplicates tables used for 10% sampling from SSIS
  - `split_tables.py`: merges raw datasets and creates data mart tables
  - `upload_data.py`: uploads data into the designed database

#### 4. Business Questions Using SSIS
- Implemented **aggregate queries** to answer key business questions:
  - Participant crash frequency per year.
  - Day/Night crash index per police beat.
  - Age-based risk factors.
  - Impact of contributory causes on injury severity and damage costs.

---
### **Part 2: Business Intelligence and Analysis**
#### 5. Data Cube Creation with SSAS
- Designed an **OLAP data cube** in **SQL Server Analysis Services (SSAS)**.
- Defined **measures and hierarchies** for efficient querying.
- Created precomputed **calculated members** for performance optimization.

#### 6. MDX Queries for Business Insights
- Developed **MDX queries** to analyze crash trends:
  - Average yearly damage costs per person.
  - Year-over-year changes in damage costs per location.
  - Identifying high-risk crash types based on injury severity.

#### 7. Power BI Dashboards
- Developed **interactive dashboards** to visualize insights:
  - **Geographical distribution** of accidents and damages.
  - **Street-wise analysis** of traffic incidents.
  - **Person-centric analysis** of injuries and responsibilities.

---
## Requirements**
- Python (>=3.8)
- Microsoft SQL Server (>=2019) with SSIS & SSAS
- Power BI Desktop
- Required Python Libraries:
  ```bash
  pip install pandas numpy geopy shapely h3
  ```

---
## Contributors
- [**Miccoli Martin**](https://github.com/Martinmiccoli)
- [**Montinaro Aldo**](https://github.com/aldomontinaroam)
- [**Poiani Marco**](https://github.com/MarcoPoiani00)

---
## References
- [City of Chicago Data Portal](https://data.cityofchicago.org/)
- [Traffic Crashes Dataset on Kaggle](https://www.kaggle.com/datasets/isadoraamorim/trafficcrasheschicago)

