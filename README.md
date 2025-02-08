# Spotlight on the Windy City

This repository contains all the files and scripts created and used for the project developed as part of the **Laboratory of Data Science** module of the **Decision Support Systems** course within the **Master Programme in Data Science and Business Informatics** at the **Department of Computer Science** of Università di Pisa.

The project focuses on analyzing traffic incidents in **Chicago** using data sourced from Kaggle, aiming to simulate a **Decision Support System** for an insurance company. The dataset contains information on crashes, people involved, and vehicles involved, spanning from January 2014 to January 2019. The project consists of two main parts:
1. **Database Design, Cleaning, and Population using Python and SQL Server Integration Services (SSIS)**
2. **Analysis and Business Intelligence using SQL Server Analysis Services (SSAS), MDX Queries, and Power BI Dashboards**

---
## Repository Structure
The repository is organized as follows:

```
├── data/
│   ├── raw/                  # Raw datasets (Crashes.csv, People.csv, Vehicles.csv)
│   ├── cleaned/               # Cleaned and processed datasets
│   ├── warehouse/             # Data warehouse schema and tables
│
├── scripts/
│   ├── cleaning/              # Python scripts for data cleaning and preprocessing
│   ├── ETL/                   # SSIS package and SQL scripts for data integration
│   ├── analysis/              # MDX queries and Power BI dashboard files
│
├── reports/
│   ├── documentation/         # Project documentation and explanations
│   ├── visualizations/        # Graphs and dashboards from Power BI
│
├── README.md                  # Project overview and instructions
└── LICENSE                     # License information
```

---
## Project Breakdown
### **Part 1: Data Preparation and Warehouse Design**
#### 1. Data Understanding and Cleaning
- Cleaning and preprocessing datasets using Python.
- Handling missing values using techniques such as reverse geocoding and external data sources.
- Scripts:
  - `crashesDataCleaning.py`
  - `peopleDataCleaning.py`
  - `vehiclesDataCleaning.py`

#### 2. Data Warehouse Schema
- Designed using a **star schema** with one fact table (`DamageToUser`) and eight dimension tables:
  - `DateDimension`
  - `PersonDimension`
  - `VehicleDimension`
  - `CrashReportDimension`
  - `CauseDimension`
  - `InjuryDimension`
  - `LocationDimension`
  - `WeatherDimension`

#### 3. Data Upload and SSIS Implementation
- **ETL Process:** Data transformation and upload using Python and **SQL Server Integration Services (SSIS)**.
- Implemented sampling and controlled flow for **incremental data uploads**.
- Scripts:
  - `dataUpload.py`
  - `ssis_dataflow.dtsx`

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
## Setup & Usage
### **1. Requirements**
- Python (>=3.8)
- Microsoft SQL Server (>=2019) with SSIS & SSAS
- Power BI Desktop
- Required Python Libraries:
  ```bash
  pip install pandas numpy geopy shapely h3
  ```

### **2. Running the Scripts**
- **Data Cleaning:**
  ```bash
  python scripts/cleaning/crashesDataCleaning.py
  python scripts/cleaning/peopleDataCleaning.py
  python scripts/cleaning/vehiclesDataCleaning.py
  ```
- **Data Upload to SQL Server:**
  ```bash
  python scripts/ETL/dataUpload.py
  ```
- **SSIS Package Execution:**
  - Open `ssis_dataflow.dtsx` in **SQL Server Data Tools (SSDT)** and run the package.
- **MDX Queries:**
  - Run queries from `scripts/analysis/mdx_queries.md` in **SQL Server Management Studio (SSMS)**.
- **Power BI Dashboards:**
  - Open `reports/visualizations/*.pbix` in **Power BI Desktop**.

---
## Contributors
- **Miccoli Martin**
- **Montinaro Aldo**
- **Poiani Marco**

---
## License
This project is licensed under the MIT License - see the `LICENSE` file for details.

---
## References
- [City of Chicago Data Portal](https://data.cityofchicago.org/)
- [Traffic Crashes Dataset on Kaggle](https://www.kaggle.com/datasets/isadoraamorim/trafficcrasheschicago)

