# Banking Data Transformation and Analytics

## 📌 Project Description

This project focuses on building a scalable data pipeline to process and analyze Canadian banking data. 
Using **PySpark on Azure Databricks**, raw financial datasets are transformed—cleaned, normalized, and enriched—for insightful business reporting. 
The final transformed data is stored in **Snowflake** and visualized in **Looker Studio**, enabling stakeholders to explore trends, assess volatility, and make informed investment decisions.

## 🏗️ Data Pipeline Architecture
<img width="1672" height="491" alt="DE_Banking Project drawio" src="https://github.com/user-attachments/assets/5451a20b-4917-42c8-946c-0ac32227d7d6" />


### 🔄 Data Flow Description:
- **yFinance API**: Fetches historical stock data for major Canadian banks.
- **Airflow**: Schedules and manages pipeline tasks.
- **Azure Blob Storage**: Stores raw parquet files as the data lake.
- **Databricks**: Executes PySpark jobs to clean, join, and enrich banking data.
- **Snowflake**: Hosts transformed data for analytics and BI.
- **Looker Studio**: Visualizes KPIs, trends, and volatility insights.

---

## 📊 Looker Studio Components & Descriptions

Below are the major visual components used in the Looker Studio dashboard, along with their tooltip descriptions for accessibility and stakeholder clarity:

| **Component**          | **Description**                                                                 |
|------------------------|------------------------------------------------------------------------------------------|
| **Monthly Volatility** | Displays the relative volatility rank of each bank over time. Useful for risk analysis. |
| **Bank Comparison**    | Compares stock performance metrics across selected Canadian banks.                      |
| **Trend Line (Close)** | Shows closing price trends over time for selected banks.                                |
| **Tab Selector**       | Switch views between performance, volatility, and volume dashboards.                    |
| **Filter Control**     | Allows filtering by bank, date range, or metric type.                                    |

> ℹ️ Enable **"Show chart header"** in Looker Studio to allow tooltip hover info and accessibility tab selection.
> 
<img width="965" height="723" alt="Screenshot 2025-07-25 at 11 49 41 PM" src="https://github.com/user-attachments/assets/ae9af505-3620-49c8-996e-55c16fad27d9" />

---

## 🚀 Future Enhancements
- Integrate real-time data streaming using Kafka.
- Implement dbt for SQL-based data modeling in Snowflake.
- Automate Looker Studio refresh using scheduled scripts or APIs.

---

## 👩‍💻 Technologies Used
- Python, PySpark  
- Azure Blob Storage, Databricks  
- Apache Airflow  
- Snowflake  
- Looker Studio  
- yFinance API





