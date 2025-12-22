# App-DRE — Automated Income Statement Data Pipeline

##  Overview
**App-DRE** is a web application designed to automate the consolidation of financial and operational data from multiple organizational departments to generate a standardized **Income Statement (DRE)** used for executive reporting and analytics.

The application replaces a highly manual process based on heterogeneous Excel files, significantly reducing processing time, improving data quality, and enabling faster and more reliable financial analysis in **Microsoft Fabric** and **Power BI**.

---

##  Business Problem
Before the application:
- Financial data was received from multiple departments in different Excel formats  
- Data cleaning, validation, and consolidation were fully manual  
- The Income Statement preparation process took **up to 30 days**  
- There was a high risk of inconsistencies, rework, and delayed decision-making  

---

## Solution
The application provides a **modular, automated data pipeline**, where each financial process is handled independently and later consolidated into a unified dataset.

### Main processing modules include:
- Inventory Movements  
- Billing and Taxes  
- Accounting Expenses  
- Payments  
- Outsourced Services Tracking  

Each module:
- Validates input files  
- Cleans and standardizes data  
- Applies business rules  
- Generates structured and traceable datasets  

At the end of the process, all datasets are consolidated and exported to **Microsoft Fabric**, where the **Income Statement (DRE)** is modeled and visualized in **Power BI**.

---

## Impact & Results
-  **Processing time reduction:**  
  - From **~30 days** → **~5 hours**
-  **Data cleaning time:**  
  - Reduced to **~5 minutes**
- Improved data quality, traceability, and governance  
- Faster and more reliable decision-making  

---

## Architecture Overview

### Data Flow
1. Excel file upload (multiple formats and departments)  
2. Automated validation and standardization  
3. Modular transformation pipelines  
4. Unified dataset generation  
5. Export to Microsoft Fabric  
6. Income Statement (DRE) visualization in Power BI  

---

## Tech Stack
- **Backend:** Python, Flask  
- **Data Processing:** Pandas, Excel Automation  
- **Frontend:** HTML, Jinja Templates  
- **Analytics & BI:** Microsoft Fabric, Power BI  
- **Deployment:** Render  

---

## Deployment
The application is deployed on **Render**, allowing users to upload files, execute data processing pipelines, and generate standardized outputs through a web interface.

---

## Project Structure
```text
App-DRE/
│
├── routes/           # Application routes and processing modules
├── templates/        # HTML templates
├── utils/            # Data processing and validation utilities
├── app.py            # Main Flask application
├── requirements.txt  # Project dependencies
└── Procfile          # Deployment configuration
