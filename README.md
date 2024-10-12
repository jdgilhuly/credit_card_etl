# Financial Risk for Loan Approval ETL Project

## Project Overview
This project aims to build an ETL pipeline that processes loan application data using PySpark, Docker, AWS, and Kubernetes for orchestration. The goal is to transform the underlying data to extract meaningful insights into credit risk models, which can be further used to assess loan approval likelihood.

The dataset being used is the **Financial Risk for Loan Approval Dataset**, which contains information about applicants' financial and personal data.

## ETL Pipeline Overview

### 1. **Extract**
- **Source**: The data is extracted from a flat file (CSV) stored in AWS S3.
- **Ingestion**: PySpark is used to load the data into a distributed environment for efficient and large-scale processing.

### 2. **Transform**
- **Data Cleaning**:
  - Handle missing values in key columns such as `CreditScore`, `AnnualIncome`, and `LoanAmount` by imputing values or dropping rows.
  - For categorical variables (`EmploymentStatus`, `EducationLevel`), fill missing values or create an "Unknown" category.
  - Detect and treat outliers in columns like `AnnualIncome`, `CreditScore`, and `LoanAmount`.
  - Convert `ApplicationDate` into a `DateTime` format for time-based analysis.

- **Data Enrichment**:
  - Create **Age Groups** from the `Age` column (e.g., 18-30, 31-50, 51+).
  - Calculate **DebtToAssetRatio** as `TotalLiabilities / TotalAssets`.
  - Create a **CreditUtilizationRisk** feature based on the `CreditCardUtilizationRate` (e.g., "Low", "Moderate", "High").
  - Calculate **LoanToIncomeRatio** as `LoanAmount / AnnualIncome`.
  - Convert `LoanDuration` into years for consistent analysis.
  - Adjust or calculate a new **RiskScore** using `CreditScore`, `DebtToIncomeRatio`, and `PreviousLoanDefaults`.

- **Aggregation and Summarization**:
  - Group applicants into **Risk Buckets** (e.g., "Low Risk", "Moderate Risk", "High Risk") based on their `RiskScore`.
  - Summarize **Loan Defaults** based on factors like age group, loan amount, and employment status.
  - Calculate aggregate metrics like average `MonthlyDebtPayments` and `LoanAmount` for different demographics.

- **Data Validation**:
  - Ensure `DebtToIncomeRatio` and `LoanToIncomeRatio` do not exceed thresholds (e.g., flag when > 50%).
  - Validate that `LoanAmount` and `MonthlyLoanPayment` are logically consistent.

### 3. **Load**
- **AWS S3**: Store both raw and transformed data in Parquet format for optimized storage and querying.
- **Amazon Redshift**: Load aggregated data (e.g., summary of loan approvals, risk assessments) for efficient querying.
- **Dashboard Integration** (optional): Visualize risk assessment and loan approval trends using Amazon QuickSight.

## Orchestration and Automation
- **Docker**: Containerize the PySpark ETL pipeline to ensure portability and consistent environments across development, testing, and production.
- **Kubernetes**: Use Kubernetes for orchestrating and scaling the ETL pipeline, managing the distributed infrastructure for large datasets.
- **AWS Step Functions / Kubernetes Cron Jobs**: Automate regular ETL jobs for data refresh (e.g., daily or weekly).

## Monitoring and Logging
- **AWS CloudWatch**: Implement logging to track the ETL process performance and monitor data quality. Set up alerts for failures or data anomalies.

## Technologies Used
- **PySpark**: For large-scale data processing, transformation, and feature engineering.
- **Docker**: To containerize the ETL pipeline.
- **AWS**: For data storage (S3, Redshift), orchestration (EMR, Lambda), and monitoring (CloudWatch).
- **Kubernetes**: For orchestrating the ETL pipeline and managing the infrastructure.

## Future Enhancements
- Implement real-time risk score calculation using streaming data.
- Expand the ETL pipeline to support multiple loan datasets or integrate external financial data sources for more comprehensive analysis.
