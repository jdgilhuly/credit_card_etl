# Financial Risk for Loan Approval ETL Project

## Project Overview
This project aims to build an ETL pipeline that processes loan application data using PySpark, Docker, AWS, and Kubernetes for orchestration. The goal is to transform the underlying data to extract meaningful insights into credit risk models, which can be further used to assess loan approval likelihood.

The dataset being used is the **Financial Risk for Loan Approval Dataset**, which contains information about applicants' financial and personal data.
https://www.kaggle.com/datasets/lorenzozoppelletto/financial-risk-for-loan-approval/data


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


## Setup

### Prerequisites
- Python 3.8+
- pip
- AWS CLI configured with your credentials

### Pipenv Setup

1. Install Pipenv if you haven't already:
   ```bash
   pip install pipenv
   ```

2. Clone the repository and navigate to the project directory:
   ```bash
   git clone https://github.com/your-username/financial-risk-loan-approval-etl.git
   cd financial-risk-loan-approval-etl
   ```

3. Create a virtual environment and install dependencies:
   ```bash
   pipenv install
   ```

4. Activate the virtual environment:
   ```bash
   pipenv shell
   ```

5. Install project dependencies:
   ```bash
   pipenv install pyspark boto3
   ```

### AWS and Docker Setup

1. Configure AWS CLI (if not already done):
   ```bash
   aws configure
   ```

2. Build the Docker image:
   ```bash
   docker build -t loan-approval-etl .
   ```

### Running the ETL Job

To run the ETL job using Pipenv:

```bash
pipenv run python src/main.py
```

To run the ETL job using Docker:

```bash
docker run loan-approval-etl
```

## Notes
```bash
chmod +x setup.sh
./setup.sh
aws configure
docker build -t loan-approval-etl .
```