import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .appName("LoanApprovalETLTest")
            .master("local[*]")
            .getOrCreate())

@pytest.fixture(scope="function")
def sample_input_data(spark):
    return spark.createDataFrame([
        ("APP001", 700, 50000.0, 10000.0, "Employed", "Bachelor", 30, 20000.0, 100000.0, 0.3, 0, 36, "2023-01-01"),
        ("APP002", 600, 30000.0, 15000.0, "Self-Employed", "Master", 45, 25000.0, 75000.0, 0.6, 1, 60, "2023-01-02"),
        ("APP003", 750, 80000.0, 20000.0, "Employed", "PhD", 35, 30000.0, 200000.0, 0.2, 0, 48, "2023-01-03"),
    ], ["ApplicationID", "CreditScore", "AnnualIncome", "LoanAmount", "EmploymentStatus", "EducationLevel",
        "Age", "TotalLiabilities", "TotalAssets", "CreditCardUtilizationRate", "PreviousLoanDefaults",
        "LoanDuration", "ApplicationDate"])

@pytest.fixture(scope="module")
def etl_pipeline():
    from src.main import ETLPipeline
    return ETLPipeline()