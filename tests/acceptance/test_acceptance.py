import pytest
from pyspark.sql.functions import col

def test_end_to_end_etl_process(spark, sample_input_data, etl_pipeline):
    # Run the entire ETL process
    result_df = etl_pipeline.run(sample_input_data)

    # Check if the result DataFrame has the expected columns
    expected_columns = ["ApplicationID", "CreditScore", "AnnualIncome", "LoanAmount", "EmploymentStatus",
                        "EducationLevel", "Age", "TotalLiabilities", "TotalAssets", "CreditCardUtilizationRate",
                        "PreviousLoanDefaults", "LoanDuration", "ApplicationDate", "AgeGroup", "DebtToAssetRatio",
                        "DebtToIncomeRatio", "CreditUtilizationRisk", "LoanToIncomeRatio", "LoanDurationYears",
                        "RiskScore", "MonthlyDebtPayments", "RiskBucket", "HighDebtToIncomeRatio",
                        "HighLoanToIncomeRatio", "LoanAmountConsistent"]
    assert set(result_df.columns) == set(expected_columns)

    # Check if the number of rows is the same as the input
    assert result_df.count() == sample_input_data.count()

    # Check if RiskBucket is correctly assigned
    risk_buckets = result_df.select("ApplicationID", "RiskScore", "RiskBucket").collect()
    for row in risk_buckets:
        if row.RiskScore >= 80:
            assert row.RiskBucket == "Low Risk"
        elif 50 <= row.RiskScore < 80:
            assert row.RiskBucket == "Moderate Risk"
        else:
            assert row.RiskBucket == "High Risk"

    # Check if HighDebtToIncomeRatio is correctly flagged
    high_dti_ratios = result_df.filter(col("DebtToIncomeRatio") > 0.5).select("HighDebtToIncomeRatio").collect()
    assert all(row.HighDebtToIncomeRatio for row in high_dti_ratios)

    # Check if HighLoanToIncomeRatio is correctly flagged
    high_lti_ratios = result_df.filter(col("LoanToIncomeRatio") > 0.5).select("HighLoanToIncomeRatio").collect()
    assert all(row.HighLoanToIncomeRatio for row in high_lti_ratios)

    # Check if LoanAmountConsistent is correctly calculated
    loan_amount_consistency = result_df.select("LoanAmount", "LoanDuration", "MonthlyDebtPayments", "LoanAmountConsistent").collect()
    for row in loan_amount_consistency:
        calculated_monthly_payment = row.LoanAmount / row.LoanDuration
        assert row.LoanAmountConsistent == (calculated_monthly_payment <= row.MonthlyDebtPayments)

def test_data_quality(spark, sample_input_data, etl_pipeline):
    result_df = etl_pipeline.run(sample_input_data)

    # Check for null values in important columns
    important_columns = ["ApplicationID", "CreditScore", "AnnualIncome", "LoanAmount", "RiskScore", "RiskBucket"]
    null_counts = result_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in important_columns]).collect()[0]
    assert all(count == 0 for count in null_counts), f"Null values found in important columns: {null_counts}"

    # Check data types of key columns
    assert result_df.schema["ApplicationID"].dataType.simpleString() == "string"
    assert result_df.schema["CreditScore"].dataType.simpleString() == "integer"
    assert result_df.schema["AnnualIncome"].dataType.simpleString() == "float"
    assert result_df.schema["LoanAmount"].dataType.simpleString() == "float"
    assert result_df.schema["RiskScore"].dataType.simpleString() == "double"
    assert result_df.schema["RiskBucket"].dataType.simpleString() == "string"

def test_data_transformations(spark, sample_input_data, etl_pipeline):
    result_df = etl_pipeline.run(sample_input_data)

    # Check if AgeGroup is correctly assigned
    age_groups = result_df.select("Age", "AgeGroup").collect()
    for row in age_groups:
        if row.Age < 31:
            assert row.AgeGroup == "18-30"
        elif 31 <= row.Age <= 50:
            assert row.AgeGroup == "31-50"
        else:
            assert row.AgeGroup == "51+"

    # Check if DebtToAssetRatio is correctly calculated
    debt_to_asset_ratios = result_df.select("TotalLiabilities", "TotalAssets", "DebtToAssetRatio").collect()
    for row in debt_to_asset_ratios:
        expected_ratio = row.TotalLiabilities / row.TotalAssets
        assert abs(row.DebtToAssetRatio - expected_ratio) < 0.01  # Allow for small floating-point differences

    # Check if LoanDurationYears is correctly calculated
    loan_durations = result_df.select("LoanDuration", "LoanDurationYears").collect()
    for row in loan_durations:
        expected_years = row.LoanDuration / 12
        assert abs(row.LoanDurationYears - expected_years) < 0.01  # Allow for small floating-point differences