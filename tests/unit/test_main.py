import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from src.main import Extractor, Transformer, Loader

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("UnitTest").getOrCreate()

@pytest.fixture(scope="module")
def sample_data(spark):
    schema = StructType([
        StructField("ApplicationID", StringType(), True),
        StructField("CreditScore", IntegerType(), True),
        StructField("AnnualIncome", FloatType(), True),
        StructField("LoanAmount", FloatType(), True),
        StructField("EmploymentStatus", StringType(), True),
        StructField("EducationLevel", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("TotalLiabilities", FloatType(), True),
        StructField("TotalAssets", FloatType(), True),
        StructField("CreditCardUtilizationRate", FloatType(), True),
        StructField("PreviousLoanDefaults", IntegerType(), True),
        StructField("LoanDuration", IntegerType(), True),
        StructField("ApplicationDate", DateType(), True)
    ])

    data = [
        ("APP001", 700, 50000.0, 10000.0, "Employed", "Bachelor", 30, 20000.0, 100000.0, 0.2, 0, 36, "2023-01-01"),
        ("APP002", 650, 40000.0, 15000.0, "Self-employed", "Master", 45, 30000.0, 80000.0, 0.5, 1, 48, "2023-01-02"),
        ("APP003", None, None, None, None, None, 55, 40000.0, 120000.0, 0.8, 2, 60, "2023-01-03")
    ]

    return spark.createDataFrame(data, schema)

def test_extractor_local(spark, tmp_path):
    # Create a temporary CSV file
    df = spark.createDataFrame([("1", "John"), ("2", "Jane")], ["id", "name"])
    temp_file = tmp_path / "test_data.csv"
    df.write.csv(str(temp_file), header=True)

    # Test extraction
    extractor = Extractor()
    result = extractor.extract_data_from_local(str(temp_file))

    assert result.count() == 2
    assert result.columns == ["id", "name"]

def test_transformer_clean_data(sample_data):
    transformer = Transformer()
    cleaned_data = transformer.clean_data(sample_data)

    assert cleaned_data.filter(cleaned_data.CreditScore.isNull()).count() == 0
    assert cleaned_data.filter(cleaned_data.AnnualIncome.isNull()).count() == 0
    assert cleaned_data.filter(cleaned_data.LoanAmount.isNull()).count() == 0
    assert cleaned_data.filter(cleaned_data.EmploymentStatus == "Unknown").count() == 1
    assert cleaned_data.filter(cleaned_data.EducationLevel == "Unknown").count() == 1

def test_transformer_enrich_data(sample_data):
    transformer = Transformer()
    cleaned_data = transformer.clean_data(sample_data)
    enriched_data = transformer.enrich_data(cleaned_data)

    assert "AgeGroup" in enriched_data.columns
    assert "DebtToAssetRatio" in enriched_data.columns
    assert "CreditUtilizationRisk" in enriched_data.columns
    assert "LoanToIncomeRatio" in enriched_data.columns
    assert "LoanDurationYears" in enriched_data.columns
    assert "RiskScore" in enriched_data.columns

def test_transformer_aggregate_data(sample_data):
    transformer = Transformer()
    cleaned_data = transformer.clean_data(sample_data)
    enriched_data = transformer.enrich_data(cleaned_data)
    main_df, loan_defaults, agg_metrics = transformer.aggregate_data(enriched_data)

    assert "RiskBucket" in main_df.columns
    assert loan_defaults.count() > 0
    assert agg_metrics.count() > 0

def test_transformer_validate_data(sample_data):
    transformer = Transformer()
    cleaned_data = transformer.clean_data(sample_data)
    enriched_data = transformer.enrich_data(cleaned_data)
    main_df, _, _ = transformer.aggregate_data(enriched_data)
    validated_data = transformer.validate_data(main_df)

    assert "HighDebtToIncomeRatio" in validated_data.columns
    assert "HighLoanToIncomeRatio" in validated_data.columns
    assert "LoanAmountConsistent" in validated_data.columns

def test_loader_s3(spark, mocker):
    mock_df = mocker.Mock()
    mock_df.write.parquet = mocker.Mock()

    loader = Loader()
    loader.load_to_s3(mock_df, "test_file")

    mock_df.write.parquet.assert_called_once_with("s3a://None/test_file", mode="overwrite")

def test_loader_redshift(spark, mocker):
    mock_df = mocker.Mock()
    mock_df.write.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value.option.return_value.mode.return_value.save = mocker.Mock()

    loader = Loader()
    loader.load_to_redshift(mock_df, "test_table")

    mock_df.write.format.assert_called_once_with("jdbc")
    mock_df.write.format().option.assert_called_with("url", "jdbc:redshift://None:None/None")
    mock_df.write.format().option().option.assert_called_with("dbtable", "test_table")
    mock_df.write.format().option().option().option.assert_called_with("user", None)
    mock_df.write.format().option().option().option().option.assert_called_with("password", None)
    mock_df.write.format().option().option().option().option().option.assert_called_with("driver", "com.amazon.redshift.jdbc42.Driver")
    mock_df.write.format().option().option().option().option().option().mode.assert_called_with("overwrite")
    mock_df.write.format().option().option().option().option().option().mode().save.assert_called_once()
