from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, datediff, current_date, year, lit, udf
from pyspark.sql.types import StringType, FloatType
import boto3

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LoanApprovalETL") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .getOrCreate()

# Set up AWS credentials (make sure you've configured AWS CLI)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

# Extract
def extract_data(bucket_name, file_name):
    df = spark.read.csv(f"s3a://{bucket_name}/{file_name}", header=True, inferSchema=True)
    return df

# Transform
def clean_data(df):
    # Handle missing values
    df = df.na.fill({
        'CreditScore': df.select('CreditScore').summary().collect()[1]['CreditScore'],
        'AnnualIncome': df.select('AnnualIncome').summary().collect()[1]['AnnualIncome'],
        'LoanAmount': df.select('LoanAmount').summary().collect()[1]['LoanAmount'],
        'EmploymentStatus': 'Unknown',
        'EducationLevel': 'Unknown'
    })

    # Convert ApplicationDate to DateTime
    df = df.withColumn('ApplicationDate', col('ApplicationDate').cast('date'))

    return df

def enrich_data(df):
    # Create Age Groups
    df = df.withColumn('AgeGroup',
        when(col('Age') < 31, '18-30')
        .when((col('Age') >= 31) & (col('Age') <= 50), '31-50')
        .otherwise('51+'))

    # Calculate DebtToAssetRatio
    df = df.withColumn('DebtToAssetRatio', col('TotalLiabilities') / col('TotalAssets'))

    # Create CreditUtilizationRisk
    df = df.withColumn('CreditUtilizationRisk',
        when(col('CreditCardUtilizationRate') < 0.3, 'Low')
        .when((col('CreditCardUtilizationRate') >= 0.3) & (col('CreditCardUtilizationRate') < 0.7), 'Moderate')
        .otherwise('High'))

    # Calculate LoanToIncomeRatio
    df = df.withColumn('LoanToIncomeRatio', col('LoanAmount') / col('AnnualIncome'))

    # Convert LoanDuration to years
    df = df.withColumn('LoanDurationYears', col('LoanDuration') / 12)

    # Calculate new RiskScore
    df = df.withColumn('RiskScore',
        (col('CreditScore') * 0.4) +
        ((1 - col('DebtToIncomeRatio')) * 100 * 0.4) +
        ((5 - col('PreviousLoanDefaults')) * 20 * 0.2))

    return df

def aggregate_data(df):
    # Create Risk Buckets
    df = df.withColumn('RiskBucket',
        when(col('RiskScore') >= 80, 'Low Risk')
        .when((col('RiskScore') >= 50) & (col('RiskScore') < 80), 'Moderate Risk')
        .otherwise('High Risk'))

    # Summarize Loan Defaults
    loan_defaults = df.groupBy('AgeGroup', 'EmploymentStatus') \
        .agg({'PreviousLoanDefaults': 'sum', 'LoanAmount': 'avg'}) \
        .withColumnRenamed('sum(PreviousLoanDefaults)', 'TotalDefaults') \
        .withColumnRenamed('avg(LoanAmount)', 'AvgLoanAmount')

    # Calculate aggregate metrics
    agg_metrics = df.groupBy('AgeGroup', 'EmploymentStatus', 'EducationLevel') \
        .agg({'MonthlyDebtPayments': 'avg', 'LoanAmount': 'avg'}) \
        .withColumnRenamed('avg(MonthlyDebtPayments)', 'AvgMonthlyDebtPayments') \
        .withColumnRenamed('avg(LoanAmount)', 'AvgLoanAmount')

    return df, loan_defaults, agg_metrics

def validate_data(df):
    # Flag high DebtToIncomeRatio and LoanToIncomeRatio
    df = df.withColumn('HighDebtToIncomeRatio', col('DebtToIncomeRatio') > 0.5)
    df = df.withColumn('HighLoanToIncomeRatio', col('LoanToIncomeRatio') > 0.5)

    # Validate LoanAmount and MonthlyLoanPayment consistency
    df = df.withColumn('LoanAmountConsistent',
        (col('LoanAmount') / col('LoanDuration')) >= col('MonthlyLoanPayment'))

    return df

# Load
def load_to_s3(df, bucket_name, file_name):
    df.write.parquet(f"s3a://{bucket_name}/{file_name}", mode="overwrite")

def load_to_redshift(df, table_name):
    # You'll need to set up the Redshift connection properties
    redshift_properties = {
        "url": "jdbc:redshift://your-redshift-cluster:5439/dev",
        "user": "your_username",
        "password": "your_password",
        "driver": "com.amazon.redshift.jdbc42.Driver"
    }

    df.write \
        .format("jdbc") \
        .option("url", redshift_properties["url"]) \
        .option("dbtable", table_name) \
        .option("user", redshift_properties["user"]) \
        .option("password", redshift_properties["password"]) \
        .option("driver", redshift_properties["driver"]) \
        .mode("overwrite") \
        .save()

# Main ETL function
def run_etl():
    # Extract
    raw_data = extract_data("your-s3-bucket", "loan_data.csv")

    # Transform
    cleaned_data = clean_data(raw_data)
    enriched_data = enrich_data(cleaned_data)
    validated_data, loan_defaults, agg_metrics = aggregate_data(enriched_data)
    final_data = validate_data(validated_data)

    # Load
    load_to_s3(final_data, "your-output-s3-bucket", "processed_loan_data")
    load_to_redshift(final_data, "processed_loan_data")
    load_to_redshift(loan_defaults, "loan_defaults_summary")
    load_to_redshift(agg_metrics, "loan_metrics_summary")

if __name__ == "__main__":
    run_etl()
