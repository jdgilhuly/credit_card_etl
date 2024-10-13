import os
from pyspark.sql.functions import col, when, datediff, current_date, year, lit, udf
from pyspark.sql.types import StringType, FloatType
import boto3
from utils import spark

# Load environment variables
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.environ.get('AWS_DEFAULT_REGION')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
REDSHIFT_HOST = os.environ.get('REDSHIFT_HOST')
REDSHIFT_PORT = os.environ.get('REDSHIFT_PORT')
REDSHIFT_DATABASE = os.environ.get('REDSHIFT_DATABASE')
REDSHIFT_USER = os.environ.get('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.environ.get('REDSHIFT_PASSWORD')


class Extractor:
    @staticmethod
    def extract_data_from_s3(file_name):
        # Set up AWS credentials
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"s3.{AWS_DEFAULT_REGION}.amazonaws.com")

        df = spark.read.csv(f"s3a://{S3_BUCKET_NAME}/{file_name}", header=True, inferSchema=True)
        return df

class Transformer:
    def __init__(self):
        pass

    def clean_data(self, df):

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

    def enrich_data(self, df):
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

    def aggregate_data(self, df):

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

    def validate_data(self, df):
        # Flag high DebtToIncomeRatio and LoanToIncomeRatio
        df = df.withColumn('HighDebtToIncomeRatio', col('DebtToIncomeRatio') > 0.5)
        df = df.withColumn('HighLoanToIncomeRatio', col('LoanToIncomeRatio') > 0.5)

        # Validate LoanAmount and MonthlyLoanPayment consistency
        df = df.withColumn('LoanAmountConsistent',
            (col('LoanAmount') / col('LoanDuration')) >= col('MonthlyLoanPayment'))

        return df



class Loader:
    @staticmethod
    def load_to_s3(df, file_name):
        df.write.parquet(f"s3a://{S3_BUCKET_NAME}/{file_name}", mode="overwrite")

    @staticmethod
    def load_to_redshift(df, table_name):
        redshift_properties = {
            "url": f"jdbc:redshift://{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DATABASE}",
            "user": REDSHIFT_USER,
            "password": REDSHIFT_PASSWORD,
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
    raw_data = Extractor.extract_data_from_s3("loan_data.csv")

    # Transform
    transformer = Transformer()
    cleaned_data = transformer.clean_data(raw_data)
    enriched_data = transformer.enrich_data(cleaned_data)
    validated_data, loan_defaults, agg_metrics = transformer.aggregate_data(enriched_data)
    final_data = transformer.validate_data(validated_data)

    # Load
    Loader.load_to_s3(final_data, "processed_loan_data")
    Loader.load_to_redshift(final_data, "processed_loan_data")
    Loader.load_to_redshift(loan_defaults, "loan_defaults_summary")
    Loader.load_to_redshift(agg_metrics, "loan_metrics_summary")

if __name__ == "__main__":
    run_etl()
