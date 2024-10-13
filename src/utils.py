from pyspark.sql import SparkSession

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'

spark = (SparkSession.builder
         .appName("CreditCardETL")
         .config("spark.driver.extraJavaOptions", "-Xss4M")
         .config("spark.executor.extraJavaOptions", "-Xss4M")
         .config("spark.driver.memory", "4g")
         .config("spark.executor.memory", "4g")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
