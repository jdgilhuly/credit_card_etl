#!/bin/bash

# Update the system
sudo yum update -y

# Install Java (Amazon Corretto 11)
sudo amazon-linux-extras install java-openjdk11 -y

# Install Python 3 and pip
sudo yum install python3 python3-pip -y

# Install Git
sudo yum install git -y

# Install Apache Spark
SPARK_VERSION="3.1.2"
HADOOP_VERSION="3.2"
wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
sudo tar -xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/
sudo ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# Set environment variables
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk" >> /home/ec2-user/.bashrc
echo "export SPARK_HOME=/opt/spark" >> /home/ec2-user/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin" >> /home/ec2-user/.bashrc
echo "export PYSPARK_PYTHON=/usr/bin/python3" >> /home/ec2-user/.bashrc

# Install pipenv
sudo pip3 install pipenv

# Clone your repository (replace with your actual repo URL)
git clone https://github.com/yourusername/credit_card_etl.git /home/ec2-user/credit_card_etl

# Change to the project directory
cd /home/ec2-user/credit_card_etl

# Install project dependencies
pipenv install

# Run your PySpark application
pipenv run python3 src/main.py