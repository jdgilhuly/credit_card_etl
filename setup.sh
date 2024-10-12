#!/bin/bash

# Update package list and install system dependencies
sudo apt-get update
sudo apt-get install -y \
    python3-pip \
    python3-venv \
    openjdk-11-jdk \
    docker.io

# Create a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt

# Install kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl
sudo mv kubectl /usr/local/bin/

# Install AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Clean up
rm awscliv2.zip
rm -rf aws

echo "Setup complete. Please configure your AWS CLI with 'aws configure' and set up your Kubernetes cluster as needed."
