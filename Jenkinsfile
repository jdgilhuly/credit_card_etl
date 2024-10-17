pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Install Dependencies') {
            steps {
                sh 'pipenv install --dev'
            }
        }

        stage('Run Tests') {
            steps {
                sh 'pipenv run test'
            }
        }

        stage('Build Docker Image') {
            steps {
                sh 'docker build -t loan-approval-etl .'
            }
        }

        stage('Push to Docker Registry') {
            steps {
                // Replace with your Docker registry details
                sh 'docker push your-registry/loan-approval-etl:latest'
            }
        }

        stage('Deploy to Kubernetes') {
            steps {
                // Replace with your Kubernetes deployment command
                sh 'kubectl apply -f deployment.yaml'
            }
        }
    }

    post {
        always {
            cleanWs()
        }
    }
}