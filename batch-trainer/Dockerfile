# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies required for PySpark
# - openjdk-17-jdk-headless for Java runtime (updated from version 11)
# - procps for the 'ps' command
RUN apt-get update && apt-get install -y openjdk-17-jdk-headless procps && rm -rf /var/lib/apt/lists/*

# Set environment variable for the new Java version
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code into the container
COPY . .

# Command to run the application
CMD ["python", "run_trainer.py"]