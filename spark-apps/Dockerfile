# Use the Bitnami Spark image as the base
FROM bitnami/spark:3.5

# Switch to a user with permissions to install packages
USER root

# Copy the requirements file into the container
COPY requirements.txt /tmp/

# Install the required Python packages
RUN pip install textblob

# Switch back to the default non-root user
USER 1001