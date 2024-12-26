FROM bitnami/spark:latest

# Set the working directory
WORKDIR /opt/bitnami/spark

# Copy the Python requirements file
COPY requirements.txt ./requirements.txt

# Switch to root user for installations
USER root

# Install Python and dependencies
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install --no-cache-dir -r ./requirements.txt && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the default user (optional, if Bitnami sets it)
USER 1001
