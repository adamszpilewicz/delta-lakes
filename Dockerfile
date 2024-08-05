FROM apache/spark

# Switch to root to install packages
USER root

# Install Python 3 and pip
RUN apt-get update && apt-get install -y python3 python3-pip

# Install PySpark
RUN pip3 install pyspark delta-spark

# Optionally switch back to the default user if there is one
# USER default_user_name

# Copy your Python script into the container
COPY app /app

# Keep the container running
CMD ["/bin/bash", "-c", "while true; do sleep 1000; done"]
