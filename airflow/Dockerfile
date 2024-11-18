FROM marcelmittelstaedt/airflow:latest

# Switch to airflow user
USER airflow
WORKDIR /home/airflow

# Install pymongo
RUN pip3 install pymongo

# Switch back to root user
USER root
WORKDIR /