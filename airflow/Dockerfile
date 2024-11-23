FROM marcelmittelstaedt/airflow:latest

USER airflow
WORKDIR /home/airflow

RUN pip3 install pymongo

# Switch back to Root User
USER root
WORKDIR /