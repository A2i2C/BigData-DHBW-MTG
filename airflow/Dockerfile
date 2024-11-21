FROM marcelmittelstaedt/airflow:latest

# Switch to root user
USER root

# Ensure permissions are set to allow airflow user to access mounted volumes
RUN chmod -R 777 /home/airflow/airflow/*

# Use bash as the default entry point
CMD ["airflow", "webserver", "--port", "8080"]

