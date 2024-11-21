FROM marcelmittelstaedt/airflow:latest

USER airflow
WORKDIR /home/airflow

# Copy Airflow Config
COPY --chown=airflow airflow.cfg /home/airflow/airflow/airflow.cfg

# Switch back to Root User
USER root
WORKDIR /

COPY startup.sh /startup.sh
RUN chmod +x /startup.sh

# Expose Airflow Web Service Port
EXPOSE 8080

# Start startup Script
ENTRYPOINT ["/startup.sh"]
