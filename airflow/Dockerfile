FROM marcelmittelstaedt/airflow:latest

USER root
WORKDIR /

COPY startup.sh /startup.sh
RUN chmod +x /startup.sh

EXPOSE 8080

ENTRYPOINT ["/startup.sh"]
