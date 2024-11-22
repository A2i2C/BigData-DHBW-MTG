FROM marcelmittelstaedt/airflow:latest

USER airflow
WORKDIR /home/airflow

COPY --chown=airflow hdfs-site.xml hadoop/etc/hadoop/hdfs-site.xml
COPY --chown=airflow core-site.xml hadoop/etc/hadoop/core-site.xml


USER root

COPY startup.sh /startup.sh
RUN chmod +x /startup.sh

EXPOSE 8080

ENTRYPOINT ["/startup.sh"]
