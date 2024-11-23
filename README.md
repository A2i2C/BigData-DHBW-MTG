# Magic: The Gathering (MTG) ETL Pipeline

## Überblick
Diese ETL-Pipeline (Extract, Transform, Load) wurde entwickelt, um Magic: The Gathering-Daten zu extrahieren, zu transformieren und in eine Hadoop-Umgebung zu laden. Sie nutzt Apache Airflow, Hadoop, PostgreSQL und ein Frontend zur Visualisierung der Daten. Die Daten werden über die **MTG-API** abgerufen und in HDFS geladen, um sie später in einer Hive-Tabelle zu speichern und zu analysieren.

## Vorbedingungen
Stelle sicher, dass du Docker und Docker Compose auf deinem Rechner installiert hast. Wenn du es noch nicht getan hast, folge den Installationsanweisungen für Docker:
- [Docker Installation](https://docs.docker.com/get-docker/)
- [Docker Compose Installation](https://docs.docker.com/compose/install/)

## Docker Compose Setup

### Dienste in `docker-compose.yml`
- **Hadoop**: Führt HDFS, YARN, Hive und Spark aus. Enthält die Web-Oberflächen für HDFS und YARN und stellt Speicher für die ETL-Daten bereit.
- **Airflow**: Orchestriert die ETL-Pipeline. Führt täglich die Extraktion, Transformation und das Laden der MTG-Daten durch.
- **PostgreSQL**: Eine relationale Datenbank zur Speicherung von Metadaten und Konfigurationsinformationen.
- **Backend**: Ein Node.js-Backend, das API-Anfragen verarbeitet und mit der PostgreSQL-Datenbank kommuniziert.
- **Frontend**: Eine einfache Weboberfläche zur Anzeige von MTG-Daten.

## Docker Compose starten

Um alle Dienste zu starten, führe folgenden Befehl im Hauptverzeichnis des Projekts aus:

```bash
docker-compose up --build

Dieser Befehl baut die Docker-Images für alle Container und startet die Dienste.

- **Airflow Web UI** ist unter [http://localhost:8080](http://localhost:8080) zugänglich.
- **Hadoop Web UI** für YARN ist unter [http://localhost:8088](http://localhost:8088) und für HDFS unter [http://localhost:9870](http://localhost:9870) erreichbar.
- **Frontend Web UI** ist unter [http://localhost:3659](http://localhost:3659) erreichbar.

## ETL-Pipeline (Airflow DAG)
Die ETL-Pipeline wird in Airflow über einen DAG (Directed Acyclic Graph) verwaltet, der in `airflow/dags/` gespeichert ist. Die Pipeline besteht aus mehreren Aufgaben:

1. **Erstellen eines Verzeichnisses in HDFS**: Ein Verzeichnis in HDFS wird für die partitionierten MTG-Daten basierend auf dem aktuellen Datum erstellt.
2. **Abrufen von MTG-Daten**: MTG-Daten werden über eine Spark-Anwendung abgerufen und in HDFS gespeichert.
3. **Erstellen einer Hive-Tabelle**: Eine Hive-Tabelle wird für die MTG-Daten erstellt, um die Daten später analysieren zu können.
4. **Hinzufügen von Partitionen**: Neue Partitionen für die MTG-Daten werden zur Hive-Tabelle hinzugefügt.
5. **Datenbereinigung und Transformation**: Mit einer weiteren Spark-Anwendung werden die Daten bereinigt und transformiert.
6. **Export der finalen Daten**: Die bereinigten Daten werden in das finale Format exportiert und können weiterverarbeitet oder analysiert werden.

Der DAG wird **täglich** ausgeführt, basierend auf dem aktuellen Datum.

## Frontend
Das Frontend stellt eine einfache Web-Oberfläche zur Verfügung, um die MTG-Daten zu visualisieren. Es besteht aus den Dateien:

- **index.html**: Die Haupt-HTML-Datei für die Benutzeroberfläche.
- **styles.css**: Das Stylesheet für das Layout der Seite.
- **app.js**: Das JavaScript für die Interaktivität der Anwendung.

Im Dockerfile des Frontends verwenden wir Nginx, um die statischen Dateien bereitzustellen.

## Backend API
Das Backend stellt eine einfache API bereit, die eine GET-Anfrage verarbeitet, um die MTG-Daten aus der PostgreSQL-Datenbank abzurufen.

**Beispiel-API-Endpunkt:**

```bash
GET http://localhost:5201/cards

