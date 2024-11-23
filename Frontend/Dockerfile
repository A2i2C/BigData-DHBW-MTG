# Verwende ein einfaches Python-Image
FROM python:3.9-slim

# Setze das Arbeitsverzeichnis im Container
WORKDIR /app

# Kopiere die statischen Dateien (index.html, styles.css, app.js) in das Container-Arbeitsverzeichnis
COPY ./inhalt /app

# Exponiere Port 8000 für den HTTP-Server
EXPOSE 8000

# Starte den einfachen HTTP-Server auf Port 8000
CMD ["python", "-m", "http.server", "8000"]
