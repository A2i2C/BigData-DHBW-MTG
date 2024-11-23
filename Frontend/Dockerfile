# Basis-Image
FROM nginx:latest

# Arbeitsverzeichnis im Container erstellen
WORKDIR /usr/share/nginx/html

# Statische Dateien in den Container kopieren
COPY index.html styles.css app.js ./

# Standardport freigeben
EXPOSE 80