# Verwende Node.js als Basis-Image
FROM node:18-alpine

# Setze das Arbeitsverzeichnis
WORKDIR /usr/src/app

# Kopiere die package.json und package-lock.json
COPY package.json ./
COPY tsconfig.json ./

# Installiere die Abhängigkeiten
RUN npm install

# Kopiere den gesamten Quellcode in den Container
COPY ./src ./src

# Baue die TypeScript-Dateien
RUN npm run build 

# Exponiere den Port, den die App verwenden wird
EXPOSE 5201

# Startbefehl für die Anwendung
CMD ["npm", "start"]
