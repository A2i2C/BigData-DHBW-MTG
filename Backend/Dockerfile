# Use a Node.js base image
FROM node:18-alpine

WORKDIR /usr/src/app
COPY package.json ./
RUN npm install
COPY ./source ./src
EXPOSE 3000

# Start the application
CMD ["npm", "start"]