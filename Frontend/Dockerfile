# Use a Node.js base image
FROM node:18-alpine

# Set the working directory
WORKDIR /usr/src/app

# Copy package.json and package-lock.json
COPY MTG-Cards-Frontend/package.json MTG-Cards-Frontend/package-lock.json ./

# Install dependencies, including Angular CLI
RUN npm install -g @angular/cli
RUN npm install

# Copy the Angular application source code
COPY MTG-Cards-Frontend/ ./

# Expose the port the Angular app runs on
EXPOSE 4200

# Default command to serve the Angular app
CMD ["ng", "serve", "--host", "0.0.0.0"]