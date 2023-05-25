
FROM node:16.19.1-alpine3.17

# For development add /bin/bash
RUN apk update && apk add bash

# Add java jdk for solr post tool
RUN apk update && apk add --no-cache openjdk11
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk

# Set up the server directory
WORKDIR /app
COPY . /app

# Install yarn dependencies
RUN yarn global add pm2
RUN yarn install
RUN yarn build

# APP LOG DIRECTORY FROM pm2.config.js
RUN mkdir -p /home/zim/app-logs/dx-backend

# Run `yarn docker` to build, migrate and run the server with pm2.
CMD ["yarn", "docker"]
