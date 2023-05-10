# Check out https://hub.docker.com/_/node to select a new base image
FROM node:16-slim

# Set to a non-root built-in user `node`
USER node
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-11-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Create app directory (with user `node`)
RUN mkdir -p /app

WORKDIR /app

# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
# where available (npm@5+)
COPY --chown=node package*.json ./

RUN npm install

# Bundle app source code
COPY --chown=node . .

RUN npm run build

# Bind to all network interfaces so that it can be mapped to the host OS
ENV HOST=0.0.0.0 PORT=4004

EXPOSE ${PORT}
CMD [ "node", "." ]
