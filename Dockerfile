# Start with python 3.11
FROM python:3.11-bullseye
COPY . /app

WORKDIR /app

# Install java 11 and set JAVA_HOME and JRE_HOME to be able to use solr post tool
RUN apt-get update && apt-get install -y openjdk-11-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Pre-install python dependencies
RUN pip install -r requirements.txt

EXPOSE 4004

# Run the app
CMD ["gunicorn", "-w", "8", "app:app", "-b", "0.0.0.0:4004"]
