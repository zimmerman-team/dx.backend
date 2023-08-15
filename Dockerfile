# Start with python 3.11
FROM python:3.11-bullseye
COPY . /app

# Set up the kaggle api token
COPY ./kaggle.json /root/.kaggle/kaggle.json
RUN chmod 600 /root/.kaggle/kaggle.json

WORKDIR /app

# Pre-install python dependencies
RUN pip install -r requirements.txt

EXPOSE 4004

# Run the app
CMD ["gunicorn", "-w", "8", "app:app", "-b", "0.0.0.0:4004", "--timeout", "600"]
