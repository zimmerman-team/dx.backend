# The dx.backend image. CI builds it on every push and tags it with the commit
# SHA; infra.dx runs it. Build it locally with `make build`.
# Debian 12 (bookworm): bullseye is end-of-life and its packages no longer install.
FROM python:3.11-bookworm

RUN apt-get update \
 && apt-get install -y freetds-dev build-essential python3-dev pkg-config gcc \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencies first, so code changes don't reinstall them.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# The Kaggle and HDX keys aren't baked into the image: infra.dx mounts them at
# /root/.kaggle/kaggle.json and /root/.hdx_configuration.yaml.
EXPOSE 4004
CMD ["gunicorn", "-w", "8", "app:app", "-b", "0.0.0.0:4004", "--timeout", "600"]
