## Requirements
- Set up a python environment (this project was built with python 3.11)
- Install the requirements.txt `pip install -r requirements.txt`
- Install the kaggle CLI tool globally. `pip install -g kaggle` and set up your token as [per their guide](https://www.kaggle.com/docs/api)
- Install java 11 and update the path. (Ex: `export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"`)
- Solr (docker in the DX project, or a local installation)

## Create your env file
- `cp .env.example .env`

## Running
Local
```
flask run --port 4004
```
Stop it with `ctrl + c`


Server
```
gunicorn -w 8 app:app -b 0.0.0.0:4004 --daemon --access-logfile ./logging/access.txt --error-logfile ./logging/error.txt --timeout 600
```
We set 8 workers, port 4004, run it in "daemon" mode to run in background, a timeout of 10 minutes, and logfiles, which are optional. We have internal logging, and access is logged through nginx

Stop it with `pkill gunicorns`
