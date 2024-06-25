## Requirements
- Set up a python environment (this project was built with python 3.11)
- Install the requirements.txt `pip install -r requirements.txt`
- Install the kaggle CLI tool globally. `pip install -g kaggle` and set up your token as [per their guide](https://www.kaggle.com/docs/api)
- Install java 11 and update the path. (Ex: `export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"`)
- Solr (docker in the DX project, or a local installation)

## Create your env file
- `cp .env.example .env`

## Install Pre-Commit
- After cloning for the first time, run `pre-commit install` in your environment

## Kaggle
For kaggle, as mentioned we need to set up the kaggle token.
Through docker, we copy it from the dx.backend root directory.
Make sure to download it set up your token as [from your account](https://www.kaggle.com/settings/account), and place it in the dx.backend root folder.

## HDX
For HDX, we need to have a HDX API Key, you can obtain one by signing in to https://data.humdata.org/user/<YOUR USER NAME>/api-tokens, and getting the API key.
Through docker, we copy it from the dx.backend root directory.

> Create a JSON or YAML file. The default path is .hdx_configuration.yaml in the current user's home directory. Then put in the YAML file:
>     `hdx_key: "HDX API KEY"`

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
