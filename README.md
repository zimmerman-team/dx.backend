# DX Backend

## Running it

This repo has the same `make` commands as the other DX repos, which [infra.dx](https://github.com/zimmerman-team/DX) clones side by side (`make repos` there).

| Command | What it does |
|---|---|
| `make setup` | Create a Python 3.11 virtualenv in `.venv` with the requirements (uses [uv](https://docs.astral.sh/uv/) if you have it), create `.env` from `.env.example` |
| `make dev` | Run the backend on http://localhost:4004, reloading on changes |
| `make build` | Build the Docker image, as CI does |
| `make logs` | Follow `logging/backend.log` |
| `make lint` | Run flake8 |

`make dev` needs MongoDB on this machine: `make native` in infra.dx starts it along with the rest of the stack. To run everything in Docker instead, use `make dev` in infra.dx.

Production runs the Docker image: CI builds it on every push (`.github/workflows/image.yaml`) and infra.dx deploys it. The Kaggle and HDX keys are no longer copied into the image; infra.dx mounts them.

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
Download it [from your account](https://www.kaggle.com/settings/account). Locally it goes in `~/.kaggle/kaggle.json`; in Docker, infra.dx mounts it from its secrets folder.

## HDX

For HDX, we need to have a HDX API Key, you can obtain one by signing in to https://data.humdata.org/user/<YOUR USER NAME>/api-tokens, and getting the API key.
In Docker, infra.dx mounts it from its secrets folder.

> Create a JSON or YAML file. The default path is .hdx_configuration.yaml in the current user's home directory. Then put in the YAML file:
>`hdx_key: "HDX API KEY"`

## Data.World
We use `datadotworld`. For this, we retrieve the API key at https://data.world/settings/advanced.
Use the following export to locally enable the data.world integration.
```
export DW_AUTH_TOKEN=<YOUR_TOKEN>
```

Use the read/write token.

## Running

### Local

```bash
flask run --port 4004
```

Stop it with `ctrl + c`

### Server

```bash
gunicorn -w 8 app:app -b 0.0.0.0:4004 --daemon --access-logfile ./logging/access.txt --error-logfile ./logging/error.txt --timeout 600
```

We set 8 workers, port 4004, run it in "daemon" mode to run in background, a timeout of 10 minutes, and logfiles, which are optional. We have internal logging, and access is logged through nginx

Stop it with `pkill gunicorns`

## Development

### Commits

[Commitlint](https://github.com/conventional-changelog/commitlint#what-is-commitlint) is used to check your commit messages.

When setting up the repository, after locally setting up an environment, ensure pre-commit is installed:

- Add pre-commit: `uv add pre-commit` or `pip install pre-commit`.
- Install pre-commit to git: `uv run pre-commit install` or `pre-commit install`.
- Install the commit hook to git: `uv run pre-commit install --hook-type commit-msg` or `pre-commit install --hook-type commit-msg`.

### Code Management

*flake8* is used to maintain code quality in pep8 style

*isort* is used to maintain the imports

*pre-commit* is used to enforce commit styles in the form:

```bash
feat: A new feature
fix: A bug fix
docs: Documentation only changes
style: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
refactor: A code change that neither fixes a bug nor adds a feature
perf: A code change that improves performance
test: Adding missing or correcting existing tests
chore: Changes to the build process or auxiliary tools and libraries such as documentation generation
```
