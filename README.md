# Uptime Service Validation

[![CI](https://github.com/MinaFoundation/uptime-service-validation/actions/workflows/ci.yaml/badge.svg)](https://github.com/MinaFoundation/uptime-service-validation/actions/workflows/ci.yaml)

## Overview

This repository is home for Validator/Coordinator component for Mina Delegation Program. 
This component is responsible for taking submissions data gathered by [uptime-service-backend](https://github.com/MinaFoundation/uptime-service-backend) and running validation against them using [stateles-verification-tool](https://github.com/MinaProtocol/mina/pull/14593). Next, based on these validation results, the Coordinator builds its own database containing uptime score.


## Getting Started

### Prerequisites

- Python >= 3.10
- [Poetry](https://python-poetry.org/docs/), a tool for dependency management and packaging in Python.

### Setting Up Your Development Environment

1. **Install dependencies:**

```sh
git clone https://github.com/MinaFoundation/uptime-service-validation.git
cd uptime-service-validation

poetry install
```

2. **Activate the Virtual Environment:**

After installing the project dependencies with `poetry install`, you can activate the virtual environment by running:

```sh
poetry shell
```

This will spawn a new shell (subshell) with the virtual environment activated. It ensures that all commands and scripts are run within the context of your project's dependencies.

### Testing

```sh
poetry run pytest -v
```

## Configuration

The program requires setting several environment variables for operation and setting up a Postgres database. Optionally, these variables can be defined in a `.env` file, which the program will load at runtime. Below, we explain the environment variables in more detail.

### Runtime Configuration

These environment variables control the program's runtime:

- `SURVEY_INTERVAL_MINUTES` - Interval in minutes between processing data batches. Determines the end time (`cur_batch_end`) of the current batch by adding this interval to `prev_batch_end`.
- `MINI_BATCH_NUMBER` - Number of mini-batches to process within each main batch. Used by `getTimeBatches` to divide the time between `prev_batch_end` and `cur_batch_end` into smaller intervals.
- `UPTIME_DAYS_FOR_SCORE` - Number of days the system must be operational to calculate a score. Used by `updateScoreboard` to define the scoreboard update period.

### Stateless Verification Tool Configuration

The Coordinator program runs the `stateless-verification-tool` for validation against submissions. Set the following environment variables for this purpose:

- `WORKER_IMAGE` - Docker image name for the stateless verifier (e.g., `mina-delegation-verify`).
- `WORKER_TAG` - Specific tag of the Docker image, indicating the version or build.

### Slack Alerts

The following environment variables are involved in sending Slack Alerts using Webhooks whenever the ZkValidator process is too quick or slow:

- `WEBHOOK_URL`
- `ALARM_ZK_LOWER_LIMIT_SEC`
- `ALARM_ZK_UPPER_LIMIT_SEC`

### Postgres Configuration

Set these environment variables for the Postgres connection:

- `POSTGRES_HOST` - Hostname or IP of the PostgreSQL server (e.g., `localhost`).
- `POSTGRES_PORT` - Port for PostgreSQL (default is `5432`).
- `POSTGRES_DB` - Specific PostgreSQL database name (e.g., `coordinator`).
- `POSTGRES_USER` - Username for PostgreSQL authentication.
- `POSTGRES_PASSWORD` - Password for the specified PostgreSQL user.

Create a database and relevant tables before first-time program execution using Python `invoke` tasks:

1. **Create database and tables**:

   ```bash
   invoke create-database
   ```

   > :warning: This script drops all existing tables and creates new ones.

2. **Initialize `bot_logs` table**:

   The program requires an entry in `bot_logs`, especially the `batch_end_epoch` column, as a starting point. For first-time runs, create this entry as follows:

   ```bash
   # Initialize with current timestamp
   invoke init-database

   # Initialize with a specific timestamp
   invoke init-database --batch-end-epoch <timestamp>

   # Initialize with a timestamp from n minutes ago
   invoke init-database --mins-ago 20
   ```

### AWS Keyspaces Configuration

To connect to AWS Keyspaces, the following environment variables need to be set:

- `AWS_KEYSPACE` - Your AWS Keyspace name.
- `CASSANDRA_HOST` - Cassandra host (e.g. cassandra.us-west-2.amazonaws.com).
- `CASSANDRA_PORT` - Cassandra port (e.g. 9142).
- `CASSANDRA_USERNAME` - Cassandra service user.
- `CASSANDRA_PASSWORD` - Cassandra service password.
- `SSL_CERTFILE` - The path to your SSL certificate for AWS Keyspaces.

> 🗒️ **Note 1:** For convenience, an SSL certificate is provided in this repository and can be found at [/uptime_service_validation/database/aws_keyspaces/cert/sf-class2-root.crt](/uptime_service_validation/database/aws_keyspaces/cert/sf-class2-root.crt). Alternatively, the certificate can also be downloaded directly from AWS. Detailed instructions for obtaining the certificate are available in the AWS Keyspaces documentation, which you can access [here](https://docs.aws.amazon.com/keyspaces/latest/devguide/using_python_driver.html#using_python_driver.BeforeYouBegin).

> 🗒️ **Note 2:** Docker image already includes cert and has `AWS_SSL_CERTIFICATE_PATH` set up, however it can be overriden by providing this env variable to docker.

### Test Configuration

By default, the program runs `stateless-verification-tool` in separate Kubernetes pods. For testing purposes, it can be configured to run them as subprocesses on the same machine. Set the optional environment variable `TEST_ENV=1` for this mode.

## Docker

Program is also shipped as a docker image.

### Building Docker

```sh
docker build -t uptime-service-validation .
```

### Running Docker

When running pass all relevant env variables to the docker (see `.env`), e.g.:

```sh
docker run -e SURVEY_INTERVAL_MINUTES \
           -e POSTGRES_HOST \
           -e POSTGRES_PORT \
           -e POSTGRES_DB \
           -e POSTGRES_USER \
           -e POSTGRES_PASSWORD \
           -e MINI_BATCH_NUMBER \
           -e UPTIME_DAYS_FOR_SCORE \
           -e WORKER_TAG \
           -e WORKER_IMAGE \
           -e AWS_KEYSPACE \
           -e CASSANDRA_HOST \
           -e CASSANDRA_PORT \
           -e CASSANDRA_USERNAME \
           -e CASSANDRA_PASSWORD \
           -e SSL_CERTFILE \
           uptime-service-validation
```
