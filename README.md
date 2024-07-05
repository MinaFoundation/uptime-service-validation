# Uptime Service Validation

[![CI](https://github.com/MinaFoundation/uptime-service-validation/actions/workflows/ci.yaml/badge.svg)](https://github.com/MinaFoundation/uptime-service-validation/actions/workflows/ci.yaml)

## Overview

This repository is home for Validator/Coordinator component for Mina Delegation Program.
This component is responsible for taking submissions data gathered by [uptime-service-backend](https://github.com/MinaFoundation/uptime-service-backend) and running validation against them using [stateless-verification-tool](https://github.com/MinaProtocol/mina/pull/14593). Next, based on these validation results, the Coordinator builds its own database containing uptime score.

**Important note:** If block producer submits at least one valid submission within the validation batch, it will be granted one point. There is a table `points_summary` that is instrumental for calculating scores. It is updated by a database trigger on every insert to the `points` table. While `points` table 
holds one point for each valid submission, the `points_summary` holds only one point if at least one submission happened in the batch. This is crucial for keeping correct score percentage.

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

- `SURVEY_INTERVAL_MINUTES` - Interval in minutes between processing data batches. Determines the end time (`cur_batch_end`) of the current batch by adding this interval to `prev_batch_end`. Default: `20`.
- `MINI_BATCH_NUMBER` - Number of mini-batches to process within each main batch. Used by `getTimeBatches` to divide the time between `prev_batch_end` and `cur_batch_end` into smaller intervals. Default: `5`.
- `UPTIME_DAYS_FOR_SCORE` - Number of days the system must be operational to calculate a score. Used by `updateScoreboard` to define the scoreboard update period. Default `90`.
- `RETRY_COUNT` - Number of times a batch should be retried before giving up. Default: `3`.
- `SUBMISSION_STORAGE` - Storage where submissions are kept. Valid options: `POSTGRES` or `CASSANDRA`. Default: `POSTGRES`.

### Stateless Verification Tool Configuration

The Coordinator program runs the `stateless-verification-tool` for validation against submissions. Set the following environment variables for this purpose:

- `WORKER_IMAGE` - Docker image name for the stateless verifier (e.g., `delegation-verify`).
- `WORKER_TAG` - Specific tag of the Docker image, indicating the version or build.
- `NO_CHECKS` - if set to `1`, stateless verifier will run with `--no-checks` flag
- `AWS_S3_BUCKET` - AWS S3 Bucket (needed for `stateless-verification-tool`)
- `NETWORK_NAME` - Network name (needed for `stateless-verification-tool`, in case block does not exist in `SUBMISSION_STORAGE` 
                   it attempts to download it from AWS S3 from `AWS_S3_BUCKET`\\`NETWORK_NAME`\blocks)

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

> **Optional**(Used with `invoke create-ro-user` task):
- `POSTGRES_RO_USER` - Desired username for creating read only postgres user.
- `POSTGRES_RO_PASSWORD` - Desired password for read only postgres user.

Create a database and relevant tables before first-time program execution using Python `invoke` tasks:

1. **Create database and tables**:

   ```sh
   invoke create-database
   ```

   > **Note:** The script creates `POSTGRES_DB` if not exists and applies `./database/create_tables.sql` script.

2. **Initialize `bot_logs` table**:

   The program requires an entry in `bot_logs`, especially the `batch_end_epoch` column, as a starting point. For first-time runs, create this entry as follows:

   ```sh
   # Initialize with current timestamp
   invoke init-database

   # Initialize with a specific unix timestamp
   invoke init-database --batch-end-epoch <timestamp>

   # Initialize with a specific date
   invoke init-database --batch-end-epoch "2024-06-05 00:00:00"

   # Initialize with a timestamp from n minutes ago
   invoke init-database --mins-ago 20
   ```

It is possible to drop database using:

```
invoke drop-database
```

A task to create a read only user that later can be used to connect to Delegation Program database with services such as `leaderboard`.

```
invoke create-ro-user
```
> **Note:** This task uses `POSTGRES_RO_USER` and `POSTGRES_RO_PASSWORD` env variables.

### AWS Keyspaces/Cassandra Configuration

To connect to AWS Keyspaces/Cassandra, the following environment variables need to be set:

**Mandatory/common env vars:**
- `AWS_KEYSPACE` - Your Keyspace name.
- `SSL_CERTFILE` - The path to your SSL certificate.
- `CASSANDRA_HOST` - Cassandra host (e.g. cassandra.us-west-2.amazonaws.com).
- `CASSANDRA_PORT` - Cassandra port (e.g. 9142).

**Depending on way of connecting:**

_Service level connection:_
- `CASSANDRA_USERNAME` - Cassandra service user.
- `CASSANDRA_PASSWORD` - Cassandra service password.

_AWS access key / web identity token:_
- `AWS_ROLE_SESSION_NAME` - AWS role session name.
- `AWS_ACCESS_KEY_ID` - Your AWS Access Key ID. No need to set if `AWS_ROLE_SESSION_NAME` is set.
- `AWS_SECRET_ACCESS_KEY` - Your AWS Secret Access Key. No need to set if `AWS_ROLE_SESSION_NAME` is set.
- `AWS_DEFAULT_REGION` - Your AWS Default region. (e.g. us-west-2, it is needed for `stateless-verification-tool`)

> 🗒️ **Note 1:** For convenience, an SSL certificate is provided in this repository and can be found at [/uptime_service_validation/database/aws_keyspaces/cert/sf-class2-root.crt](/uptime_service_validation/database/aws_keyspaces/cert/sf-class2-root.crt). Alternatively, the certificate can also be downloaded directly from AWS. Detailed instructions for obtaining the certificate are available in the AWS Keyspaces documentation, which you can access [here](https://docs.aws.amazon.com/keyspaces/latest/devguide/using_python_driver.html#using_python_driver.BeforeYouBegin).

> 🗒️ **Note 2:** Docker image of this program already includes cert and has `SSL_CERTFILE` set up, however it can be overriden by providing this env variable to docker.

### Application Status Update

The coordinator is responsible for updating the application statuses based on responses from the participants' registration forms. This process involves retrieving relevant information from a designated Google Spreadsheet and updating the statuses in the database at the start of each validation batch.

The operation can be configured using the following environment variables:

- `IGNORE_APPLICATION_STATUS` - Setting this to `1` instructs the coordinator to bypass the application status update process. This is primarily intended for use in testing environments.
- `SPREADSHEET_NAME` - Specifies the name of the Google Spreadsheet document containing the registration form responses.
- `SPREADSHEET_CREDENTIALS_JSON` - The path to the JSON file with the Google Service Account credentials, which are necessary for accessing the spreadsheet.

If the system encounters any issues while updating statuses, it will log the error and proceed with the validation batch without interrupting the process. It's important to note that the application status plays a crucial role in the Leaderboard UI: only block-producers with `application_status = true` are eligible to appear on the Leaderboard. This ensures that only registered and validated participants are displayed.

### Test Configuration

By default, the program runs `stateless-verification-tool` in separate Kubernetes pods. For testing purposes, it can be configured to run them as subprocesses on the same machine. Set the optional environment variable `TEST_ENV=1` for this mode.

## Running the program

Once everything is configured we can start the program by running:

```sh
poetry run start
```

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
           -e MINI_BATCH_NUMBER \
           -e UPTIME_DAYS_FOR_SCORE \
           -e WORKER_IMAGE \
           -e WORKER_TAG \
           -e POSTGRES_HOST \
           -e POSTGRES_PORT \
           -e POSTGRES_DB \
           -e POSTGRES_USER \
           -e POSTGRES_PASSWORD \
           -e AWS_KEYSPACE \
           -e CASSANDRA_HOST \
           -e CASSANDRA_PORT \
           -e AWS_ACCESS_KEY_ID \
           -e AWS_SECRET_ACCESS_KEY \
           -e AWS_DEFAULT_REGION \
           -e SSL_CERTFILE \
           uptime-service-validation
```

## Maintenance

### Database Cleanup

The system's database can grow over time, but typically, keeping more than 90 days of data isn't necessary. However, you might choose to retain data for a longer period. The schema includes a function to clean up data older than a specified number of days:

```sql
-- Example call to the function to clean up data older than 180 days
SELECT cleanup_old_data(180);
```

**Note:** It is advisable to perform a database backup before initiating the cleanup process.
