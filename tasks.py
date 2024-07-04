from datetime import datetime, timedelta, timezone
import re
from invoke import task
import os
import psycopg2
from psycopg2 import sql


@task
def create_database(ctx):
    db_host = os.environ.get("POSTGRES_HOST")
    db_port = os.environ.get("POSTGRES_PORT")
    db_name = os.environ.get("POSTGRES_DB")
    db_user = os.environ.get("POSTGRES_USER")
    db_password = os.environ.get("POSTGRES_PASSWORD")

    # Establishing connection to PostgreSQL server
    # (connect to initial database 'postgres' to create a new database)
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname="postgres",
        user=db_user,
        password=db_password,
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Creating the database
    try:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        print(f"Database '{db_name}' created successfully")
    except psycopg2.errors.DuplicateDatabase:
        print(f"Database '{db_name}' already exists, not creating")

    cursor.close()
    conn.close()

    # Connect to the new database
    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Path to the SQL script relative to tasks.py
    sql_script_path = "uptime_service_validation/database/create_tables.sql"

    # Running the SQL script file
    with open(sql_script_path, "r") as file:
        sql_script = file.read()
        cursor.execute(sql_script)
        print("'create_tables.sql' script completed successfully")

    cursor.close()
    conn.close()


@task
def init_database(ctx, batch_end_epoch=None, mins_ago=None, override_empty=False):
    db_host = os.environ.get("POSTGRES_HOST")
    db_port = os.environ.get("POSTGRES_PORT")
    db_name = os.environ.get("POSTGRES_DB")
    db_user = os.environ.get("POSTGRES_USER")
    db_password = os.environ.get("POSTGRES_PASSWORD")

    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password
    )
    cursor = conn.cursor()

    if mins_ago is not None:
        batch_end_epoch = (
            datetime.now(timezone.utc) - timedelta(minutes=int(mins_ago))
        ).timestamp()
    elif batch_end_epoch is None:
        batch_end_epoch = datetime.now(timezone.utc).timestamp()
    else:
        datetime_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}.\d{2}:\d{2}:\d{2}.*$")
        # Convert batch_end_epoch to a timestamp in utc if it is a datetime string
        # use regex to check if it is of format 'YYYY-MM-DD HH:MM:SS'
        if datetime_pattern.match(batch_end_epoch):
            batch_end_epoch = datetime.fromisoformat(batch_end_epoch).timestamp()
            print(f"Converted datetime string to timestamp: {batch_end_epoch}")
        else:
            batch_end_epoch = int(batch_end_epoch)
            print(f"Using provided timestamp: {batch_end_epoch}")

    # Check if the table is empty, if override_empty is False
    should_insert = True
    if not override_empty:
        cursor.execute("SELECT COUNT(*) FROM bot_logs")
        count = cursor.fetchone()[0]
        should_insert = count == 0

    if should_insert:
        processing_time = 0
        files_processed = -1  # -1 indicates that this is initialization
        file_timestamps = datetime.fromtimestamp(batch_end_epoch, timezone.utc)
        batch_start_epoch = batch_end_epoch

        # Inserting data into the bot_logs table
        cursor.execute(
            "INSERT INTO bot_logs (processing_time, files_processed, file_timestamps, batch_start_epoch, batch_end_epoch) \
            VALUES (%s, %s, %s, %s, %s)",
            (
                processing_time,
                files_processed,
                file_timestamps,
                batch_start_epoch,
                batch_end_epoch,
            ),
        )
        print(f"Row inserted into bot_logs table. batch_end_epoch: {batch_end_epoch}.")
    else:
        print(
            "Table bot_logs is not empty. Row not inserted. You can override this by passing --override-empty."
        )

    conn.commit()
    cursor.close()
    conn.close()

@task
def create_ro_user(ctx):
    db_host = os.environ.get("POSTGRES_HOST")
    db_port = os.environ.get("POSTGRES_PORT")
    db_name = os.environ.get("POSTGRES_DB")
    db_user = os.environ.get("POSTGRES_USER")
    db_password = os.environ.get("POSTGRES_PASSWORD")
    db_ro_user = os.environ.get("POSTGRES_RO_USER")
    db_ro_password = os.environ.get("POSTGRES_RO_PASSWORD")

    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password
    )
    cursor = conn.cursor()

    # Check if the user exists
    user_exists = False
    cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s;", (db_ro_user,))
    user_exists = cursor.fetchone() is not None

    if not user_exists:
        cursor.execute(sql.SQL("CREATE USER {} WITH PASSWORD %s;").format(sql.Identifier(db_ro_user)), (db_ro_password,))
        cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {};").format(sql.Identifier(db_name),sql.Identifier(db_ro_user)))
        cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {};").format(sql.Identifier(db_ro_user)))
        cursor.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {};").format(sql.Identifier(db_ro_user)))
        cursor.execute(sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {};").format(sql.Identifier(db_ro_user)))
        print(f"User {db_ro_user} created")
    else:
        print(f"User {db_ro_user} already exists")

    conn.commit()
    cursor.close()
    conn.close()

@task
def drop_database(ctx):
    db_host = os.environ.get("POSTGRES_HOST")
    db_port = os.environ.get("POSTGRES_PORT")
    db_name = os.environ.get("POSTGRES_DB")
    db_user = os.environ.get("POSTGRES_USER")
    db_password = os.environ.get("POSTGRES_PASSWORD")

    # Establishing connection to PostgreSQL server
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname="postgres",
        user=db_user,
        password=db_password,
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Dropping the database
    try:
        cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(db_name)))
        print(f"Database '{db_name}' dropped!")
    except Exception as e:
        print(f"Error dropping database '{db_name}'! Error: {e}")

    cursor.close()
    conn.close()
