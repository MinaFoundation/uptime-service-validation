"""The coordinator script of the uptime service. Its job is to manage
the validator processes, distribute work them and, when they're done,
collect their results, compute scores for the delegation program and
put the results in the database."""

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import logging
import os
import sys
from time import sleep

from dotenv import load_dotenv
import pandas as pd
import psycopg2
from uptime_service_validation.coordinator.config import Config
from uptime_service_validation.coordinator.helper import (
    DB,
    Timer,
    get_relations,
    find_new_values_to_insert,
    filter_state_hash_percentage,
    create_graph,
    apply_weights,
    bfs,
    send_slack_message,
    get_contact_details_from_spreadsheet,
)
from uptime_service_validation.coordinator.server import (
    setUpValidatorPods,
    setUpValidatorProcesses,
)
from uptime_service_validation.coordinator.aws_keyspaces_client import (
    AWSKeyspacesClient,
)

# Add project root to python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, project_root)


class State:
    """The state aggregates all the data that remains constant while processing
    a single batch, but changes between batches. It also takes care of valid
    state transitions. It should likely be split into smaller chunks, but at
    this stage it's sufficient."""

    def __init__(self, batch):
        self.batch = batch
        self.current_timestamp = datetime.now(timezone.utc)
        self.retrials_left = Config.RETRY_COUNT
        self.interval = Config.SURVEY_INTERVAL_MINUTES
        self.loop_count = 0
        self.stop = False

    def wait_until_batch_ends(self):
        "If the time window if the current batch is not yet over, sleep until it is."
        if self.batch.end_time > self.current_timestamp:
            delta = timedelta(minutes=2)
            sleep_interval = (self.batch.end_time - self.current_timestamp) + delta
            time_until = self.current_timestamp + sleep_interval
            logging.info(
                "All submissions are processed till date. "
                "Will wait %s (until %s) before starting next batch...",
                sleep_interval,
                time_until,
            )
            sleep(sleep_interval.total_seconds())
            self.__update_timestamp()

    def advance_to_next_batch(self, next_bot_log_id):
        """Update the state so that it describes the next batch in line;
        transitioning the state to the next loop pass."""
        self.retrials_left = Config.RETRY_COUNT
        self.batch = self.batch.next(next_bot_log_id)
        self.__warn_if_work_took_longer_then_expected()
        self.__next_loop()
        self.__update_timestamp()

    def retry_batch(self):
        logging.error(
            "Error in processing, retrying the batch... Retrials left: %s out of %s.",
            self.retrials_left,
            Config.RETRY_COUNT,
        )
        if self.retrials_left > 0:
            self.retrials_left -= 1
        else:
            logging.error("Error in processing, retry count exceeded... Exitting!")
            self.stop = True
        self.__warn_if_work_took_longer_then_expected()
        self.__next_loop()
        self.__update_timestamp()

    def __update_timestamp(self):
        self.current_timestamp = datetime.now(timezone.utc)

    def __next_loop(self):
        self.loop_count += 1
        logging.info("Processed it loop count : %s.", self.loop_count)

    def __warn_if_work_took_longer_then_expected(self):
        if self.batch.start_time >= self.current_timestamp:
            logging.warning(
                "It seems that batch processing took a bit too long than \
                expected as prev_batch_end: %s >= cur_timestamp: %s... \
                progressing to the next batch anyway...",
                self.batch.start_time,
                self.current_timestamp,
            )


def load_submissions(time_intervals, db, submission_storage=Config.SUBMISSION_STORAGE):
    """
    Load submissions from Config.SUBMISSION_STORAGE:
     - return validated subs as a DataFrame for further processing.
     - return all subs for storing in the submissions_by_submitter table.
    """
    submissions = []
    submissions_verified = []

    if submission_storage == Config.STORAGE_CASSANDRA:
        cassandra = AWSKeyspacesClient()
        try:
            cassandra.connect()
            for time_interval in time_intervals:
                submissions.extend(
                    cassandra.get_submissions(
                        submitted_at_start=time_interval[0],
                        submitted_at_end=time_interval[1],
                        start_inclusive=True,
                        end_inclusive=False,
                    )
                )
        except Exception as e:
            logging.error("Error in loading submissions: %s", e)
            return [pd.DataFrame([]), submissions]
        finally:
            cassandra.close()
    elif submission_storage == Config.STORAGE_POSTGRES:
        start_date = time_intervals[0][0]
        end_date = time_intervals[-1][1]
        try:
            submissions_result = db.get_submissions(start_date, end_date)
            if submissions_result is None:
                logging.error("Failed to load submissions from database.")
                return [pd.DataFrame([]), submissions]
            submissions.extend(submissions_result)
        except Exception as e:
            logging.error("Error in loading submissions: %s", e)
            return [pd.DataFrame([]), submissions]
    else:
        raise ValueError(f"Invalid submission storage: {submission_storage}")

    # for further processing
    # we use only submissions verified = True and validation_error = None or ""
    for submission in submissions:
        if submission.verified and (
            submission.validation_error is None or submission.validation_error == ""
        ):
            submissions_verified.append(submission)

    all_submissions_count = len(submissions)
    submissions_to_process_count = len(submissions_verified)
    logging.info("number of all submissions: %s", all_submissions_count)
    logging.info("number of submissions to process: %s", submissions_to_process_count)
    if submissions_to_process_count < all_submissions_count:
        logging.warning(
            "some submissions were not processed, because they were not verified or had validation errors"
        )
    return [
        pd.DataFrame([asdict(submission) for submission in submissions_verified]),
        submissions,
    ]


def process_statehash_df(db, batch, state_hash_df, verification_time):
    """Process the state hash dataframe and return the master dataframe."""
    all_files_count = state_hash_df.shape[0]
    master_df = pd.DataFrame()
    master_df["state_hash"] = state_hash_df["state_hash"]
    master_df["blockchain_height"] = state_hash_df["height"]
    master_df["slot"] = pd.to_numeric(state_hash_df["slot"])
    master_df["parent_state_hash"] = state_hash_df["parent"]
    master_df["submitter"] = state_hash_df["submitter"]
    master_df["file_updated"] = state_hash_df["submitted_at"]
    master_df["file_name"] = (
        state_hash_df["submitted_at"].astype(str)
        + "-"
        + state_hash_df["submitter"].astype(str)
    )  # Perhaps this should be changed? Filename makes less sense now.
    master_df["blockchain_epoch"] = state_hash_df["created_at"].apply(
        lambda row: int(row.timestamp() * 1000)
    )

    state_hash = pd.unique(
        master_df[["state_hash", "parent_state_hash"]].values.ravel("k")
    )
    existing_state_df = db.get_statehash_df()
    existing_nodes = db.get_existing_nodes()
    logging.info("number of nodes in the previous batch: %s", len(existing_nodes))
    state_hash_to_insert = find_new_values_to_insert(
        existing_state_df, pd.DataFrame(state_hash, columns=["statehash"])
    )
    logging.info("number of statehashes to insert: %s", len(state_hash_to_insert))
    if not state_hash_to_insert.empty:
        db.create_statehash(state_hash_to_insert)

    nodes_in_cur_batch = pd.DataFrame(
        master_df["submitter"].unique(), columns=["block_producer_key"]
    )
    logging.info("number of nodes in the current batch: %s", len(nodes_in_cur_batch))

    node_to_insert = find_new_values_to_insert(existing_nodes, nodes_in_cur_batch)
    logging.info("number of nodes to insert: %s", len(node_to_insert))

    if not node_to_insert.empty:
        node_to_insert["updated_at"] = datetime.now(timezone.utc)
        db.create_node_record(node_to_insert, 100)

    master_df.rename(
        inplace=True,
        columns={
            "file_updated": "file_timestamps",
            "submitter": "block_producer_key",
        },
    )

    relation_df, p_selected_node_df = db.get_previous_statehash(batch.bot_log_id)

    p_map = list(get_relations(relation_df))
    c_selected_node = filter_state_hash_percentage(master_df)

    logging.info("creating graph for the current batch...")
    batch_graph = create_graph(master_df, p_selected_node_df, c_selected_node, p_map)
    logging.info("graph created successfully.")

    logging.info("applying weights to the graph...")
    weighted_graph = apply_weights(
        batch_graph=batch_graph,
        c_selected_node=c_selected_node,
        p_selected_node=p_selected_node_df,
    )
    logging.info("weights applied successfully.")

    queue_list = list(p_selected_node_df["state_hash"].values) + c_selected_node
    batch_state_hash = list(master_df["state_hash"].unique())

    logging.info("running BFS on the graph...")
    shortlisted_state_hash_df = bfs(
        graph=weighted_graph,
        queue_list=queue_list,
        node=queue_list[0],
        # batch_statehash=batch_state_hash, (this used to be here in old code,
        # but it's not used anywhere inside the function)
    )
    logging.info("BFS completed successfully.")
    point_record_df = master_df[
        master_df["state_hash"].isin(shortlisted_state_hash_df["state_hash"].values)
    ]

    for index, row in shortlisted_state_hash_df.iterrows():
        if not row["state_hash"] in batch_state_hash:
            shortlisted_state_hash_df.drop(index, inplace=True, axis=0)
    p_selected_node_df = shortlisted_state_hash_df.copy()
    parent_hash = []
    for s in shortlisted_state_hash_df["state_hash"].values:
        p_hash = master_df[master_df["state_hash"] == s]["parent_state_hash"].values[0]
        parent_hash.append(p_hash)
    shortlisted_state_hash_df["parent_state_hash"] = parent_hash

    p_map = list(
        get_relations(shortlisted_state_hash_df[["parent_state_hash", "state_hash"]])
    )
    if not point_record_df.empty:
        file_timestamp = master_df.iloc[-1]["file_timestamps"]
    else:
        file_timestamp = batch.end_time
        logging.info(
            "empty point record for start epoch %s end epoch %s",
            batch.start_time.timestamp(),
            batch.end_time.timestamp(),
        )

    values = (
        all_files_count,
        file_timestamp,
        batch.start_time.timestamp(),
        batch.end_time.timestamp(),
        verification_time.total_seconds(),
    )
    bot_log_id = db.create_bot_log(values)

    shortlisted_state_hash_df["bot_log_id"] = bot_log_id
    db.insert_statehash_results(shortlisted_state_hash_df)

    if not point_record_df.empty:
        point_record_df = point_record_df.copy()
        point_record_df.loc[:, "amount"] = 1
        point_record_df.loc[:, "created_at"] = datetime.now(timezone.utc)
        point_record_df.loc[:, "bot_log_id"] = bot_log_id
        point_record_df = point_record_df[
            [
                "file_name",
                "file_timestamps",
                "blockchain_epoch",
                "block_producer_key",
                "blockchain_height",
                "amount",
                "created_at",
                "bot_log_id",
                "state_hash",
            ]
        ]
        db.create_point_record(point_record_df)
    return bot_log_id


def process(db, state):
    """Perform a signle iteration of the coordinator loop, processing exactly
    one batch of submissions. Launch verifiers to process submissions, then
    compute scores and store them in the database."""
    logging.info(
        "iteration start at: %s, cur_timestamp: %s",
        state.batch.start_time,
        state.current_timestamp,
    )
    logging.info(
        "running for batch: %s - %s.", state.batch.start_time, state.batch.end_time
    )

    # sleep until batch ends, update the state accordingly, then continue.
    state.wait_until_batch_ends()
    time_intervals = list(state.batch.split(Config.MINI_BATCH_NUMBER))

    timer = Timer()
    if Config.is_test_environment():
        logging.info("running in test environment")
        with timer.measure():
            setUpValidatorProcesses(
                time_intervals, logging, Config.WORKER_IMAGE, Config.WORKER_TAG
            )
    else:
        with timer.measure():
            setUpValidatorPods(
                time_intervals, logging, Config.WORKER_IMAGE, Config.WORKER_TAG
            )

    logging.info(
        "reading ZKValidator results from a db between the time range: %s - %s",
        state.batch.start_time,
        state.batch.end_time,
    )

    logging.info("ZKValidator results read from a db in %s.", timer.duration)
    webhook_url = Config.WEBHOOK_URL
    if webhook_url is not None:
        if timer.duration < float(Config.ALARM_ZK_LOWER_LIMIT_SEC):
            send_slack_message(
                webhook_url,
                f"ZkApp Validation took {timer.duration} seconds, which is too quick",
                logging,
            )
        if timer.duration > float(Config.ALARM_ZK_UPPER_LIMIT_SEC):
            send_slack_message(
                webhook_url,
                f"ZkApp Validation took {timer.duration}, which is too long",
                logging,
            )

    state_hash_df, all_submissions = load_submissions(
        time_intervals, db, Config.SUBMISSION_STORAGE
    )
    if not state_hash_df.empty:
        try:
            bot_log_id = process_statehash_df(
                db, state.batch, state_hash_df, timer.duration
            )
            db.connection.commit()
        except Exception as error:
            db.connection.rollback()
            logging.error("ERROR: %s", error)
            state.retry_batch()
            return
    else:
        # process_statehash_df not processed so new bot log id hasn't been created,
        # creating a new bot log id entry with 0 submissions processed
        values = (
            0,  # submissions processed
            state.batch.end_time,
            state.batch.start_time.timestamp(),
            state.batch.end_time.timestamp(),
            timer.duration.total_seconds(),
        )
        bot_log_id = db.create_bot_log(values)
        logging.info("Finished processing data from table.")
    try:
        db.update_scoreboard(
            state.batch.end_time,
            Config.UPTIME_DAYS_FOR_SCORE,
        )
        # we only copy submissions to Postgres if we're using Cassandra as the primary storage
        if Config.SUBMISSION_STORAGE == Config.STORAGE_CASSANDRA:
            db.insert_submissions(all_submissions)
    except Exception as error:
        db.connection.rollback()
        logging.error("ERROR: %s", error)
    else:
        db.connection.commit()
    state.advance_to_next_batch(bot_log_id)


def main():
    "The entrypoint to the program."
    load_dotenv()

    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if Config.SUBMISSION_STORAGE not in Config.VALID_STORAGE_OPTIONS:
        raise ValueError(
            f"Invalid storage option: {Config.SUBMISSION_STORAGE}. Valid options are {Config.VALID_STORAGE_OPTIONS}"
        )
    else:
        logging.info("Using SUBMISSION_STORAGE: %s", Config.SUBMISSION_STORAGE)

    connection = psycopg2.connect(
        host=Config.POSTGRES_HOST,
        port=Config.POSTGRES_PORT,
        database=Config.POSTGRES_DB,
        user=Config.POSTGRES_USER,
        password=Config.POSTGRES_PASSWORD,
    )

    interval = Config.SURVEY_INTERVAL_MINUTES
    db = DB(connection, logging)
    batch = db.get_batch_timings(timedelta(minutes=interval))
    state = State(batch)
    while not state.stop:
        if Config.ignore_application_status():
            logging.info("Ignoring application status update.")
        else:
            try:
                contact_details = get_contact_details_from_spreadsheet()
                db.update_application_status(contact_details)
            except Exception as error:
                logging.error(
                    "ERROR updating application status: %s", error, exc_info=True
                )

        process(db, state)


if __name__ == "__main__":
    main()
