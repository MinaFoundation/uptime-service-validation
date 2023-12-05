import sys
import os
import logging
import calendar
import pandas as pd
import psycopg2
from kubernetes import config
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from time import time
from dataclasses import asdict
from uptime_service_validation.coordinator.helper import *
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


def test_env():
    test = os.environ.get("TEST_ENV")
    if test == "1":
        return True
    else:
        return False


def main():
    process_loop_count = 0
    load_dotenv()

    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    # Load the in-cluster configuration only if not test environment
    if not test_env():
        config.load_incluster_config()

    connection = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    # Step 1 Get previous record and build relations list
    interval = int(os.environ["SURVEY_INTERVAL_MINUTES"])
    prev_batch_end, cur_batch_end, bot_log_id = getBatchTimings(
        connection, logging, interval
    )
    cur_timestamp = datetime.now(timezone.utc)

    logging.info(
        "script start at {0}  end at {1}".format(prev_batch_end, cur_timestamp)
    )
    do_process = True
    while do_process:
        existing_state_df = getStatehashDF(connection, logging)
        existing_nodes = getExistingNodes(connection, logging)
        logging.info(
            "running for batch: {0} - {1}".format(prev_batch_end, cur_batch_end)
        )

        if cur_batch_end > cur_timestamp:
            logging.info("all files are processed till date")
            return
        else:
            master_df = pd.DataFrame()
            # Step 2 Create time ranges:
            time_intervals = getTimeBatches(
                prev_batch_end, cur_batch_end, int(os.environ["MINI_BATCH_NUMBER"])
            )
            # Step 3 Create Kubernetes ZKValidators and pass mini-batches.
            worker_image = os.environ["WORKER_IMAGE"]
            worker_tag = os.environ["WORKER_TAG"]
            start = time()
            jobs = []
            if test_env():
                logging.warning("running in test environment")
                setUpValidatorProcesses(
                    time_intervals, logging, worker_image, worker_tag
                )
            else:
                setUpValidatorPods(
                    time_intervals, jobs, logging, worker_image, worker_tag
                )
            end = time()
            # Step 4 We need to read the ZKValidator results from a db.
            logging.info(
                "reading ZKValidator results from a db between the time range: {0} - {1}".format(
                    prev_batch_end, cur_batch_end
                )
            )

            webhookURL = os.environ.get("WEBHOOK_URL")
            if (webhookURL != None):
                if (end - start < float(os.environ["ALARM_ZK_LOWER_LIMIT_SEC"])): 
                    sendSlackMessage(webhookURL, f'ZkApp Validation took {end- start} seconds, which is too quick', logging)
                if (end - start > float(os.environ["ALARM_ZK_UPPER_LIMIT_SEC"])): 
                    sendSlackMessage(webhookURL, f'ZkApp Validation took {end- start} seconds, which is too long', logging)
                    
            submissions = []
            cassandra = AWSKeyspacesClient()
            try:
                cassandra.connect()
                submissions = cassandra.get_submissions(
                    submitted_at_start=prev_batch_end,
                    submitted_at_end=cur_batch_end,
                    start_inclusive=True,
                    end_inclusive=False,
                )
            finally:
                cassandra.close()

            logging.info("number of submissions: {0}".format(len(submissions)))
            # print("submitter, state_hash, parent, height, slot, validation_error")
            # for s in submissions:
            #     print(
            #         s.submitter,
            #         s.state_hash,
            #         s.parent,
            #         s.height,
            #         s.slot,
            #         s.validation_error,
            #     )

            # Step 5 checks for forks and writes to the db.
            state_hash_df = pd.DataFrame(
                [asdict(submission) for submission in submissions]
            )
            all_files_count = state_hash_df.shape[0]
            if not state_hash_df.empty:
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
                state_hash_to_insert = findNewValuesToInsert(
                    existing_state_df, pd.DataFrame(state_hash, columns=["statehash"])
                )
                if not state_hash_to_insert.empty:
                    createStatehash(connection, logging, state_hash_to_insert)

                nodes_in_cur_batch = pd.DataFrame(
                    master_df["submitter"].unique(), columns=["block_producer_key"]
                )
                node_to_insert = findNewValuesToInsert(
                    existing_nodes, nodes_in_cur_batch
                )

                if not node_to_insert.empty:
                    node_to_insert["updated_at"] = datetime.now(timezone.utc)
                    createNodeRecord(connection, logging, node_to_insert, 100)

                master_df = master_df.rename(
                    columns={
                        "file_updated": "file_timestamps",
                        "submitter": "block_producer_key",
                    }
                )

                relation_df, p_selected_node_df = getPreviousStatehash(
                    connection, logging, bot_log_id
                )
                p_map = getRelationList(relation_df)
                c_selected_node = filterStateHashPercentage(master_df)
                batch_graph = createGraph(
                    master_df, p_selected_node_df, c_selected_node, p_map
                )
                weighted_graph = applyWeights(
                    batch_graph=batch_graph,
                    c_selected_node=c_selected_node,
                    p_selected_node=p_selected_node_df,
                )

                queue_list = (
                    list(p_selected_node_df["state_hash"].values) + c_selected_node
                )

                batch_state_hash = list(master_df["state_hash"].unique())

                shortlisted_state_hash_df = bfs(
                    graph=weighted_graph,
                    queue_list=queue_list,
                    node=queue_list[0],
                    # batch_statehash=batch_state_hash, (this used to be here in old code, but it's not used anywhere inside the function)
                )
                point_record_df = master_df[
                    master_df["state_hash"].isin(
                        shortlisted_state_hash_df["state_hash"].values
                    )
                ]

                for index, row in shortlisted_state_hash_df.iterrows():
                    if not row["state_hash"] in batch_state_hash:
                        shortlisted_state_hash_df.drop(index, inplace=True, axis=0)
                p_selected_node_df = shortlisted_state_hash_df.copy()
                parent_hash = []
                for s in shortlisted_state_hash_df["state_hash"].values:
                    p_hash = master_df[master_df["state_hash"] == s][
                        "parent_state_hash"
                    ].values[0]
                    parent_hash.append(p_hash)
                shortlisted_state_hash_df["parent_state_hash"] = parent_hash

                p_map = getRelationList(
                    shortlisted_state_hash_df[["parent_state_hash", "state_hash"]]
                )
                try:
                    if not point_record_df.empty:
                        file_timestamp = master_df.iloc[-1]["file_timestamps"]
                    else:
                        file_timestamp = cur_batch_end
                        logging.info(
                            "empty point record for start epoch {0} end epoch {1} ".format(
                                prev_batch_end.timestamp(), cur_batch_end.timestamp()
                            )
                        )

                    values = (
                        all_files_count,
                        file_timestamp,
                        prev_batch_end.timestamp(),
                        cur_batch_end.timestamp(),
                        end - start,
                    )
                    bot_log_id = createBotLog(connection, logging, values)

                    shortlisted_state_hash_df["bot_log_id"] = bot_log_id
                    insertStatehashResults(
                        connection, logging, shortlisted_state_hash_df
                    )

                    if not point_record_df.empty:
                        point_record_df["amount"] = 1
                        point_record_df["created_at"] = datetime.now(timezone.utc)
                        point_record_df["bot_log_id"] = bot_log_id
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

                        createPointRecord(connection, logging, point_record_df)
                except Exception as error:
                    connection.rollback()
                    logging.error(ERROR.format(error))
                finally:
                    connection.commit()
                process_loop_count += 1
                logging.info("Processed it loop count : {0}".format(process_loop_count))

            else:
                logging.info("Finished processing data from table.")
            try:
                updateScoreboard(
                    connection,
                    logging,
                    cur_batch_end,
                    int(os.environ["UPTIME_DAYS_FOR_SCORE"]),
                )
            except Exception as error:
                connection.rollback()
                logging.error(ERROR.format(error))
            finally:
                connection.commit()

            prev_batch_end = cur_batch_end
            cur_batch_end = prev_batch_end + timedelta(minutes=interval)
            if (
                prev_batch_end >= cur_timestamp
            ):  # This gets the coordinator to continue onto the next batch if it's taking so long as for the next interval is already finished.
                do_process = False


if __name__ == "__main__":
    main()
