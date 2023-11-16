from datetime import datetime, timedelta
from uptime_service_validation.coordinator.aws_keyspaces_client import Submission
from uptime_service_validation.coordinator.helper import (
    pullFileNames,
    getTimeBatches,
)
import pandas as pd

# The folloiwng two tests will fail as I have not given an accurate bucket name.
# def testFilePullSmallRange(self):
#     start_time = datetime.strptime('2023-08-03T16:31:58Z',"%Y-%m-%dT%H:%M:%SZ")
#     end_time = datetime.strptime('2023-08-03T16:31:59Z', "%Y-%m-%dT%H:%M:%SZ")
#     filtered_list = pullFileNames(start_time, end_time, "block-bucket-name", True)
#     self.assertEqual(len(filtered_list), 1)

# def testFilePullLargeRange(self):
#     start_time = datetime.strptime('2023-08-03T16:32:00Z',"%Y-%m-%dT%H:%M:%SZ")
#     end_time = datetime.strptime('2023-08-03T16:33:59Z', "%Y-%m-%dT%H:%M:%SZ")
#     filtered_list = pullFileNames(start_time, end_time, "block-bucket-name", True)
#     self.assertEqual(len(filtered_list), 11)


def test_get_time_batches():
    a = datetime(2023, 11, 6, 15, 35, 47, 630499)
    b = a + timedelta(minutes=5)
    result = getTimeBatches(a, b, 10)

    assert len(result) == 10
    assert result[0] == (a, datetime(2023, 11, 6, 15, 36, 17, 630499))
    assert result[1] == (
        datetime(2023, 11, 6, 15, 36, 17, 630499),
        datetime(2023, 11, 6, 15, 36, 47, 630499),
    )
    assert result[2] == (
        datetime(2023, 11, 6, 15, 36, 47, 630499),
        datetime(2023, 11, 6, 15, 37, 17, 630499),
    )
    assert result[3] == (
        datetime(2023, 11, 6, 15, 37, 17, 630499),
        datetime(2023, 11, 6, 15, 37, 47, 630499),
    )
    assert result[4] == (
        datetime(2023, 11, 6, 15, 37, 47, 630499),
        datetime(2023, 11, 6, 15, 38, 17, 630499),
    )
    assert result[5] == (
        datetime(2023, 11, 6, 15, 38, 17, 630499),
        datetime(2023, 11, 6, 15, 38, 47, 630499),
    )
    assert result[6] == (
        datetime(2023, 11, 6, 15, 38, 47, 630499),
        datetime(2023, 11, 6, 15, 39, 17, 630499),
    )
    assert result[7] == (
        datetime(2023, 11, 6, 15, 39, 17, 630499),
        datetime(2023, 11, 6, 15, 39, 47, 630499),
    )
    assert result[8] == (
        datetime(2023, 11, 6, 15, 39, 47, 630499),
        datetime(2023, 11, 6, 15, 40, 17, 630499),
    )
    assert result[9] == (datetime(2023, 11, 6, 15, 40, 17, 630499), b)

def test_array_dataframe():
    submissions = [
        Submission(
            "submitted_at_date_1",
            "submitted_at_1" ,
            "submitter_1",
            "created_at_1",
            "block_hash_1",
            "remote_addr_1",
            "peer_id_1",
            "snark_work_1",
            "graphql_control_port_1",
            "built_with_commit_sha_1",
            "state_hash_1", 
            "parent_1",
            "height_1",
            "slot_1",
            "validation_error_1"),
        Submission(
            "submitted_at_date_2",
            "submitted_at_2" ,
            "submitter_2",
            "created_at_2",
            "block_hash_2",
            "remote_addr_2",
            "peer_id_2",
            "snark_work_2",
            "graphql_control_port_2",
            "built_with_commit_sha_2",
            "state_hash_2", 
            "parent_2",
            "height_2",
            "slot_2",
            "validation_error_2")
    ]
    state_hash_df = pd.DataFrame(submissions)

    pd.testing.assert_frame_equal(state_hash_df[["submitted_at_date"]], pd.DataFrame(["submitted_at_date_1", "submitted_at_date_2"], columns=["submitted_at_date"]))
    pd.testing.assert_frame_equal(state_hash_df[["submitted_at"]], pd.DataFrame(["submitted_at_1", "submitted_at_2"], columns=["submitted_at"]))
    pd.testing.assert_frame_equal(state_hash_df[["submitter"]], pd.DataFrame(["submitter_1", "submitter_2"], columns=["submitter"]))
    pd.testing.assert_frame_equal(state_hash_df[["created_at"]], pd.DataFrame(["created_at_1", "created_at_2"], columns=["created_at"]))
    pd.testing.assert_frame_equal(state_hash_df[["block_hash"]], pd.DataFrame(["block_hash_1", "block_hash_2"], columns=["block_hash"]))
    pd.testing.assert_frame_equal(state_hash_df[["remote_addr"]], pd.DataFrame(["remote_addr_1", "remote_addr_2"], columns=["remote_addr"]))
    pd.testing.assert_frame_equal(state_hash_df[["peer_id"]], pd.DataFrame(["peer_id_1", "peer_id_2"], columns=["peer_id"]))
    pd.testing.assert_frame_equal(state_hash_df[["snark_work"]], pd.DataFrame(["snark_work_1", "snark_work_2"], columns=["snark_work"]))
    pd.testing.assert_frame_equal(state_hash_df[["graphql_control_port"]], pd.DataFrame(["graphql_control_port_1", "graphql_control_port_2"], columns=["graphql_control_port"]))
    pd.testing.assert_frame_equal(state_hash_df[["built_with_commit_sha"]], pd.DataFrame(["built_with_commit_sha_1", "built_with_commit_sha_2"], columns=["built_with_commit_sha"]))
    pd.testing.assert_frame_equal(state_hash_df[["state_hash"]], pd.DataFrame(["state_hash_1", "state_hash_2"], columns=["state_hash"]))
    pd.testing.assert_frame_equal(state_hash_df[["parent"]], pd.DataFrame(["parent_1", "parent_2"], columns=["parent"]))
    pd.testing.assert_frame_equal(state_hash_df[["height"]], pd.DataFrame(["height_1", "height_2"], columns=["height"]))
    pd.testing.assert_frame_equal(state_hash_df[["slot"]], pd.DataFrame(["slot_1", "slot_2"], columns=["slot"]))
    pd.testing.assert_frame_equal(state_hash_df[["validation_error"]], pd.DataFrame(["validation_error_1", "validation_error_2"], columns=["validation_error"]))