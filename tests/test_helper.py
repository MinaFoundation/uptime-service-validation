from datetime import datetime, timedelta
from uptime_service_validation.coordinator.aws_keyspaces_client import Submission
from uptime_service_validation.coordinator.helper import (getTimeBatches)
import pandas as pd
from uptime_service_validation.coordinator.helper import (filterStateHashPercentage, createGraph, applyWeights, bfs)
import calendar

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

def test_filter_state_hash_single():
    master_state_hash = pd.DataFrame([['state_hash_1', 'block_producer_key_1']], 
                                        columns=['state_hash', 'block_producer_key'])
    output = filterStateHashPercentage(master_state_hash)
    assert output == ['state_hash_1']

def test_filter_state_hash_multi():
    master_state_hash = pd.DataFrame([['state_hash_1', 'block_producer_key_1'], 
                                        ['state_hash_1', 'block_producer_key_2'], 
                                        ['state_hash_2', 'block_producer_key_3']], 
                                        columns=['state_hash', 'block_producer_key'])
    output = filterStateHashPercentage(master_state_hash)
    assert output == ['state_hash_1']

# The create_graph function creates a graph and adds all the state_hashes that appear in the batch as nodes, as well as the hashes from the previous batch.
# It also adds edges between any child and parent hash (this is even for parent-child relationship between batches)
# The arguments are:
# --batch_df: state-hashes of current batch.
# --p_selected_node_df: these are all the (short-listed) state-hashes from the previous batch (as well as their weights).
# --c_selected_node: these are the hashes from the current batch above 34% threshold
# --p_map: this lists the parent-child relationships in the previous batch.

def test_create_graph_count_number_of_nodes_and_edges():
    # current batch that was downloaded
    batch_df = pd.DataFrame([['state_hash_1', 'parent_state_hash_1'], ['state_hash_2', 'parent_state_hash_2']], columns=['state_hash', 'parent_state_hash'])
    # previous_state_hashes with weight
    p_selected_node_df = pd.DataFrame(['parent_state_hash_1'], columns=['state_hash'])
    # filtered_state_hashes  
    c_selected_node =  ['state_hash_1', 'state_hash_2']
    # relations between parents and children, i.e. those previous stte hashes that are parents in this batch.
    p_map = [['parent_state_hash_1', 'state_hash_1']]
    output = createGraph(batch_df, p_selected_node_df, c_selected_node, p_map)
    # total number of nodes is always those in the current batch + those from previous
    assert len(output.nodes) == len(batch_df) + len(p_selected_node_df)
    # there are no nodes in the current batch that are also parents of later nodes in the batch (see next test)
    assert len(output.edges) == len(p_map)

def test_create_graph_count_number_of_nodes_and_edges_nested():
    # current batch that was downloaded
    batch_df = pd.DataFrame([
        ['state_hash_1', 'parent_state_hash_1'], 
        ['state_hash_2', 'state_hash_1'], 
        ['state_hash_3', 'state_hash_2']], 
        columns=['state_hash', 'parent_state_hash'])
    # previous_state_hashes with weight
    p_selected_node_df = pd.DataFrame([['parent_state_hash_1'] ,['parent_state_hash_2']], columns=['state_hash'])
    # filtered_state_hashes  
    c_selected_node =  ['state_hash_1', 'state_hash_2']
    # relations between parents and children, i.e. those previous stte hashes that are parents in this batch.
    p_map = [['parent_state_hash_2', 'parent_state_hash_1']]
    output = createGraph(batch_df, p_selected_node_df, c_selected_node, p_map)
    # total number of nodes is the same
    assert len(output.nodes) == len(batch_df) + len(p_selected_node_df)
    # total number of edges is the parent-child relations in p_map, but plus also the parent-child relationships in the batch (i.e. 2) and between the two batches (i.e. 1).      
    assert len(output.edges) == len(p_map) + 3

# The apply_weights function sets the weights to 0 for any node above the 34% threshold and if a parent_hash to the weight computed form last time.
# The arguments are:
# --batch_df: state-hashes of current batch.
# --p_selected_node_df: these are all the (short-listed) state-hashes from the previous batch (as well as their weights).
# --c_selected_node: these are the hashes from the current batch above 34% threshold
def test_apply_weights_sum_weights_empty_parents_and_empty_selected_node():
    batch_df = pd.DataFrame([['state_hash_1', 'parent_state_hash_1'], 
                                ['state_hash_2', 'state_hash_1'], 
                                ['state_hash_3', 'state_hash_2']], 
                                columns=['state_hash', 'parent_state_hash'])
    p_selected_node_df = pd.DataFrame([['parent_state_hash_1', 123],
                                        ['parent_state_hash_2', 345]], 
                                        columns=['state_hash', 'weight'])
    c_selected_node =  ['state_hash_1', 'state_hash_2']
    p_map = [['parent_state_hash_2', 'parent_state_hash_1']]
    batch_graph = createGraph(batch_df, p_selected_node_df, c_selected_node, p_map)
    # pass in empty short-lists and parent nodes to the weight function and ensure every node has infinite weighting.
    c_selected_node_empty = []
    p_selected_node_df_empty = pd.DataFrame([], columns=['state_hash', 'weight'])
    weighted_graph = applyWeights(batch_graph, c_selected_node_empty, p_selected_node_df_empty)
    assert len(list(weighted_graph.nodes)) == 5
    for node in list(weighted_graph.nodes):
        assert weighted_graph.nodes[node]['weight'] == 9999

def test_apply_weights_sum_weights_nested():
    batch_df = pd.DataFrame([['state_hash_1', 'parent_state_hash_1'], 
                                ['state_hash_2', 'state_hash_1'], 
                                ['state_hash_3', 'state_hash_2']], 
                                columns=['state_hash', 'parent_state_hash'])
    p_selected_node_df = pd.DataFrame([['parent_state_hash_1', 123], 
                                        ['parent_state_hash_2', 345]], 
                                        columns=['state_hash', 'weight'])
    c_selected_node =  ['state_hash_1', 'state_hash_2']
    p_map = [['parent_state_hash_2', 'parent_state_hash_1']]
    batch_graph = createGraph(batch_df, p_selected_node_df, c_selected_node, p_map)
    weighted_graph = applyWeights(batch_graph, c_selected_node, p_selected_node_df)
    assert len(list(weighted_graph.nodes))== 5
    for node in list(weighted_graph.nodes):
        if node == 'state_hash_1':
            assert weighted_graph.nodes[node]['weight'] == 0
        if node == 'state_hash_2':
            assert weighted_graph.nodes[node]['weight'] == 0
        if node == 'state_hash_3':
            assert weighted_graph.nodes[node]['weight'] == 9999
        if node == 'parent_state_hash_1':
            assert weighted_graph.nodes[node]['weight']== 123
        if node == 'parent_state_hash_2':
            assert weighted_graph.nodes[node]['weight']== 345            

# The bfs is what computes the weight for nodes that aren't previous hashes or above the 34% threshold (which automatically have weight 0).
# The bfs output actually includes the parent-hashes, as well, and all those hashes from the current batch with computed weight <= 2.
# The arguments are:
# --graph: weighted graph computed from create_graph and apply_weights function
# --queue_list: these are the parent_hashes and the theshold hashes from the current batch.
# --node: first element of the queue
def test_bfs_easy():
    batch_df = pd.DataFrame([['state_hash_1', 'parent_state_hash_1'],
                            ['state_hash_2', 'state_hash_1'], 
                            ['state_hash_3', 'state_hash_2']], 
                            columns=['state_hash', 'parent_state_hash'])
    p_selected_node_df = pd.DataFrame([['parent_state_hash_1', 123],
                                        ['parent_state_hash_2', 345]], 
                                        columns=['state_hash', 'weight'])
    # empty short-list
    c_selected_node =  ['state_hash_1', 'state_hash_2']
    p_map = [['parent_state_hash_2', 'parent_state_hash_1']]
    batch_graph = createGraph(batch_df, p_selected_node_df, c_selected_node, p_map)
    weighted_graph = applyWeights(batch_graph, c_selected_node, p_selected_node_df)        
    queue_list = list(p_selected_node_df['state_hash'].values) + c_selected_node
    shortlist = bfs(graph=weighted_graph, queue_list=queue_list, node=queue_list[0])
                                                
    expected = pd.DataFrame([['state_hash_1', 0], ['state_hash_2', 0], ['state_hash_3', 1]], columns=['state_hash','weight'])
    pd.testing.assert_frame_equal(shortlist, expected)

def test_bfs_hard():
    batch_df = pd.DataFrame([
        ['state_hash_11', 'parent_state_hash_1'], 
        ['state_hash_12', 'state_hash_11'], 
        ['state_hash_13', 'state_hash_12'], 
        ['state_hash_14', 'state_hash_13'],
        ['state_hash_15', 'state_hash_14'],
        ['state_hash_16', 'state_hash_15'],
        ['state_hash_21', 'parent_state_hash_2'], 
        ['state_hash_22', 'state_hash_21'], 
        ['state_hash_23', 'state_hash_22'], 
        ['state_hash_24', 'state_hash_23'],
        ['state_hash_25', 'state_hash_24'],
        ['state_hash_26', 'state_hash_25']
        ], 
        columns=['state_hash', 'parent_state_hash'])
    p_selected_node_df = pd.DataFrame([['parent_state_hash_1', 1], 
                                        ['parent_state_hash_2', 1]], 
                                        columns=['state_hash', 'weight'])
    c_selected_node =  ['state_hash_11', 'state_hash_21']
    p_map = [['parent_state_hash_2', 'parent_state_hash_1']]
    batch_graph = createGraph(batch_df, p_selected_node_df, c_selected_node, p_map)
    weighted_graph = applyWeights(batch_graph, c_selected_node, p_selected_node_df)        
    queue_list = list(p_selected_node_df['state_hash'].values) + c_selected_node
    shortlist = bfs(weighted_graph, queue_list, queue_list[0])
    expected = pd.DataFrame([['parent_state_hash_1', 1],
                                ['parent_state_hash_2', 1],
                                ['state_hash_11', 0],
                                ['state_hash_21', 0], 
                                ['state_hash_12', 1],
                                ['state_hash_22', 1], 
                                ['state_hash_13', 2],
                                ['state_hash_23', 2]], 
                                columns=['state_hash','weight'])
    assert set(shortlist['state_hash']) == set(expected['state_hash'])

def test_blockchain_epoch():
    state_hash_df = pd.DataFrame(['2021-12-21T10:15:30Z', '2021-12-31T10:15:30Z'], columns=['created_at'])   
    state_hash_df['blockchain_epoch'] = state_hash_df['created_at'].apply(lambda row: int(calendar.timegm(datetime.strptime(row, "%Y-%m-%dT%H:%M:%SZ").timetuple()) * 1000))
    expected = pd.DataFrame([1640081730000, 1640945730000], columns=['blockchain_epoch'])
    pd.testing.assert_frame_equal(state_hash_df[['blockchain_epoch']], expected)