import psycopg2
import psycopg2.extras as extras
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, timezone
import pandas as pd
import networkx as nx

ERROR = "Error: {0}"

def getTimeBatches(start_time, end_time, range_number):
    diff = (end_time - start_time) / range_number
    print(diff)
    time_intervals = []
    for i in range(range_number):
        time_intervals.append((start_time + diff * i, start_time + diff * (i + 1)))
    return time_intervals

def getBatchTimings(conn, logger, interval):
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, end_date FROM bot_logs ORDER BY end_date DESC limit 1 "
        )
        result = cursor.fetchone()
        bot_log_id = result[0]
        prev_epoch = result[1]

        prev_batch_end = datetime.fromtimestamp(prev_epoch, timezone.utc)
        cur_batch_end = prev_batch_end + timedelta(minutes=interval)
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(ERROR.format(error))
        return -1
    finally:
        cursor.close()
    return prev_batch_end, cur_batch_end, bot_log_id

def getPreviousStatehash(conn, logger, bot_log_id):
    cursor = conn.cursor()
    try:
        sql_query = """select  ps.value parent_statehash, s1.value statehash, b.weight
            from bot_logs_statehash b join statehash s1 ON s1.id = b.statehash_id 
		    join statehash ps on b.parent_statehash_id = ps.id where bot_log_id =%s"""
        cursor.execute(sql_query, (bot_log_id,))
        result = cursor.fetchall()

        df = pd.DataFrame(result, columns=["parent_state_hash", "state_hash", "weight"])
        previous_result_df = df[["parent_state_hash", "state_hash"]]
        p_selected_node_df = df[["state_hash", "weight"]]
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(ERROR.format(error))
        cursor.close()
        return -1
    finally:
        cursor.close()
    return previous_result_df, p_selected_node_df

def getRelationList(df):
    relation_list = []
    for child, parent in df[["state_hash", "parent_state_hash"]].values:
        if parent in df["state_hash"].values:
            relation_list.append((parent, child))
    return relation_list

def getStatehashDF(conn, logger):
    cursor = conn.cursor()
    try:
        cursor.execute("select value from statehash")
        result = cursor.fetchall()
        state_hash = pd.DataFrame(result, columns=["statehash"])
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(ERROR.format(error))
        return -1
    finally:
        cursor.close()
    return state_hash

def findNewValuesToInsert(existing_values, new_values):
    return (
        existing_values.merge(new_values, how="outer", indicator=True)
        .loc[lambda x: x["_merge"] == "right_only"]
        .drop("_merge", 1)
        .drop_duplicates()
    )

def createStatehash(conn, logger, statehash_df, page_size=100):
    tuples = [tuple(x) for x in statehash_df.to_numpy()]
    logger.info("create_statehash: {0}".format(tuples))
    query = """INSERT INTO statehash ( value) 
            VALUES ( %s)  """
    cursor = conn.cursor()
    try:
        cursor = conn.cursor()
        extras.execute_batch(cursor, query, tuples, page_size)
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(ERROR.format(error))
        cursor.close()
        return -1
    finally:
        cursor.close()
    logger.info("create_statehash  end ")
    return 0

def createNodeRecord(conn, logger, df, page_size=100):
    tuples = [tuple(x) for x in df.to_numpy()]
    query = """INSERT INTO nodes ( block_producer_key, updated_at) 
            VALUES ( %s,  %s )  """
    cursor = conn.cursor()
    try:
        extras.execute_batch(cursor, query, tuples, page_size)
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(ERROR.format(error))
        cursor.close()
        return 1
    finally:
        cursor.close()
    logger.info("create_point_record  end ")
    return 0

def filterStateHashPercentage(df, p=0.34):
    state_hash_list = (
        df["state_hash"].value_counts().sort_values(ascending=False).index.to_list()
    )
    # get 34% number of blk in given batch
    total_unique_blk = df["block_producer_key"].nunique()
    percentage_result = round(total_unique_blk * p, 2)
    good_state_hash_list = list()
    for s in state_hash_list:
        blk_count = df[df["state_hash"] == s]["block_producer_key"].nunique()
        # check blk_count for state_hash submitted by blk least 34%
        if blk_count >= percentage_result:
            good_state_hash_list.append(s)
    return good_state_hash_list

def createGraph(batch_df, p_selected_node_df, c_selected_node, p_map):
    batch_graph = nx.DiGraph()
    parent_hash_list = batch_df["parent_state_hash"].unique()
    state_hash_list = set(
        list(batch_df["state_hash"].unique())
        + list(p_selected_node_df["state_hash"].values)
    )
    selected_parent = [
        parent for parent in parent_hash_list if parent in state_hash_list
    ]
    """ t1=[w[42:] for w in list(p_selected_node_df['state_hash'].values)]
    t2=[w[42:] for w in c_selected_node]
    t3=[w[42:] for w in state_hash_list]
    batch_graph.add_nodes_from(t1)
    batch_graph.add_nodes_from( t2)
    batch_graph.add_nodes_from(t3) """

    batch_graph.add_nodes_from(list(p_selected_node_df["state_hash"].values))
    batch_graph.add_nodes_from(c_selected_node)
    batch_graph.add_nodes_from(state_hash_list)

    for row in batch_df.itertuples():
        state_hash = getattr(row, "state_hash")
        parent_hash = getattr(row, "parent_state_hash")

        if parent_hash in selected_parent:
            batch_graph.add_edge(parent_hash, state_hash)

    #  add edges from previous batch nodes
    batch_graph.add_edges_from(p_map)

    return batch_graph

def applyWeights(batch_graph, c_selected_node, p_selected_node):
    for node in list(batch_graph.nodes()):
        if node in c_selected_node:
            batch_graph.nodes[node]["weight"] = 0
        elif node in p_selected_node["state_hash"].values:
            batch_graph.nodes[node]["weight"] = p_selected_node[
                p_selected_node["state_hash"] == node
            ]["weight"].values[0]
        else:
            batch_graph.nodes[node]["weight"] = 9999

    return batch_graph

def plotGraph(batch_graph, g_pos, title):
    # plot the graph
    plt.figure(figsize=(8, 8))
    plt.title(title)
    if not g_pos:
        g_pos = nx.spring_layout(batch_graph, k=0.3, iterations=1)
    nx.draw(
        batch_graph,
        pos=g_pos,
        connectionstyle="arc3, rad = 0.1",
        with_labels=False,
        node_size=500,
    )
    t_dict = {}
    for n in batch_graph.nodes:
        if batch_graph.nodes[n]["weight"] == 0:
            t_dict[n] = (n[-4:], 0)
        else:
            t_dict[n] = (n[-4:], batch_graph.nodes[n]["weight"])
    nx.draw_networkx_labels(batch_graph, pos=g_pos, labels=t_dict, font_size=8)
    plt.show()  # pause before exiting
    return g_pos

def getMinimumWeight(graph, child_node):
    child_node_weight = graph.nodes[child_node]["weight"]
    for parent in list(graph.predecessors(child_node)):
        lower = min(graph.nodes[parent]["weight"] + 1, child_node_weight)
        child_node_weight = lower
    return child_node_weight

def bfs(graph, queue_list, node, max_depth=2):
    visited = list()
    visited.append(node)
    cnt = 2
    while queue_list:
        m = queue_list.pop(0)
        for neighbour in list(graph.neighbors(m)):
            if neighbour not in visited:
                graph.nodes[neighbour]["weight"] = getMinimumWeight(graph, neighbour)
                visited.append(neighbour)
                # if not neighbour in visited:
                queue_list.append(neighbour)
        # plot_graph(graph, g_pos, str(cnt)+'.'+m)
        cnt += 1
    shortlisted_state = []
    hash_weights = []
    for node in list(graph.nodes()):
        if graph.nodes[node]["weight"] <= max_depth:
            # if node in batch_statehash:
            shortlisted_state.append(node)
            hash_weights.append(graph.nodes[node]["weight"])

    shortlisted_state_hash_df = pd.DataFrame()
    shortlisted_state_hash_df["state_hash"] = shortlisted_state
    shortlisted_state_hash_df["weight"] = hash_weights
    return shortlisted_state_hash_df

def createBotLog(conn, logger, values):
    query = """INSERT INTO bot_logs(files_processed, file_timestamps, batch_start_epoch, batch_end_epoch, 
                processing_time)  values ( %s, %s, %s, %s, %s) RETURNING id """
    try:
        cursor = conn.cursor()
        cursor.execute(query, values)
        result = cursor.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(ERROR.format(error))
        cursor.close()
        return -1
    finally:
        cursor.close()
    logger.info("create_bot_log  end ")
    return result[0]

def insertStatehashResults(conn, logger, df, page_size=100):
    temp_df = df[["parent_state_hash", "state_hash", "weight", "bot_log_id"]]
    tuples = [tuple(x) for x in temp_df.to_numpy()]
    query = """INSERT INTO bot_logs_statehash(parent_statehash_id, statehash_id, weight, bot_log_id ) 
        VALUES ( (SELECT id FROM statehash WHERE value= %s), (SELECT id FROM statehash WHERE value= %s), %s, %s ) """
    cursor = conn.cursor()

    try:
        extras.execute_batch(cursor, query, tuples, page_size)
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(ERROR.format(error))
        cursor.close()
        return 1
    finally:
        cursor.close()
    logger.info("create_bot_logs_statehash  end ")
    return 0

def createPointRecord(conn, logger, df, page_size=100):
    tuples = [tuple(x) for x in df.to_numpy()]
    query = """INSERT INTO points (file_name, file_timestamps, blockchain_epoch, node_id, blockchain_height,
                amount, created_at, bot_log_id, statehash_id) 
            VALUES ( %s, %s,  %s, (SELECT id FROM nodes WHERE block_producer_key= %s), %s, %s, 
                    %s, %s, (SELECT id FROM statehash WHERE value= %s) )"""
    try:
        cursor = conn.cursor()
        extras.execute_batch(cursor, query, tuples, page_size)
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(ERROR.format(error))
        cursor.close()
        return 1
    finally:
        cursor.close()
    logger.info("create_point_record  end ")
    return 0

def updateScoreboard(conn, logger, score_till_time, uptime_days=30):
    sql = """with vars  (snapshot_date, start_date) as( values (%s AT TIME ZONE 'UTC', 
			(%s - interval '%s' day) AT TIME ZONE 'UTC')
	)
	, epochs as(
		select extract('epoch' from snapshot_date) as end_epoch,
		extract('epoch' from start_date) as start_epoch from vars
	)
	, b_logs as(
		select (count(1) ) as surveys
		from bot_logs b , epochs e
		where b.batch_start_epoch >= start_epoch and  b.batch_end_epoch <= end_epoch
	)
	, scores as (
		select p.node_id, count(p.bot_log_id) bp_points
		from points p join bot_logs b on p.bot_log_id =b.id, epochs
		where b.batch_start_epoch >= start_epoch and  b.batch_end_epoch <= end_epoch
		group by 1
	)
	, final_scores as (
	select node_id, bp_points, 
		surveys, trunc( ((bp_points::decimal*100) / surveys),2) as score_perc
	from scores l join nodes n on l.node_id=n.id, b_logs t
	)
	update nodes nrt set score = s.bp_points, score_percent=s.score_perc  
	from final_scores s where nrt.id=s.node_id """

    history_sql = """insert into score_history (node_id, score_at, score, score_percent)
        SELECT id as node_id, %s, score, score_percent from nodes where score is not null """
    try:
        cursor = conn.cursor()
        cursor.execute(
            sql,
            (
                score_till_time,
                score_till_time,
                uptime_days,
            ),
        )
        cursor.execute(history_sql, (score_till_time,))
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(ERROR.format(error))
        cursor.close()
        return -1
    finally:
        cursor.close()
    return 0

def getExistingNodes(conn, logger):
    cursor = conn.cursor()
    try:
        cursor.execute("select block_producer_key from nodes")
        result = cursor.fetchall()
        nodes = pd.DataFrame(result, columns=["block_producer_key"])
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(ERROR.format(error))
        cursor.close()
        return -1
    finally:
        cursor.close()
    return nodes
