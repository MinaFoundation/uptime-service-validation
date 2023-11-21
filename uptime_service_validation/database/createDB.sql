--Need to check on this structure. Feels like some of these should be non-null.
DROP TABLE IF EXISTS bot_logs;
CREATE TABLE bot_logs (
	id SERIAL PRIMARY KEY, 
	processing_time DOUBLE PRECISION, 
	files_processed INT, 
	file_timestamps TIMESTAMPTZ(6), 
	batch_start_epoch BIGINT, 
	batch_end_epoch BIGINT
);

DROP TABLE IF EXISTS statehash;
CREATE TABLE statehash (
	id SERIAL PRIMARY KEY,
	value TEXT
);
CREATE UNIQUE INDEX uq_state_hash ON statehash USING btree (value);

DROP TABLE IF EXISTS botlogs_statehash;
CREATE TABLE botlogs_statehash (
	id SERIAL PRIMARY KEY,
	parent_statehash_id INT, 
	statehash_id INT, 
	weight INT NOT NULL, 
	bot_log_id INT,
	CONSTRAINT fk_parent_statehash
		FOREIGN KEY(parent_statehash_id) 
		REFERENCES statehash(id),
	CONSTRAINT fk_statehash
		FOREIGN KEY(statehash_id) 
		REFERENCES statehash(id),
	CONSTRAINT fk_bot_log
		FOREIGN KEY(bot_log_id) 
		REFERENCES bot_logs(id)
);

DROP TABLE IF EXISTS nodes;
CREATE TABLE nodes (
	id SERIAL PRIMARY KEY,
	block_producer_key TEXT,
	updated_at TIMESTAMPTZ(6),
	score INT,
	score_percent NUMERIC(6,2),
	discord_id TEXT,
	email_id TEXT,
	application_status BOOLEAN
);

DROP TABLE IF EXISTS points;
CREATE TABLE points (
	id SERIAL PRIMARY KEY,
	file_name TEXT,
	file_timestamps TIMESTAMPTZ(6), 
	blockchain_epoch BIGINT, 
	blockchain_height BIGINT,
    amount INT, 
	created_at TIMESTAMPTZ(6),
	node_id INT,
	bot_log_id INT,
	statehash_id INT,
	CONSTRAINT fk_nodes
		FOREIGN KEY(node_id) 
		REFERENCES nodes(id),
	CONSTRAINT fk_bot_log
		FOREIGN KEY(bot_log_id) 
		REFERENCES bot_logs(id),
	CONSTRAINT fk_statehashes
		FOREIGN KEY(statehash_id) 
		REFERENCES statehash(id)
);

--Should some of these values be nullable? If uptime file doesn't pass validation, say?
DROP TABLE IF EXISTS uptime_file_history;
CREATE TABLE uptime_file_history (
	id SERIAL PRIMARY KEY,
	file_name TEXT, 
	receivedat BIGINT,
	receivedfrom TEXT, 
	node_id INT NOT NULL, 
	block_statehash INT,
    parent_block_statehash INT 
	nodedata_blockheight BIGINT, 
	nodedata_slot BIGINT, 
	file_modified_at TIMESTAMP(6), 
	file_created_at TIMESTAMP(6), 
	file_generation BIGINT,
    file_crc32c TEXT, 
	file_md5_hash TEXT,
	CONSTRAINT fk_nodes
		FOREIGN KEY(node_id) 
		REFERENCES nodes(id),
	CONSTRAINT fk_parent_statehash
		FOREIGN KEY(parent_statehash_id) 
		REFERENCES statehash(id),
	CONSTRAINT fk_statehash
		FOREIGN KEY(statehash_id) 
		REFERENCES statehash(id)
);
CREATE INDEX idx_ufh_node_id ON uptime_file_history USING btree (node_id);

DROP TABLE IF EXISTS score_history;
CREATE TABLE score_history (
	id SERIAL PRIMARY KEY,
	node_id INT,
	score_at TIMESTAMP(6), 
	score INT, 
	score_percent NUMERIC(6,2),
	CONSTRAINT fk_nodes
		FOREIGN KEY(node_id) 
		REFERENCES nodes(id)
);
CREATE UNIQUE INDEX uq_sh_node_score_at ON score_history USING btree (node_id, score_at);

-- Point Summary table that is auto-gen?