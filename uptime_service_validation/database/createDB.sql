--Need to check on this structure. Feels like some of these should be non-null.
DROP TABLE IF EXISTS bot_logs CASCADE;
CREATE TABLE bot_logs (
	id SERIAL PRIMARY KEY, 
	processing_time DOUBLE PRECISION, 
	files_processed INT, 
	file_timestamps TIMESTAMPTZ(6), 
	batch_start_epoch BIGINT, 
	batch_end_epoch BIGINT
);

DROP TABLE IF EXISTS statehash CASCADE;
CREATE TABLE statehash (
	id SERIAL PRIMARY KEY,
	value TEXT
);
CREATE UNIQUE INDEX uq_state_hash ON statehash USING btree (value);

DROP TABLE IF EXISTS botlogs_statehash CASCADE;
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

DROP TABLE IF EXISTS nodes CASCADE;
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

DROP TABLE IF EXISTS points CASCADE;
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

DROP TABLE IF EXISTS score_history CASCADE;
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