--One time set up 

CREATE TABLE IF NOT EXISTS history_events(
    event_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    machine_id VARCHAR(40),
    household_id VARCHAR(40), 
    usage NUMERIC (10, 4)
 );

CREATE TABLE IF NOT EXISTS events(
    event_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    machine_id VARCHAR(40),
    household_id VARCHAR(40), 
    usage NUMERIC (8, 4),
 );

 CREATE TABLE IF NOT EXISTS anomalies(
    anomaly_id SERIAL PRIMARY KEY,
    event_id INTEGER,
    timestamp TIMESTAMP,
    machine_id VARCHAR(40),
    household_id VARCHAR(40), 
    usage NUMERIC (8, 4)
 );

CREATE TABLE IF NOT EXISTS machines(
    machine_id VARCHAR(40) PRIMARY KEY,
    machine_type VARCHAR(40),
    household_id VARCHAR(40), 
    export BOOLEAN
);

CREATE TABLE IF NOT EXISTS households(
    household_id VARCHAR(40) PRIMARY KEY,
    household_type VARCHAR(40),
    household_address VARCHAR(100)
);

-- import data
-- COPY events(timestamp,machine_id,household_id,usage) 
-- FROM '/home/ubuntu/usage_newschema.csv' DELIMITER ',' CSV HEADER;
