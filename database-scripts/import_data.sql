COPY events(timestamp,machine_id,household_id,usage) 
FROM '/home/ubuntu/usage_newschema.csv' DELIMITER ',' CSV HEADER;




SELECT AVG(usage) FROM events
WHERE machine_id='pv' and household_id='industrial2' 
GROUP BY machine_id;